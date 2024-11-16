package mr

import (
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	files       []string
	nReduce     int
	nMap        int
	doneMap     int
	mapState    []int //0 for to alloc,1 for complete,2 for in process
	doneReduce  int
	reduceState []int
	mu          sync.Mutex
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.

func (c *Coordinator) AllocTask(args *EmptyArgs, reply *Reply) error {
	c.mu.Lock()
	reply.NReduce = c.nReduce
	reply.NMap = c.nMap
	reply.JobType = 2
	if c.nMap > c.doneMap {
		toAlloc := -1
		for i := 0; i < c.nMap; i++ {
			if c.mapState[i] == 0 {
				toAlloc = i
				break
			}
		}
		if toAlloc != -1 {
			c.mapState[toAlloc] = 2
			reply.JobType = 0
			reply.Filename = c.files[toAlloc]
			reply.JobIndex = toAlloc
			c.mu.Unlock()
			go func(x int) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				if c.mapState[x] != 1 {
					c.mapState[x] = 0 //realloc
				}
				c.mu.Unlock()
			}(toAlloc)
		} else {
			c.mu.Unlock()
			reply.JobType = 2
		}
	} else if c.nReduce > c.doneReduce {
		toAlloc := -1
		for i := 0; i < c.nReduce; i++ {
			if c.reduceState[i] == 0 {
				toAlloc = i
				break
			}
		}
		if toAlloc != -1 {
			c.reduceState[toAlloc] = 2
			reply.JobType = 1
			reply.JobIndex = toAlloc
			c.mu.Unlock()
			go func(x int) {
				time.Sleep(10 * time.Second)
				c.mu.Lock()
				if c.reduceState[x] != 1 {
					c.reduceState[x] = 0 //realloc
				}
				c.mu.Unlock()
			}(toAlloc)
		} else {
			c.mu.Unlock()
			reply.JobType = 2
		}
	} else {
		reply.JobType = 3
	}
	return nil
}

func (c *Coordinator) ReceiveMapFinish(args *TaskArgs, reply *FinishReply) error {
	c.mu.Lock()
	if c.mapState[args.JobIndex] != 1 {
		c.mapState[args.JobIndex] = 1
		c.doneMap++
		reply.State = 1 //allow for rename
	} else {
		reply.State = 0
	}
	c.mu.Unlock()
	return nil
}

func (c *Coordinator) ReceiveReduceFinish(args *TaskArgs, reply *FinishReply) error {
	c.mu.Lock()
	if c.reduceState[args.JobIndex] != 1 {
		c.reduceState[args.JobIndex] = 1
		c.doneReduce++
		reply.State = 1
	} else {
		reply.State = 0
	}
	c.mu.Unlock()
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server() {
	rpc.Register(c)
	rpc.HandleHTTP()
	//l, e := net.Listen("tcp", ":1234")
	sockname := coordinatorSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	// Your code here.
	ret := c.doneReduce == c.nReduce
	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}
	c.nReduce = nReduce
	c.nMap = len(files)
	c.mapState = make([]int, len(files))
	c.reduceState = make([]int, nReduce)
	c.files = files
	c.doneReduce = 0
	c.doneMap = 0
	// Your code here.

	c.server()
	return &c
}
