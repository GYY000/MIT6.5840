package mr

import (
	"encoding/json"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	for {
		args := EmptyArgs{}
		reply := Reply{}
		call("Coordinator.AllocTask", &args, &reply)
		if reply.JobType == 3 {
			break
		} else if reply.JobType == 2 {
			time.Sleep(time.Second)
		} else if reply.JobType == 1 {
			//reduce
			var kva []KeyValue
			for i := 0; i < reply.NMap; i++ {
				fileName := "mr-" + strconv.Itoa(i) + "-" + strconv.Itoa(reply.JobIndex)
				file, err := os.Open(fileName)
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}
				dec := json.NewDecoder(file)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}
				err = file.Close()
				if err != nil {
					log.Fatalf("cannot close %v", fileName)
				}
			}
			sort.Sort(ByKey(kva))
			oname := "mr-out-" + strconv.Itoa(reply.JobIndex)
			tempFile, _ := ioutil.TempFile("", oname+"*")

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				var values []string
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducef(kva[i].Key, values)
				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tempFile, "%v %v\n", kva[i].Key, output)
				i = j
			}
			os.Rename(tempFile.Name(), oname)
			tempFile.Close()
			state := FinishReply{}
			call("Coordinator.ReceiveReduceFinish", &TaskArgs{JobIndex: reply.JobIndex}, &state)
		} else if reply.JobType == 0 {
			//map
			file, err := os.Open(reply.Filename)
			if err != nil {
				log.Fatalf("cannot open %v", reply.Filename)
			}
			content, err := io.ReadAll(file)
			if err != nil {
				log.Fatalf("cannot read %v", reply.Filename)
			}
			file.Close()
			kva := mapf(reply.Filename, string(content))
			buckets := make([][]KeyValue, reply.NReduce)
			for i := range buckets {
				buckets[i] = []KeyValue{}
			}
			for _, kv := range kva {
				buckets[ihash(kv.Key)%reply.NReduce] = append(buckets[ihash(kv.Key)%reply.NReduce], kv)
			}
			for i := 0; i < reply.NReduce; i++ {
				fileName := "mr-" + strconv.Itoa(reply.JobIndex) + "-" + strconv.Itoa(i)
				tempfile, err := ioutil.TempFile("", fileName+"*")
				if err != nil {
					log.Fatalf("cannot open %v", fileName)
				}
				enc := json.NewEncoder(tempfile)
				for _, kv := range buckets[i] {
					err = enc.Encode(&kv)
					if err != nil {
						log.Fatalf("write err %v", fileName)
					}
				}
				os.Rename(tempfile.Name(), fileName)
			}
			state := FinishReply{}
			call("Coordinator.ReceiveMapFinish", &TaskArgs{JobIndex: reply.JobIndex}, &state)
		}
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
