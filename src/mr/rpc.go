package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import "os"
import "strconv"

type EmptyArgs struct {
}

type EmptyReply struct {
}

// example to show how to declare the arguments
// and reply for an RPC.
type FinishReply struct {
	State int // 1 for ok 0 for not
}

type Args struct {
}

type Reply struct {
	JobType  int //0 for Map 1 for reduce 2 for wait 3 for end
	JobIndex int

	//Map
	Filename string
	NReduce  int
	//Reduce
	NMap int
}

type TaskArgs struct {
	JobIndex int
}

// Add your RPC definitions here.

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
