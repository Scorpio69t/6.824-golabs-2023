package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"strconv"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.
type TaskType string

const (
	Map    TaskType = "Map"
	Reduce TaskType = "Reduce"
	Exit   TaskType = "Exit"
)

type RequestTaskArgs struct{}

type RequestTaskReply struct {
	Id       int      // Map id or reduce id
	Type     TaskType // Task type
	FileName string   // Map input filename
	N        int      // Map number or reduce number
}

type FinishTaskArgs struct {
	Id   int      // Map id or reduce id
	Type TaskType // Task type
}

type FinishTaskReply struct{}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
