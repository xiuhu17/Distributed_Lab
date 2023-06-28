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

type Request_Start_Task struct {
}

type Reply_Start_Task struct {
	nMap    int
	nRecude int
}

type Request_Map_Task struct {
}

type Reply_Map_Task struct {
	file  string
	index int
}

type Request_Reduce_Task struct {
}

type Reply_Reduce_Task struct {
	index int
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/5840-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
