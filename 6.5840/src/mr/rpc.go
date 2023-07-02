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

type Request_Ask_Task struct {
	ts *Task
}

type Reply_Ask_Task struct {
	tp    Type
	state State
	index int
	file  string
}

type Wrap_Ask struct {
	req *Request_Ask_Task
	rep *Reply_Ask_Task
	tmp chan struct{}
}

type Request_Done_Task struct {
	ts *Task
}

type Reply_Done_Task struct {
	state State
}

type Wrap_Done struct {
	req *Request_Done_Task
	rep *Reply_Done_Task
	tmp chan struct{}
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
