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
	NMap    int
	NRecude int
}

type Request_Ask_Task struct {
	Ts *Task
}

type Reply_Ask_Task struct {
	Tp    Type
	Index int
	File  string
}

type Wrap_Ask struct {
	Req *Request_Ask_Task
	Rep *Reply_Ask_Task
	Tmp chan struct{}
}

type Request_Done_Task struct {
	Ts *Task
}

type Reply_Done_Task struct {
	Dn bool
}

type Wrap_Done struct {
	Req *Request_Done_Task
	Rep *Reply_Done_Task
	Tmp chan struct{}
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
