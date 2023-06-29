package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
)

type Coordinator struct {
	// Your definitions here.
	state   State
	nReduce int
	nMap    int
}

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
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
// 让 main/mrcoordinator.go 退出
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	return ret
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.state = progress
	c.nMap = len(files)
	c.nReduce = nReduce

	c.server()
	return &c
}

func (c *Coordinator) PRC_Start_Task(request *Request_Start_Task, reply *Reply_Start_Task) {
	reply.nMap = c.nMap
	reply.nRecude = c.nReduce
}

func (c *Coordinator) RPC_Map_Task(request *Request_Map_Task, reply *Reply_Map_Task) {
}

func (c *Coordinator) RPC_Reduce_Task(request *Request_Map_Task, reply *Reply_Map_Task) {
}

func (c *Coordinator) RPC_Task_Done(request *Request_Map_Task, reply *Reply_Map_Task) {
}
