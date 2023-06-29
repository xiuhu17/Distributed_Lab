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
	files   []string

	// files[idx] = file_name
	map_allocated map[int]bool // idx ----> which allocated
	map_done      map[int]bool // idx ----> which done

	// not start until
	reduce_allocated map[int]bool
	reduce_done      map[int]bool

	// channel for accepting data
	handle_allocate chan interface{}
	handle_done     chan interface{}
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

func (c *Coordinator) RPC_Ask_Task(request *Request_Ask_Task, reply *Reply_Ask_Task) {
}

func (c *Coordinator) RPC_Task_Done(request *Request_Task_Done, reply *Reply_Task_Done) {
}

func (c *Coordinator) loop() {

}
