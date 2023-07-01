package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	state   State
	nReduce int
	nMap    int
	files   []string

	// files[idx] = file_name
	// index to work
	map_allocated map[int]bool // idx ----> which allocated
	map_done      map[int]bool // idx ----> which done

	// index to work
	reduce_allocated map[int]bool
	reduce_done      map[int]bool

	// task to last heartbeat time
	task_time map[*Task]time.Time

	// channel for accepting data
	handle_ask       chan *Request_Ask_Task
	handle_done      chan *Request_Done_Task
	handle_heartbeat chan interface{}
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

	// initiate the server
	c.files = append([]string(nil), files...)
	c.nReduce = nReduce
	c.nMap = len(files)
	c.map_allocated = make(map[int]bool)
	c.map_done = make(map[int]bool)
	c.reduce_allocated = make(map[int]bool)
	c.reduce_done = make(map[int]bool)
	c.task_time = make(map[*Task]time.Time)
	c.handle_ask = make(chan *Request_Ask_Task)
	c.handle_done = make(chan *Request_Done_Task)
	c.handle_heartbeat = make(chan interface{})

	c.server()
	return &c
}

func (c *Coordinator) PRC_Start_Task(request *Request_Start_Task, reply *Reply_Start_Task) {
	reply.nMap = c.nMap
	reply.nRecude = c.nReduce
}

func (c *Coordinator) RPC_Ask_Task(request *Request_Ask_Task, reply *Reply_Ask_Task) {
	c.handle_ask <- request
}

func (c *Coordinator) RPC_Done_Task(request *Request_Done_Task, reply *Reply_Done_Task) {
	c.handle_done <- request
}

func (c *Coordinator) loop() {
	for {
		select {
		case request := <-c.handle_ask:

		}
	}
}

// the loop will call this function to assign task: 1 MAP 2 REDUCE
func (c *Coordinator) assign_task(ts *Task) {

}
