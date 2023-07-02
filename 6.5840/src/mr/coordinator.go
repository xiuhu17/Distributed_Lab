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
	nReduce int
	nMap    int
	files   []string

	// files[idx] = file_name
	// index to task
	map_allocated map[int]*Task // idx ----> which task allocated
	map_done      map[int]*Task // idx ----> which task done

	// index to work
	reduce_allocated map[int]*Task
	reduce_done      map[int]*Task

	// task to last heartbeat time
	task_time map[*Task]time.Time

	// channel for accepting data
	ask_chan         chan *Wrap_Ask
	done_chan        chan *Wrap_Done
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
	c.map_allocated = make(map[int]*Task)
	c.map_done = make(map[int]*Task)
	c.reduce_allocated = make(map[int]*Task)
	c.reduce_done = make(map[int]*Task)
	c.task_time = make(map[*Task]time.Time)
	c.ask_chan = make(chan *Wrap_Ask)
	c.done_chan = make(chan *Wrap_Done)
	c.handle_heartbeat = make(chan interface{})

	c.server()
	return &c
}

func (c *Coordinator) PRC_Start_Task(request *Request_Start_Task, reply *Reply_Start_Task) {
	reply.nMap = c.nMap
	reply.nRecude = c.nReduce
}

func (c *Coordinator) RPC_Ask_Task(request *Request_Ask_Task, reply *Reply_Ask_Task) {
	wrp := Wrap_Ask{req: request, rep: reply, tmp: make(chan struct{})}
	c.ask_chan <- (&wrp)
	<-wrp.tmp
}

func (c *Coordinator) RPC_Done_Task(request *Request_Done_Task, reply *Reply_Done_Task) {
	wrp := Wrap_Done{req: request, rep: reply, tmp: make(chan struct{})}
	c.done_chan <- (&wrp)
	<-wrp.tmp
}

func (c *Coordinator) loop() {
	for {
		select {
		case wrp := <-c.ask_chan:
			c.handle_ask_chan(wrp)
		case wrp := <-c.done_chan:
			c.handle_done_chan(wrp)
		}
	}
}

// the loop will call this function to assign task: 1 MAP 2 REDUCE
func (c *Coordinator) handle_ask_chan(wrp *Wrap_Ask) {
	// if the map has not been finished ---> start for map allocation
	if len(c.map_done) != c.nMap {
		for i, _ := range c.files {
			if c.map_allocated[i] == nil && c.map_done[i] == nil {
				c.map_allocated[i] = wrp.req.ts
				go func(i int) {
					wrp.rep.state = progress
					wrp.rep.tp = MAP
					wrp.rep.index = i
					wrp.rep.file = c.files[i]
					<-wrp.tmp
				}(i)
				break
			}
		}
	} else { // for reduce
		for i := 0; i < c.nReduce; i += 1 {
			if c.reduce_allocated[i] == nil && c.reduce_done[i] == nil {
				c.reduce_allocated[i] = wrp.req.ts
				go func(i int) {
					wrp.rep.state = progress
					wrp.rep.tp = REDUCE
					wrp.rep.index = i
					<-wrp.tmp
				}(i)
				break
			}
		}
	}
}

// the loop will call this function to assign task: 1 MAP 2 REDUCE
// if the worker has already time out, but still doing their job
func (c *Coordinator) handle_done_chan(wrp *Wrap_Done) {
	if wrp.req.ts.tp == MAP {
		if c.map_allocated[wrp.req.ts.index] == nil {
			return
		}
		delete(c.map_allocated, wrp.req.ts.index)
		c.map_done[wrp.req.ts.index] = wrp.req.ts
	} else {
		if c.reduce_allocated[wrp.req.ts.index] == nil {
			return
		}
		delete(c.reduce_allocated, wrp.req.ts.index)
		c.reduce_done[wrp.req.ts.index] = wrp.req.ts
	}

	go func() {
		wrp.rep.state = idle
		<-wrp.tmp
	}()
}

// coordinator still have tasks, and worker has time out
func (c *Coordinator) handle_time_out(ts *Task) {
	ts.state = idle
	if ts.tp == MAP {
		delete(c.map_allocated, ts.index)
	} else {
		delete(c.reduce_allocated, ts.index)
	}
}
