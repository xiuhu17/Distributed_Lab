package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"path/filepath"
	"sync"
	"time"

	"6.5840/logger"
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
	map_time    map[int]time.Time
	reduce_time map[int]time.Time

	// channel for accepting data
	ask_chan   chan *Wrap_Ask
	done_chan  chan *Wrap_Done
	heart_beat chan *chan bool
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
func (c *Coordinator) Done() bool {
	ret := true
	return ret
}

// create a Coordinator.
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
	c.map_time = make(map[int]time.Time)
	c.reduce_time = make(map[int]time.Time)
	c.ask_chan = make(chan *Wrap_Ask)
	c.done_chan = make(chan *Wrap_Done)
	c.heart_beat = make(chan *chan bool)
	tmpPath := filepath.Join("mr-tmp")
	os.MkdirAll(tmpPath, os.ModePerm)

	// start the server
	c.server()

	// start those two goroutines
	var wg sync.WaitGroup
	wg.Add(2)
	go c.check_time_out(&wg)
	go c.loop(&wg)
	wg.Wait()

	return &c
}

func (c *Coordinator) PRC_Start_Task(request *Request_Start_Task, reply *Reply_Start_Task) error {
	reply.NMap = c.nMap
	reply.NRecude = c.nReduce
	return nil
}

func (c *Coordinator) RPC_Ask_Task(request *Request_Ask_Task, reply *Reply_Ask_Task) error {
	wrp := Wrap_Ask{Req: request, Rep: reply, Tmp: make(chan struct{})}
	c.ask_chan <- (&wrp) // used for for{select{case: case:}}, 并发
	<-wrp.Tmp            // used for wait purpose, handle_... function
	reply.Tp = wrp.Rep.Tp
	if reply.Tp == NONE {
		return nil
	} else if reply.Tp == FINISHED {
		return nil
	} else {
		reply.File = wrp.Rep.File
		reply.Index = wrp.Rep.Index
		reply.Tp = wrp.Rep.Tp
	}
	return nil
}

func (c *Coordinator) RPC_Done_Task(request *Request_Done_Task, reply *Reply_Done_Task) error {
	wrp := Wrap_Done{Req: request, Rep: reply, Tmp: make(chan struct{})}
	c.done_chan <- (&wrp)
	<-wrp.Tmp
	return nil
}

func (c *Coordinator) loop(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		select {
		case wrp := <-c.ask_chan:
			c.handle_ask_chan(wrp)
		case wrp := <-c.done_chan:
			c.handle_done_chan(wrp)
		case beat := <-c.heart_beat:
			if c.handle_time_out(beat) {
				return
			}
		}
	}
}

// the loop will call this function to assign task: 1 MAP 2 REDUCE
func (c *Coordinator) handle_ask_chan(wrp *Wrap_Ask) {
	// if the map has not been finished ---> start for map allocation
	if len(c.map_done) != c.nMap {
		for i, _ := range c.files {
			_, ok1 := c.map_allocated[i]
			_, ok2 := c.map_done[i]
			if !ok1 && !ok2 {
				c.map_allocated[i] = wrp.Req.Ts
				c.map_time[i] = time.Now()
				wrp.Rep.Tp = MAP
				wrp.Rep.Index = i
				wrp.Rep.File = c.files[i]
				logger.Debug(logger.DInfo, "Assign: ts_pointer: %p, ts_idx: %v", wrp.Req.Ts, wrp.Rep.Index)
				wrp.Tmp <- struct{}{}
				return
			}
		}
		wrp.Rep.Tp = NONE
		wrp.Tmp <- struct{}{}
		return
	}

	if len(c.map_done) == c.nMap && len(c.reduce_done) != c.nReduce { // for reduce
		for i := 0; i < c.nReduce; i += 1 {
			_, ok1 := c.reduce_allocated[i]
			_, ok2 := c.reduce_done[i]
			if !ok1 && !ok2 {
				c.reduce_allocated[i] = wrp.Req.Ts
				c.reduce_time[i] = time.Now()
				wrp.Rep.Tp = REDUCE
				wrp.Rep.Index = i
				wrp.Tmp <- struct{}{}
				return
			}
		}
		wrp.Rep.Tp = NONE
		wrp.Tmp <- struct{}{}
		return
	}

	wrp.Rep.Tp = FINISHED
	wrp.Tmp <- struct{}{}
}

// the loop will call this function to handle done task which is sent by worker
func (c *Coordinator) handle_done_chan(wrp *Wrap_Done) {
	// handle main logic
	if wrp.Req.Ts.Tp == MAP {
		delete(c.map_time, wrp.Req.Ts.Index)
		delete(c.map_allocated, wrp.Req.Ts.Index)
		c.map_done[wrp.Req.Ts.Index] = wrp.Req.Ts
	} else if wrp.Req.Ts.Tp == REDUCE {
		delete(c.reduce_time, wrp.Req.Ts.Index)
		delete(c.reduce_allocated, wrp.Req.Ts.Index)
		c.reduce_done[wrp.Req.Ts.Index] = wrp.Req.Ts
	}

	if len(c.reduce_done) == c.nReduce {
		wrp.Rep.Dn = true
	} else {
		wrp.Rep.Dn = false
	}

	wrp.Tmp <- struct{}{}
}

// coordinator still have tasks, and worker has time out
// delete the assigned work and delete the time
func (c *Coordinator) handle_time_out(beat *chan bool) bool {
	if len(c.reduce_done) == c.nReduce {
		(*beat) <- true
		return true
	}

	for key, value := range c.map_time {
		if time.Since(value).Seconds() > 10 {
			delete(c.map_time, key)
			delete(c.map_allocated, key)
		}
	}

	for key, value := range c.reduce_time {
		if time.Since(value).Seconds() > 10 {
			delete(c.reduce_time, key)
			delete(c.reduce_allocated, key)
		}
	}

	(*beat) <- false
	return false
}

// change check_time_out to time.sleep(time.second)
func (c *Coordinator) check_time_out(wg *sync.WaitGroup) {
	defer wg.Done()
	for {
		time.Sleep(time.Second)
		beat := make(chan bool)
		c.heart_beat <- &beat
		if res := <-beat; res {
			return
		}
	}
}
