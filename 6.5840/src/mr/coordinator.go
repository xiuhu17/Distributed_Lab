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
	task_time map[*Task]time.Time

	// channel for accepting data
	ask_chan   chan *Wrap_Ask
	done_chan  chan *Wrap_Done
	heart_beat chan chan bool
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
	tmpPath := filepath.Join("mr-tmp")
	os.MkdirAll(tmpPath, os.ModePerm)
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
	c.heart_beat = make(chan chan bool)

	// start the server
	c.server()

	// start those two goroutines
	var wg sync.WaitGroup
	wg.Add(2)
	go c.loop(&wg)
	go c.check_time_out(&wg)
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
	<-wrp.Tmp            // used for wait purpose
	reply.File = wrp.Rep.File
	reply.Index = wrp.Rep.Index
	reply.Tp = wrp.Rep.Tp
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
		if len(c.reduce_done) == c.nReduce {
			break
		}
		select {
		case wrp := <-c.ask_chan:
			c.handle_ask_chan(wrp)
		case wrp := <-c.done_chan:
			c.handle_done_chan(wrp)
		case beat := <-c.heart_beat:
			if c.handle_time_out(&beat) {
				os.RemoveAll("mr-tmp/")
				return
			}
		default:
			logger.Debug(logger.DLog, "Problem with the server loop goroutine")
		}
	}
}

// the loop will call this function to assign task: 1 MAP 2 REDUCE
func (c *Coordinator) handle_ask_chan(wrp *Wrap_Ask) {
	// if the map has not been finished ---> start for map allocation
	logger.Debug(logger.DLog, "handle_ask_chan Start")
	if len(c.map_done) != c.nMap {
		for i, _ := range c.files {
			if c.map_allocated[i] == nil && c.map_done[i] == nil {
				c.map_allocated[i] = wrp.Req.Ts
				c.task_time[wrp.Req.Ts] = time.Now()
				wrp.Rep.Tp = MAP
				wrp.Rep.Index = i
				wrp.Rep.File = c.files[i]
				wrp.Tmp <- struct{}{}
				break
			}
		}
	} else { // for reduce
		for i := 0; i < c.nReduce; i += 1 {
			if c.reduce_allocated[i] == nil && c.reduce_done[i] == nil {
				c.reduce_allocated[i] = wrp.Req.Ts
				c.task_time[wrp.Req.Ts] = time.Now()
				wrp.Rep.Tp = REDUCE
				wrp.Rep.Index = i
				wrp.Tmp <- struct{}{}
				break
			}
		}
	}
	logger.Debug(logger.DLog, "handle_ask_chan End")
}

// the loop will call this function to handle done task which is sent by worker
func (c *Coordinator) handle_done_chan(wrp *Wrap_Done) {
	logger.Debug(logger.DLog, "handle_done_chan Start")
	// handle time stamp
	delete(c.task_time, wrp.Req.Ts)

	// handle main logic
	if wrp.Req.Ts.Tp == MAP {
		if c.map_allocated[wrp.Req.Ts.Index] != nil { // not time out
			delete(c.map_allocated, wrp.Req.Ts.Index)
			c.map_done[wrp.Req.Ts.Index] = wrp.Req.Ts
		}
	} else {
		if c.reduce_allocated[wrp.Req.Ts.Index] != nil { // not time out
			delete(c.reduce_allocated, wrp.Req.Ts.Index)
			c.reduce_done[wrp.Req.Ts.Index] = wrp.Req.Ts
		}
	}

	// send back to client
	if len(c.reduce_done) == c.nReduce {
		wrp.Rep.dn = true
	} else {
		wrp.Rep.dn = false
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

	collect := []*Task{}
	for key, value := range c.task_time {
		if elapsed := time.Since(value); elapsed.Seconds() >= 10.00 {
			collect = append(collect, key)
		}
	}

	for _, ts := range collect {
		delete(c.task_time, ts)
		if ts.Tp == MAP {
			delete(c.map_allocated, ts.Index)
		} else {
			delete(c.reduce_allocated, ts.Index)
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
		c.heart_beat <- beat
		if <-beat {
			return
		}
	}
}
