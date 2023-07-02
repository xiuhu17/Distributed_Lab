package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"
	"sort"

	"6.5840/logger"
)

// idle --gettask--> progress --taskdone--> idle

// for sorting by key.
type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

type State int

const (
	idle State = iota
	progress
)

type Type int

const (
	MAP Type = iota
	REDUCE
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Task struct {
	State   State
	Tp      Type
	NReduce int
	NMap    int
	Index   int
	File    string
}

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go -> plugin -> eg: mrapps/wc.go -> Map & Reduce -> mapf & reducef -> mr/worker.go -> Worker(mapf, reducef)
// main/mrcoordinator.go 															-> mr/coordinator.go -> Makecoordinator(files []string, nReduce int)
// call(rpcname string, args interface{}, reply interface{}) bool 调用rpc
// call(rpcname string, args interface{}, reply interface{}) bool 判断coordinator是否结束
func Worker(mapf func(string, string) []KeyValue, reducef func(string, []string) string) {
	// Init the task
	task := Task{}
	end_start := task.Start_Task()
	if !end_start {
		return
	}
	for {
		end_ask := task.Ask_Task()
		if !end_ask {
			return
		}
		if task.Tp == MAP {
			task.Do_Map(mapf)
		} else {
			task.Do_Reduce(reducef)
		}
		end_done := task.Done_Task(task.Tp)
		if !end_done {
			return
		}
	}
}

func (task *Task) Start_Task() bool {
	logger.Debug(logger.DLog, "Start_Task started")

	request := Request_Start_Task{}
	reply := Reply_Start_Task{}
	finished := call("Coordinator.PRC_Start_Task", &request, &reply)
	if !finished {
		// logger.Debug(logger.DLog, "Seems the coordinator has finished")
		return finished
	}
	task.NMap = reply.NMap
	task.NReduce = reply.NRecude
	logger.Debug(logger.DLog, "Start_Task ended")

	return true
}

func (task *Task) Ask_Task() bool {
	// request the info from coordinator
	logger.Debug(logger.DLog, "Ask_Task started")
	request := Request_Ask_Task{Ts: task}
	reply := Reply_Ask_Task{}
	finished := call("Coordinator.RPC_Ask_Task", &request, &reply)
	if !finished {
		return finished
	}

	// assign the reply
	task.Tp = reply.Tp
	task.Index = reply.Index
	task.State = reply.State
	if reply.Tp == MAP {
		task.File = reply.File
	}
	logger.Debug(logger.DLog, "Ask_Task ended")
	return true
}

// ask the coordinate for the map task
// do the map task
func (task *Task) Do_Map(mapf func(string, string) []KeyValue) {
	// read the file
	logger.Debug(logger.DLog, "Do_Map Started")
	file, err := os.Open(task.File)
	if err != nil {
		logger.Debug(logger.DLog, "File can not be opened")
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		logger.Debug(logger.DLog, "File can not be read")
	}
	file.Close()
	kva := mapf(task.File, string(content))

	// temp files created and write into them
	temp_files := make([]string, task.NReduce)
	hash_json := make([]*json.Encoder, task.NReduce)
	for i := 0; i < task.NReduce; i += 1 {
		temp_files[i] = fmt.Sprintf("mr-%d-%d", task.Index, i)
		fd, err := ioutil.TempFile(".", temp_files[i])
		if err != nil {
			logger.Debug(logger.DLog, "File can not be encoded to json")
		}
		enc := json.NewEncoder(fd)
		hash_json[i] = enc
	}
	for _, kv := range kva {
		enc := hash_json[(ihash(kv.Key) % task.NReduce)]
		err := enc.Encode(&kv)
		if err != nil {
			logger.Debug(logger.DLog, "Can not write into json file")
		}
	}

	// rename the file
	for i, temp_file := range temp_files {
		useName := fmt.Sprintf("mr-%d-%d", task.Index, i)
		if _, err := os.Stat(useName); err == nil { // exists
			continue
		} else if os.IsNotExist(err) {
			os.Rename(temp_file, useName)
		} else {
			logger.Debug(logger.DLog, "Problem with renaming the file")
		}
	}
	logger.Debug(logger.DLog, "Do_Map Ended")
}

// ask the coordinate for the reduce task
// do the reduce task
func (task *Task) Do_Reduce(reducef func(string, []string) string) {
	logger.Debug(logger.DLog, "Do_Reduce Start")
	// read all files
	kva := []KeyValue{}
	for i := 0; i < task.NMap; i += 1 {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, task.Index))
		if err != nil {
			logger.Debug(logger.DLog, "File can not be opened")
		}
		dec := json.NewDecoder(file)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err != nil {
				break
			}
			kva = append(kva, kv)
		}
		file.Close()
	}

	// temp final file created and write into it
	temp_file := fmt.Sprintf("mr-out-%d", task.Index)
	fd, err := ioutil.TempFile(".", temp_file)
	if err != nil {
		logger.Debug(logger.DLog, "File can not be encoded to json")
	}

	// sort the kva, and deal with the data
	sort.Sort(ByKey(kva))
	for i := 0; i < len(kva); {
		j := i + 1
		for j < len(kva) && kva[j].Key == kva[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, kva[k].Value)
		}
		output := reducef(kva[i].Key, values)
		fmt.Fprintf(fd, "%v %v\n", kva[i].Key, output)
		i = j
	}

	// rename the file
	useName := fmt.Sprintf("mr-out-%d", task.Index)
	if _, err := os.Stat(useName); err == nil { // exists
		return
	} else if os.IsNotExist(err) {
		os.Rename(temp_file, useName)
	} else {
		logger.Debug(logger.DLog, "Problem with renaming the file")
	}
	logger.Debug(logger.DLog, "Do_Reduce End")
}

// indicate which kind of task has done
func (task *Task) Done_Task(tp Type) bool {
	logger.Debug(logger.DLog, "Done_Task started")
	request := Request_Done_Task{Ts: task}
	reply := Reply_Done_Task{}
	task.State = reply.State
	finished := call("Coordinator.RPC_Done_Task", &request, &reply)
	if !finished {
		// logger.Debug(logger.DLog, "Seems the coordinator has finished")
		return finished
	}
	logger.Debug(logger.DLog, "Done_Task ended")
	return true
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}
