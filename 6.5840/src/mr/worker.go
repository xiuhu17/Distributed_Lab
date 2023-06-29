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
	completed
)

type Type int

const (
	MAP    Type = iota
	REDUCE Type = iota
)

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type Task struct {
	state   State
	nReduce int
	nMap    int
	index   int
	file    string
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

	// Your worker implementation here.
	// Init the task
	task := Task{}
	request := Request_Start_Task{}
	reply := Reply_Start_Task{}
	finished := call("Coordinator.PRC_Start_Task", &request, &reply)
	if !finished {
		logger.Debug(logger.DLog, "Seems the coordinator has finished")
		return
	}
	task.nMap = reply.nMap
	task.nReduce = reply.nRecude
	task.state = idle

}

// ask the coordinate for the map task
// do the map task
func (task *Task) Map_Task(mapf func(string, string) []KeyValue) {
	// set state
	task.state = progress

	// request the info from coordinator
	request := Request_Map_Task{}
	reply := Reply_Map_Task{}
	finished := call("Coordinator.RPC_Map_Task", &request, &reply)
	if !finished {
		logger.Debug(logger.DLog, "Seems the coordinator has finished")
	}
	task.file = reply.file
	task.index = reply.index

	// read the file
	file, err := os.Open(task.file)
	if err != nil {
		logger.Debug(logger.DLog, "File can not be opened")
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		logger.Debug(logger.DLog, "File can not be read")
	}
	file.Close()
	kva := mapf(task.file, string(content))

	// temp files created and write into them
	temp_files := make([]string, task.nReduce)
	hash_json := make([]*json.Encoder, task.nReduce)
	for i := 0; i < task.nReduce; i += 1 {
		temp_files[i] = fmt.Sprintf("mr-%d-%d", task.index, i)
		fd, err := ioutil.TempFile(".", temp_files[i])
		if err != nil {
			logger.Debug(logger.DLog, "File can not be encoded to json")
		}
		enc := json.NewEncoder(fd)
		hash_json[i] = enc
	}
	for _, kv := range kva {
		enc := hash_json[(ihash(kv.Key) % task.nReduce)]
		err := enc.Encode(&kv)
		if err != nil {
			logger.Debug(logger.DLog, "Can not write into json file")
		}
	}

	// rename the file
	for i, temp_file := range temp_files {
		os.Rename(temp_file, fmt.Sprintf("mr-%d-%d", task.index, i))
	}

	// set state
	task.state = idle
}

// ask the coordinate for the reduce task
// do the reduce task
func (task *Task) Reduce_Task(reducef func(string, []string) string) {
	// set state
	task.state = progress

	// request the info from coordinator
	request := Request_Reduce_Task{}
	reply := Reply_Reduce_Task{}
	finished := call("Coordinator.RPC_Reduce_Task", &request, &reply)
	if !finished {
		logger.Debug(logger.DLog, "Seems the coordinator has finished")
	}
	task.index = reply.index

	// read all files
	kva := []KeyValue{}
	for i := 0; i < task.nMap; i += 1 {
		file, err := os.Open(fmt.Sprintf("mr-%d-%d", i, task.index))
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
	temp_file := fmt.Sprintf("mr-out-%d", task.index)
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
	os.Rename(temp_file, fmt.Sprintf("mr-out-%d", task.index))

	// set state
	task.state = idle
}

// indicate which kind of task has done
func (task *Task) Task_Done(tp Type) {
	request := Request_Task_Done{tp: tp, ts: task}
	reply := Reply_Task_Done{}
	finished := call("Coordinator.RPC_Task_Done", &request, &reply)
	if !finished {
		logger.Debug(logger.DLog, "Seems the coordinator has finished")
	}
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
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
