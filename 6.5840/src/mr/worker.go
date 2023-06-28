package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"net/rpc"
	"os"

	"6.5840/logger"
)

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
	request := Request_Map_Task{}
	reply := Reply_Map_Task{}
	finished := call("Coordinator.RPC_Map_task", &request, &reply)
	if !finished {
		logger.Debug(logger.DLog, "Seems the coordinator has finished")
		return
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

	temp_files := make([]string, task.nReduce)
	hash_json := make([]*json.Encoder, task.nReduce)
	var fd *os.File
	for i := 0; i < task.nReduce; i += 1 {
		temp_files[i] = fmt.Sprintf("mr-%d-%d", task.index, i)
		path, _ := os.Getwd()
		fd, err = ioutil.TempFile(path, temp_files[i])
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

// ask the coordinate for the reduce task
// do the reduce task
func (task *Task) Reduce_Task(mapf func(string, string) []KeyValue) {

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
