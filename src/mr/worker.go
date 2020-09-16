package mr

import (
	"fmt"
 	"log"
 	"net/rpc"
	"hash/fnv"
	"time"
	"os"
)

const (
	MAP_TASK int = 1
	REDUCE_TASK int = 2
	WAIT_TASK int = 3
	POISON_TASK int = 4
)

type PoisonTask struct {
	
}

type WaitTask struct {
	TimeToSleep time.Duration
}

type MapTask struct {
	MapTaskID	int
	InFileName 	string
	Offset		int
	Size		int
	NumReducers int
}

type ReduceTask struct {
	ReducetTaskID int
	InFilenames   []string
	OutFilename   string
}

type TaskObject struct {
	ID		 int
	TaskType int
	Task 	 interface{}
}

//
// Map functions return a slice of KeyValue.
//
type KeyValue struct {
	Key   string
	Value string
}

//
// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
//
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

var mapfn func(string, string) []KeyValue
var reducefn func(string, []string) string

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	// CallExample()
	mapfn = mapf
	reducefn = reducef

	for {
		task, status := RequestTask();
		if status {
			ProcessTask(task)
			CallTaskDone(task)
		} else {
			fmt.Println("Master Didn't respond, Exiting with code 3")
			os.Exit(3)
		}	
	}
}

func CallTaskDone(taskObj TaskObject) bool{
	args := TaskDoneArgs{}
	reply := TaskDoneReply{}

	status := call("Master.TaskDone", &args, &reply)
	
	return status
}

func ProcessTask(taskObj TaskObject) {
	switch taskObj.TaskType {
	case MAP_TASK:
		t, ok := taskObj.Task.(MapTask)
		if ok {

		} else {
			fmt.Println("Unknown task type", t)
		}
	case REDUCE_TASK:
		t, ok := taskObj.Task.(ReduceTask)
		if ok {

		} else {
			fmt.Println("Unknown task type", t)
		}
	case WAIT_TASK:
		t, ok := taskObj.Task..(WaitTask)
		if ok {
			time.Sleep(t.TimeToSleep * time.Second)
		} else {
			fmt.Println("Unknown task type", t)
		}
	case POISON_TASK:
		t, ok := taskObj.Task.(PoisonTask)
		if ok {
			os.Exit(3)
		} else {
			fmt.Println("Unknown task type", t)
		}

	}
}

func RequestTask() (TaskObject, bool){
	t, status := CallGetTask()
	return t, status
}

func CallGetTask() (TaskObject, bool) {
	args := GetTaskArgs{}
	reply := GetTaskReply{}

	status := call("Master.GetTask", &args, &reply)
	
	return reply.TaskObj, status
}

//
// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
//
func CallExample() {

	// declare an argument structure.
	args := ExampleArgs{}

	// fill in the argument(s).
	args.X = 99

	// declare a reply structure.
	reply := ExampleReply{}

	// send the RPC request, wait for the reply.
	call("Master.Example", &args, &reply)

	// reply.Y should be 100.
	fmt.Printf("reply.Y %v\n", reply.Y)
}

//
// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := masterSock()
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
