package mr

import (
	"fmt"
 	"log"
 	"net/rpc"
	"hash/fnv"
	"time"
	"os"
	"encoding/gob"
	"io/ioutil"
	"encoding/json"
	"sort"
)

const (
	MAP_TASK int = 1
	REDUCE_TASK int = 2
	WAIT_TASK int = 3
)

type WaitTask struct {
	TimeToSleep time.Duration
}

type MapTask struct {
	MapTaskID	int
	InFilename 	string
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

type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

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
	gob.Register(TaskObject{})
	gob.Register(ReduceTask{})
	gob.Register(MapTask{})
	gob.Register(WaitTask{})

	mapfn = mapf
	reducefn = reducef

	for {
		task, status := RequestTask();
		if status {
			status = ProcessTask(task)
			if status {
				CallTaskDone(task)
			} else {
				fmt.Println("Error processing task")
			}
		} else {
			fmt.Println("Master Didn't respond, Exiting with code 3")
			os.Exit(3)
		}	
	}
}

func CallTaskDone(taskObj TaskObject) bool{
	args := TaskDoneArgs{}
	reply := TaskDoneReply{}

	args.TaskObj = taskObj

	status := call("Master.TaskDone", &args, &reply)
	
	return status
}

func ProcessTask(taskObj TaskObject) bool {
	switch taskObj.TaskType {
	case MAP_TASK:
		t, ok := taskObj.Task.(MapTask)
		if ok {
			file, err := os.Open(t.InFilename)
			if err != nil {
				fmt.Printf("cannot open %v", t.InFilename)
				return false
			}

			content, err := ioutil.ReadAll(file)
			if err != nil {
				fmt.Printf("cannot read %v", t.InFilename)
				return false
			}
			file.Close()
			kva := mapfn(t.InFilename, string(content))


			tempEncoders := make(map[int]*json.Encoder)
			tempFiles := make(map[int]*os.File)

			for _, kv := range kva {
				reducerID := ihash(kv.Key) % t.NumReducers + 1
				tmpEncoder, ok := tempEncoders[reducerID]
				var encoder *json.Encoder
				
				if ok {
					encoder = tmpEncoder
				} else {
					tmpfile, err := ioutil.TempFile(".", "tmp-file-*")
					if err != nil {
						fmt.Println("Error", err)
						return false
					}
					tempFiles[reducerID] = tmpfile
					tempEncoders[reducerID] = json.NewEncoder(tmpfile)
					encoder = tempEncoders[reducerID]
				}

				err := encoder.Encode(&kv)
				if err != nil {
					fmt.Println("Error", err)
					return false
				}
			}

			for reducerID, tmpfile := range tempFiles {
				tmpfile.Close()
				err := os.Rename(tmpfile.Name(), fmt.Sprintf("mr-%d-%d", t.MapTaskID, reducerID))
				if err != nil {
					fmt.Println("Error", err)
				}
			}

		} else {
			fmt.Println("Unknown task type", t)
		}
	case REDUCE_TASK:
		t, ok := taskObj.Task.(ReduceTask)
		if ok {
			kva := []KeyValue{}

			for _, filename := range t.InFilenames {
				f, err := os.Open(filename)
				if err != nil {
					continue
				}
				dec := json.NewDecoder(f)
				for {
					var kv KeyValue
					if err := dec.Decode(&kv); err != nil {
						break
					}
					kva = append(kva, kv)
				}

				if err != nil {
					fmt.Print("Error", err)
				}
			}

			sort.Sort(ByKey(kva))

			tmpOut, _ := ioutil.TempFile(".", "tmp-out-*")

			i := 0
			for i < len(kva) {
				j := i + 1
				for j < len(kva) && kva[j].Key == kva[i].Key {
					j++
				}
				values := []string{}
				for k := i; k < j; k++ {
					values = append(values, kva[k].Value)
				}
				output := reducefn(kva[i].Key, values)

				// this is the correct format for each line of Reduce output.
				fmt.Fprintf(tmpOut, "%v %v\n", kva[i].Key, output)

				i = j
			}

			tmpOut.Close()

			err := os.Rename(tmpOut.Name(), t.OutFilename)
			if err == nil {
				for _, filename := range t.InFilenames {
					err = os.Remove(filename)
				}
			}

		} else {
			fmt.Println("Unknown task type", t)
		}
	case WAIT_TASK:
		t, ok := taskObj.Task.(WaitTask)
		if ok {
			time.Sleep(t.TimeToSleep * time.Second)
		} else {
			fmt.Println("Unknown task type", t)
		}
	}

	return true
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
