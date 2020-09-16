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
	POISON_TASK int = 4
)

type PoisonTask struct {
	
}

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
	gob.Register(PoisonTask{})

	// uncomment to send the Example RPC to the master.
	// CallExample()
	mapfn = mapf
	reducefn = reducef

	for {
		task, status := RequestTask();
		if status {
			status = ProcessTask(task)
			if status {
				CallTaskDone(task)
			} else {
				fmt.Println("!!!!!!!!!ERROR processing task")
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
	fmt.Println("processing task")
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
				log.Fatalf("cannot read %v", t.InFilename)
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

			fmt.Println("RENAMING", tempFiles)

			for reducerID, tmpfile := range tempFiles {
				tmpfile.Close()
				err := os.Rename(tmpfile.Name(), fmt.Sprintf("mr-%d-%d", t.MapTaskID, reducerID))
				if err != nil {
					fmt.Println("error renaming", err)
				}
			}

		} else {
			fmt.Println("Unknown task type", t)
		}
	case REDUCE_TASK:
		t, ok := taskObj.Task.(ReduceTask)
		if ok {
			fmt.Println("Processing reduce task", t)

			kva := []KeyValue{}

			for _, filename := range t.InFilenames {
				f, err := os.Open(filename)
				if err != nil {
					fmt.Println("Error", err)
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

				err = os.Remove(filename)
				if err != nil {
					fmt.Print("Error", err)
				}
			}

			sort.Sort(ByKey(kva))

			ofile, _ := os.Create(t.OutFilename)

			//
			// call Reduce on each distinct key in intermediate[],
			// and print the result to mr-out-0.
			//
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
				fmt.Fprintf(ofile, "%v %v\n", kva[i].Key, output)

				i = j
			}

			ofile.Close()

		} else {
			fmt.Println("Unknown task type", t)
		}
	case WAIT_TASK:
		t, ok := taskObj.Task.(WaitTask)
		if ok {
			fmt.Println("Processing wait task", t)
			time.Sleep(t.TimeToSleep * time.Second)
		} else {
			fmt.Println("Unknown task type", t)
		}
	case POISON_TASK:
		t, ok := taskObj.Task.(PoisonTask)
		if ok {
			fmt.Println("Processing poison task", t)
			os.Exit(3)
		} else {
			fmt.Println("Unknown task type", t)
		}

	}

	return true
}

func RequestTask() (TaskObject, bool){
	fmt.Println("requesting task")
	t, status := CallGetTask()
	fmt.Println("received a task", t)
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
