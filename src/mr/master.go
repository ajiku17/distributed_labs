package mr

import (
	"log"
	"net"
	"os"
	"net/rpc"
	"net/http"
	"encoding/gob"
	"sync"
	"time"
	"fmt"
)

type Master struct {
	// Your definitions here.
	nextTaskID		  int
	currentPhaseTasks map[int]chan int // buffer of size 1
	doneTasks		  map[int]bool
	unscheduledTasks  []TaskObject
	mu 			      sync.Mutex
	waitGroup		  sync.WaitGroup
	done 			  bool
	mappers 		  int
	reducers 		  int
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

func (m *Master) GetNextTaskID() int {
	m.mu.Lock()
	defer m.mu.Lock()
	
	res := m.nextTaskID;
	m.nextTaskID++

	return res
}


func (m *Master) generateMapTasks(files []string, nReduce int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	const taskSize = 1024 * 256
	mapTaskID := 1

	for _, filename := range files {
		fmt.Println("chopping up", filename)
		_, err := os.Stat(filename)
		if err == nil {
			task := TaskObject {
				m.nextTaskID,
				MAP_TASK,
				MapTask {
					mapTaskID,
					filename,
					nReduce,
				},
			}

			fmt.Println(task)

			mapTaskID++;
			m.nextTaskID++
			m.mappers++;

			m.unscheduledTasks = append(m.unscheduledTasks, task)
			m.currentPhaseTasks[task.ID] = make(chan int, 1)
			m.doneTasks[task.ID] = false

			m.waitGroup.Add(1)
			
		}
	}
}

func (m *Master) generateReduceTasks(nReduce int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	// ReducetTaskID int
	// InFilenames   []string
	// OutFilename   string
	for reducer := 1; reducer <= nReduce; reducer++ {
		inFilenames := []string{}
		for mapper := 1; mapper <= m.mappers; mapper++ {
			inFilenames = append(inFilenames, fmt.Sprintf("mr-%d-%d", mapper, reducer))
		}
		
		task := TaskObject {
			m.nextTaskID,
			REDUCE_TASK,
			ReduceTask {
				reducer,
				inFilenames,
				fmt.Sprintf("mr-out-%d", reducer),
			},
		}
		m.nextTaskID++

		m.unscheduledTasks = append(m.unscheduledTasks, task)
		m.currentPhaseTasks[task.ID] = make(chan int, 1)
		m.doneTasks[task.ID] = false

		m.waitGroup.Add(1)
	}
}

func (m *Master) clearPhaseTasks() {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Println("clearing phase tasks")
	m.currentPhaseTasks = make(map[int]chan int)
	m.doneTasks = make(map[int]bool)
	m.unscheduledTasks = []TaskObject{}

}

func (m *Master) PhaseMonitor(files []string, nReduce int) {

	fmt.Println("================== MAP PHASE START ===================")
	fmt.Println("generating map tasks")
	m.generateMapTasks(files, nReduce)

	fmt.Println("waiting for map tasks")
	m.waitGroup.Wait() // wait for map tasks to finish
	fmt.Println("================== MAP PHASE FINISHED ===================")
	
	m.clearPhaseTasks()

	fmt.Println("================== REDUCE PHASE START ===================")
	fmt.Println("generating reduce tasks")
	m.generateReduceTasks(nReduce)

	fmt.Println("waiting for reduce tasks tasks")
	m.waitGroup.Wait() // wait for reduce tasks to finish
	fmt.Println("================== REDUCE PHASE FINISHED ===================")

	m.clearPhaseTasks()
	// TODO generate poison tasks (optional)
	fmt.Println("generating poison tasks")

	fmt.Println("marking self as done")
	
	m.mu.Lock()
	m.done = true
	m.mu.Unlock()
}

func (m *Master) MarkTaskAsDone(taskID int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Println("Marking task:", taskID, "as done")

	isDone, ok := m.doneTasks[taskID]
	if !ok {
		fmt.Println("received done on unknown task:", taskID)
	} else if !isDone {
		m.doneTasks[taskID] = true
		m.waitGroup.Done()
	}
}

func (m *Master) rescheduleTask(task TaskObject) {
	m.mu.Lock()
	defer m.mu.Unlock()

	m.unscheduledTasks = append(m.unscheduledTasks, task)
}

func (m *Master) watchdog(task TaskObject, doneChannel chan int) {
	timeout := time.After(10 * time.Second)

	select {
	case <- timeout:
		// TODO reschedule
		m.rescheduleTask(task)
		fmt.Println("rescheduling task:", task)
	case <- doneChannel:
		m.MarkTaskAsDone(task.ID)
	}
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	m.mu.Lock()
	defer m.mu.Unlock()

	fmt.Println("Get Task was called", len(m.unscheduledTasks))
	if len(m.unscheduledTasks) == 0 {
		taskObj := TaskObject{
			m.nextTaskID,
			WAIT_TASK,
			WaitTask{
				5,
			},
		}
		m.nextTaskID++
		fmt.Println(taskObj)
		reply.TaskObj = taskObj
	} else {

		task := m.unscheduledTasks[0]
		m.unscheduledTasks = m.unscheduledTasks[1:]

		reply.TaskObj = task
		go m.watchdog(task, m.currentPhaseTasks[task.ID])
	}

	fmt.Println("get task returns")	

	return nil
}

func (m *Master) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	task := args.TaskObj
	fmt.Println("in task done", task)
	m.mu.Lock()
	defer m.mu.Unlock()

	doneChannel, ok := m.currentPhaseTasks[task.ID]
	if ok {
		doneChannel <- 1 // mark as done for watchdog
	}

	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()

	//l, e := net.Listen("tcp", ":1234")
	sockname := masterSock()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatal("listen error:", e)
	}
	go http.Serve(l, nil)
}

//
// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
//
func (m *Master) Done() bool {
	m.mu.Lock()
	defer m.mu.Unlock()

	return m.done
}

//
// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	gob.Register(TaskObject{})
	gob.Register(ReduceTask{})
	gob.Register(MapTask{})
	gob.Register(WaitTask{})
	gob.Register(PoisonTask{})

	// Your code here.
	m.nextTaskID = 1
	m.currentPhaseTasks = make(map[int]chan int)
	m.unscheduledTasks = []TaskObject{}
	m.doneTasks = make(map[int]bool)
	m.reducers = nReduce
	m.mappers = 0
	m.done = false

	m.server()

	go m.PhaseMonitor(files, nReduce)

	return &m
}
