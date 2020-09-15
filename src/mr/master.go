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
)

type TaskObject struct {
	ID 	 int
	Task Task
}

type Master struct {
	// Your definitions here.
	nextTaskID		  int
	currentPhaseTasks map[int]chan int // buffer of size 1
	doneTasks		  map[int]bool
	unscheduledTasks  []TaskObject
	mu 			      sync.Mutex
	waitGroup		  sync.WaitGroup
	done 			  bool
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
func (m *Master) PhaseMonitor() {

	// TODO generate map tasks

	m.waitGroup.Wait() // wait for map tasks to finish
	
	// TODO generate reduce tasks
	
	m.waitGroup.Wait() // wait for reduce tasks to finish
	
	// TODO generate poison tasks

	m.mu.Lock()
	m.done = true
	m.mu.Unlock()
}

func (m *Master) MarkTaskAsDone(taskID int) {
	m.mu.Lock()
	defer m.mu.Unlock()

	isDone, ok := m.doneTasks[taskID]
	if !ok || !isDone {
		m.doneTasks[taskID] = true
		m.waitGroup.Add(1)
	}
}

func (m *Master) watchdog(taskID int, doneChannel chan int) {
	timeout := time.After(10 * time.Second)

	select {
	case <- timeout:
		// TODO reschedule
	case <- doneChannel:
		m.MarkTaskAsDone(taskID)
	}
}

func (m *Master) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {

	if len(m.unscheduledTasks) == 0 {
		task := TaskObject{
			m.GetNextTaskID(),
			Task{
				WAIT_TASK,
				WaitTask{
					2,
				},
			},
		}

		reply.TaskObj = task
	} else {
		m.mu.Lock()
		defer m.mu.Unlock()

		task := m.unscheduledTasks[0]
		m.unscheduledTasks = m.unscheduledTasks[1:]
		

		reply.TaskObj = task
		go m.watchdog(task.ID, m.currentPhaseTasks[task.ID])
	}

	return nil
}

func (m *Master) TaskDone(args *TaskDoneArgs, reply *TaskDoneReply) error {
	task := args.TaskObj
	
	m.mu.Lock()
	defer m.mu.Unlock()

	m.currentPhaseTasks[task.ID] <- 1 // mark as done for watchdog

	return nil
}


//
// start a thread that listens for RPCs from worker.go
//
func (m *Master) server() {
	rpc.Register(m)
	rpc.HandleHTTP()

	gob.Register(Task{})
	gob.Register(ReduceTask{})
	gob.Register(MapTask{})
	gob.Register(WaitTask{})
	gob.Register(PoisonTask{})

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

	// Your code here.
	m.nextTaskID = 1
	m.currentPhaseTasks = make(map[int]chan int)
	m.done = false

	m.server()
	return &m
}
