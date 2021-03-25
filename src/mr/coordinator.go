package mr

import (
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type TaskStatus string

const (
	Idle       TaskStatus = "Idle"
	InProgress TaskStatus = "InProgress"
	Completed  TaskStatus = "Completed"
)

type Task struct {
	id       int        // Map id or reduce id
	taskType TaskType   // Task type
	status   TaskStatus // Task status
	fileName string     // Map input filename
}

type Coordinator struct {
	// Your definitions here.
	mutex              sync.Mutex // Mutual exclusive lock
	cond               *sync.Cond // Condition variable
	mapTasks           []Task     // Map tasks
	reduceTasks        []Task     // Reduce tasks
	nMap               int        // Map number
	nReduce            int        // Reduce number
	nUncompletedMap    int        // Uncompleted map number
	nUncompletedReduce int        // Uncompleted reduce number
	completed          bool       // Entire tasks are completed
}

func (c *Coordinator) timeout(t *Task) {
	time.Sleep(10 * time.Second)
	c.mutex.Lock()
	if t.status == InProgress {
		t.status = Idle
		c.cond.Signal()
	}
	c.mutex.Unlock()
}

func (c *Coordinator) requestMapTask(reply *RequestTaskReply) bool {
	for i, t := range c.mapTasks {
		if t.status == Idle {
			reply.Id = t.id
			reply.Type = t.taskType
			reply.FileName = t.fileName
			reply.N = c.nReduce
			c.mapTasks[i].status = InProgress
			go c.timeout(&c.mapTasks[i])
			return true
		}
	}
	return false
}

func (c *Coordinator) requestReduceTask(reply *RequestTaskReply) bool {
	for i, t := range c.reduceTasks {
		if t.status == Idle {
			reply.Id = t.id
			reply.Type = t.taskType
			reply.N = c.nMap
			c.reduceTasks[i].status = InProgress
			go c.timeout(&c.reduceTasks[i])
			return true
		}
	}
	return false
}

func (c *Coordinator) RequestTask(args *RequestTaskArgs, reply *RequestTaskReply) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	for {
		if c.nUncompletedMap > 0 {
			if c.requestMapTask(reply) {
				return nil
			}
		} else if c.nUncompletedReduce > 0 {
			if c.requestReduceTask(reply) {
				return nil
			}
		} else {
			break
		}
		c.cond.Wait()
	}
	reply.Type = Exit
	return nil
}

func (c *Coordinator) finishMapTask(id int) {
	if c.mapTasks[id].status == InProgress {
		c.mapTasks[id].status = Completed
		c.nUncompletedMap--
		if c.nUncompletedMap == 0 {
			c.cond.Broadcast()
		}
	}
}

func (c *Coordinator) finishReduceTask(id int) {
	if c.reduceTasks[id].status == InProgress {
		c.reduceTasks[id].status = Completed
		c.nUncompletedReduce--
		if c.nUncompletedReduce == 0 {
			c.cond.Broadcast()
			c.completed = true
		}
	}
}

// Your code here -- RPC handlers for the worker to call.
func (c *Coordinator) FinishTask(arg *FinishTaskArgs, reply *FinishTaskReply) error {
	log.Printf("Finish task %v\n", arg)
	id := arg.Id
	c.mutex.Lock()
	if arg.Type == Map {
		c.finishMapTask(id)
	} else if arg.Type == Reduce {
		c.finishReduceTask(id)
	} else {
		log.Printf("unknown task type %v\n", arg.Type)
	}
	c.mutex.Unlock()
	return nil
}

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

//
// start a thread that listens for RPCs from worker.go
//
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

//
// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
//
func (c *Coordinator) Done() bool {
	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	return c.completed
}

func (c *Coordinator) initMapTasks(files *[]string) {
	for i, f := range *files {
		task := Task{
			id:       i,
			fileName: f,
			status:   Idle,
			taskType: Map,
		}
		c.mapTasks = append(c.mapTasks, task)
	}
	c.nMap = len(*files)
	c.nUncompletedMap = c.nMap
}

func (c *Coordinator) initReduceTasks(nReduce int) {
	for i := 0; i < nReduce; i++ {
		task := Task{
			id:       i,
			status:   Idle,
			taskType: Reduce,
		}
		c.reduceTasks = append(c.reduceTasks, task)
	}
	c.nReduce = nReduce
	c.nUncompletedReduce = c.nReduce
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.initMapTasks(&files)
	c.initReduceTasks(nReduce)
	c.cond = sync.NewCond(&c.mutex)

	c.server()
	return &c
}
