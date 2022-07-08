package mr

import (
	"fmt"
	"log"
	"net"
	"net/http"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type Coordinator struct {
	// Your definitions here.
	NReduce     int
	mutex       sync.RWMutex
	Workers     []WorkerInfo
	MapTasks    []TaskInfo
	ReduceTasks []TaskInfo
}

// Your code here -- RPC handlers for the worker to call.

//
// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
//
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// regist worker in coordinator
func (c *Coordinator) WorkerRegist(args *NotNeccessary, curWorkerInfo *WorkerInfo) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	curWorkerInfo.No = len(c.Workers)
	curWorkerInfo.State = WORKER_REGIST
	curWorkerInfo.LastAliveTime = time.Now()
	c.Workers = append(c.Workers, *curWorkerInfo)

	return nil
}

func (c *Coordinator) WorkerRestart(preWorkerInfo *WorkerInfo, newWorkerInfo *WorkerInfo) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()

	// only has 5 times to restart worker
	if preWorkerInfo.restartTimes >= 5 {
		return fmt.Errorf("has reject too many times, will never regist to coordinator")
	}
	newWorkerInfo.No = preWorkerInfo.No * (-1)
	newWorkerInfo.State = WORKER_REGIST
	newWorkerInfo.restartTimes = preWorkerInfo.restartTimes + 1
	newWorkerInfo.LastAliveTime = time.Now()
	c.Workers = append(c.Workers, *newWorkerInfo)

	return nil
}

func (c *Coordinator) AssignTask(curWorkerInfo *WorkerInfo, taskInfo *TaskInfo) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	if curWorkerInfo.State != WORKER_REGIST && curWorkerInfo.No >= len(c.Workers) {
		return nil
	}
	c.Workers[curWorkerInfo.No].LastAliveTime = time.Now()

	// if woiker has failed, worker index will be negative
	if curWorkerInfo.State == WORKER_FAIL {
		taskInfo.WorkerNo *= -1
		return fmt.Errorf("current worker failed, need restart it")
	}

	// log.Printf("Coordinator: Worker-%v: want to get a task\n", curWorkerInfo.No)
	allMapDone := true
	// log.Printf("Worker-%v want to get map Task!\n", curWorkerInfo.No)
	for i := 0; i < len(c.MapTasks); i++ {
		outTime := c.MapTasks[i].UpdateTime.Add(time.Duration(10 * time.Second))
		if c.MapTasks[i].State == TASK_EXECUTE && time.Now().After(outTime) {
			c.MapTasks[i].State = TASK_INIT
		}

		if c.MapTasks[i].State == TASK_INIT {
			// this task don't be executed
			c.MapTasks[i].WorkerNo = curWorkerInfo.No
			c.MapTasks[i].State = TASK_EXECUTE
			c.MapTasks[i].UpdateTime = time.Now()
			*taskInfo = c.MapTasks[i]
			return nil
		}
		if c.MapTasks[i].State != TASK_DONE {
			// log.Printf("Map Task-%v not done: state: %v\n", i, c.MapTasks[i].State)
			// any task not done, won't begin reduce task
			allMapDone = false
		}
	}
	if !allMapDone {
		return nil
	}
	log.Printf("Worker-%v want to get reduce Task!\n", curWorkerInfo.No)
	// All Map task has Done
	for i := 0; i < len(c.ReduceTasks); i++ {
		outTime := c.ReduceTasks[i].UpdateTime.Add(time.Duration(10 * time.Second))
		if c.ReduceTasks[i].State == TASK_EXECUTE && time.Now().After(outTime) {
			c.ReduceTasks[i].State = TASK_INIT
		}
		if c.ReduceTasks[i].State == TASK_INIT {
			c.ReduceTasks[i].State = TASK_EXECUTE
			c.ReduceTasks[i].WorkerNo = curWorkerInfo.No
			c.ReduceTasks[i].UpdateTime = time.Now()
			*taskInfo = c.ReduceTasks[i]
			return nil
		}
	}
	return nil
}

func (c *Coordinator) UpdateTask(task *TaskInfo, reply *NotNeccessary) error {
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// check worker alive
	if c.Workers[task.WorkerNo].State == WORKER_FAIL {
		return fmt.Errorf("worker-%v has failed, try restart", task.WorkerNo)
	}
	// update heatbeat
	c.Workers[task.WorkerNo].LastAliveTime = time.Now()
	// update task info
	if task.IsMapTask {
		c.MapTasks[task.MapIdx] = *task
	} else {
		c.ReduceTasks[task.ReduceIdx] = *task
	}
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
	// ret := true

	// Your code here.
	c.mutex.Lock()
	defer c.mutex.Unlock()
	// confirm worker alive
	// the task which finished by failed worker should restart
	for i := 0; i < len(c.Workers); i++ {
		if c.Workers[i].State == WORKER_FAIL {
			continue
		}
		outOfTime := c.Workers[i].LastAliveTime.Add(time.Duration(10 * time.Second))
		if time.Now().After(outOfTime) {
			// worker out of time
			log.Printf("Worker-%v fail", i)
			c.Workers[i].LastAliveTime = time.Now()
			c.Workers[i].State = WORKER_FAIL
		}
		if c.Workers[i].State == WORKER_FAIL {
			for i := 0; i < len(c.MapTasks); i++ {
				if c.MapTasks[i].WorkerNo == i {
					c.MapTasks[i].State = TASK_INIT
				}
			}
			for i := 0; i < len(c.ReduceTasks); i++ {
				if c.ReduceTasks[i].WorkerNo == i {
					c.ReduceTasks[i].State = TASK_INIT
				}
			}
		}
	}

	// confirm all map task has done
	allMapDone := true
	for _, task := range c.MapTasks {
		if task.State != TASK_DONE {
			allMapDone = false
		}
	}

	// confirm all reduce task has done
	allReduceDone := true
	for _, task := range c.ReduceTasks {
		if task.State != TASK_DONE {
			allReduceDone = false
		}
	}

	return allMapDone && allReduceDone
}

//
// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
//
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{}

	// Your code here.
	c.NReduce = nReduce
	for i, fileName := range files {
		c.MapTasks = append(c.MapTasks,
			TaskInfo{IsMapTask: true, Filename: fileName, NReduce: nReduce, MapIdx: i})
	}

	for i := 0; i < nReduce; i++ {
		c.ReduceTasks = append(c.ReduceTasks, TaskInfo{IsMapTask: false, ReduceIdx: i})
	}

	c.server()
	return &c
}
