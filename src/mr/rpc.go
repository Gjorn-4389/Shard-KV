package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"os"
	"reflect"
	"strconv"
	"time"
)

//
// example to show how to declare the arguments
// and reply for an RPC.
//

type ExampleArgs struct {
	X int
}

type ExampleReply struct {
	Y int
}

// Add your RPC definitions here.

const (
	WORKER_CREATE int = 0
	WORKER_REGIST int = 1
	WORKER_FAIL   int = 2

	TASK_INIT    int = 0
	TASK_EXECUTE int = 1
	TASK_CONFIRM int = 2
	TASK_DONE    int = 4
)

type NotNeccessary struct{}

type WorkerInfo struct {
	State         int
	No            int
	restartTimes  int
	LastAliveTime time.Time
}

type TaskInfo struct {
	// Your definitions here.
	// Task State
	State      int
	UpdateTime time.Time
	MapIdx     int
	ReduceIdx  int

	// WorkerNo who finish this task
	WorkerNo int

	// Task Type
	IsMapTask bool

	// Map Task Info
	Filename     string
	NReduce      int
	TempFileName []string
}

func (t TaskInfo) IsEmpty() bool {
	return reflect.DeepEqual(t, TaskInfo{})
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the coordinator.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func coordinatorSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
