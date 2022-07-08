package mr

import (
	"encoding/json"
	"fmt"
	"hash/fnv"
	"io/ioutil"
	"log"
	"math"
	"net/rpc"
	"os"
	"path/filepath"
	"sort"
	"time"
)

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

//
// main/mrworker.go calls this function.
//
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// Your worker implementation here.

	info := WorkerInfo{}
	task := TaskInfo{}
	defer func() {
		// log.Printf("Worker fail, send message to coordinator: %v\n", info)
		if p := recover(); p != nil {
			log.Println("Restart 5 times, but always die!")
		}
	}()

	for {
		time.Sleep(time.Second)
		// regist worker
		if info.State != WORKER_REGIST {
			call("Coordinator.WorkerRegist", &NotNeccessary{}, &info)
			continue
		}
		if ok := call("Coordinator.AssignTask", &info, &task); !ok {
			// worker has failed
			// fmt.Printf("Worker-%v restart\n", info.No)
			restart(&info)
			continue
		}
		if task.IsEmpty() {
			time.Sleep(time.Second)
			continue
		}
		log.Printf("Worker-%v get Task: %v\n", info.No, task)
		if task.IsMapTask {
			MapTask(mapf, &task)
			// map task may out of time, so need to confirm this worker not fail
			task.State = TASK_CONFIRM
			task.UpdateTime = time.Now()
			if confirmTask := call("Coordinator.UpdateTask", &task, &NotNeccessary{}); confirmTask {
				RenameTask(&task)
				task.State = TASK_DONE
				task.UpdateTime = time.Now()
				call("Coordinator.UpdateTask", &task, &NotNeccessary{})
			}
		} else {
			ReduceTask(reducef, &task)
			task.State = TASK_DONE
			task.UpdateTime = time.Now()
			call("Coordinator.UpdateTask", &task, &NotNeccessary{})
		}
		task = TaskInfo{}
	}

	// uncomment to send the Example RPC to the coordinator.
	// CallExample()
}

// restart worker after
func restart(taskInfo *WorkerInfo) {
	log.Println("want to restart!")
	for {
		log.Printf("Worker Restart-%v: %v\n", taskInfo.restartTimes, taskInfo)
		// exp sleep time
		time.Sleep(time.Second * time.Duration(math.Pow(2, float64(taskInfo.restartTimes))))
		// time.Sleep(time.Second)
		newTaskInfo := WorkerInfo{}
		if restartSuccess := call("Coordinator.WorkerRestart", &taskInfo, &newTaskInfo); restartSuccess {
			break
		}
		*taskInfo = newTaskInfo
	}

}

// method to implement map task
func MapTask(mapf func(string, string) []KeyValue, task *TaskInfo) {
	// log.Printf("Worker-%v: Map Task-%v\n", task.WorkerNo, task.MapIdx)
	intermediate := [][]KeyValue{}
	for i := 0; i < task.NReduce; i++ {
		intermediate = append(intermediate, []KeyValue{})
	}

	file, err := os.Open(task.Filename)
	if err != nil {
		log.Fatalf("cannot open file: %v", task.Filename)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read file: %v", task.Filename)
	}
	defer file.Close()
	kva := mapf(task.Filename, string(content))
	for _, kv := range kva {
		idx := ihash(kv.Key) % task.NReduce
		intermediate[idx] = append(intermediate[idx], kv)
	}

	// save temp file names orderd by nReduce index
	task.TempFileName = []string{}
	for i := 0; i < task.NReduce; i++ {
		task.TempFileName = append(task.TempFileName, writeTempFile(&intermediate[i]))
	}
}

// rename temp file(me-temp-*.json) to intermediate file(mr-X-Y.json)
func RenameTask(task *TaskInfo) {
	// log.Printf("Worker-%v: working file-%v: dest file name: ./mr-%v-*.json\n", task.WorkerNo, task.MapIdx, task.MapIdx)
	for i := 0; i < len(task.TempFileName); i++ {
		jsonName := fmt.Sprintf("mr-%v-%v.json", task.MapIdx, i)
		if renameError := os.Rename(task.TempFileName[i], jsonName); renameError != nil {
			log.Fatalf("Rename temp file error: %v", renameError)
		}
	}
}

// reduce task method
func ReduceTask(reducef func(string, []string) string, task *TaskInfo) {
	// log.Printf("Worker-%v: Reduce Task-%v\n", task.WorkerNo, task.ReduceIdx)
	intermediate := []KeyValue{}

	if pwd, pwdErr := os.Getwd(); pwdErr == nil {
		filename := fmt.Sprintf("mr-*-%v.json", task.ReduceIdx)
		filepathNames, fileNameErr := filepath.Glob(filepath.Join(pwd, filename))
		if fileNameErr != nil {
			log.Fatalf("Can't get all file name: %v", fileNameErr)
		}
		for _, fileName := range filepathNames {
			curFileKVList := readJson(fileName)
			intermediate = append(intermediate, curFileKVList...)
		}
	} else {
		log.Fatalf("Can't get dir info: %v", pwdErr)
	}

	sort.Sort(ByKey(intermediate))

	// change by reduce worker index
	oname := fmt.Sprintf("mr-out-%v", task.ReduceIdx)
	ofile, _ := os.Create(oname)
	defer ofile.Close()

	i := 0
	for i < len(intermediate) {
		j := i + 1
		for j < len(intermediate) && intermediate[j].Key == intermediate[i].Key {
			j++
		}
		values := []string{}
		for k := i; k < j; k++ {
			values = append(values, intermediate[k].Value)
		}
		output := reducef(intermediate[i].Key, values)

		// this is the correct format for each line of Reduce output.
		fmt.Fprintf(ofile, "%v %v\n", intermediate[i].Key, output)

		i = j
	}
}

func writeTempFile(data *[]KeyValue) string {
	tempFilePath, tempFileError := ioutil.TempFile("", "mr-temp-*.json")
	if tempFileError != nil {
		log.Fatalf("cannot create temp file: %v", tempFileError)
	}
	defer tempFilePath.Close()

	jsonData, marshalError := json.Marshal(*data)
	if marshalError != nil {
		log.Fatalf("Encoder failed: %v", marshalError)
	}
	_, jsonWriteError := tempFilePath.Write(jsonData)
	if jsonWriteError != nil {
		log.Fatalf("write file failed: %v", jsonWriteError)
	}
	return tempFilePath.Name()
}

func readJson(fileName string) []KeyValue {
	filePtr, err := os.Open(fileName)
	if err != nil {
		log.Fatalf("Open file failed: %s", err)
	}
	defer filePtr.Close()

	tempKVList := []KeyValue{}
	decoder := json.NewDecoder(filePtr)
	err = decoder.Decode(&tempKVList)
	if err != nil {
		log.Fatalf("Decoder failed %v", err.Error())
	}

	return tempKVList
}

//
// example function to show how to make an RPC call to the coordinator.
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

//
// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
//
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
