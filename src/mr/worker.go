package mr

import (
	"fmt"
	"io/ioutil"
	"os"
	"sort"
	"strings"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}

type ByKey []KeyValue

// for sorting by key.
func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

func handleMapTask(mapTask MapTask, mapf func(string, string) []KeyValue) {
	file, err := os.Open(mapTask.FileName)
	if err != nil {
		log.Fatalf("cannot open1 %v: %v", mapTask.FileName, err)
	}
	content, err := ioutil.ReadAll(file)
	if err != nil {
		log.Fatalf("cannot read %v: %v", mapTask.FileName, err)
	}
	file.Close()
	kva := mapf(mapTask.FileName, string(content))
	keyStorePos := make([][]KeyValue, mapTask.NReduce)
	for i := 0; i < len(kva); i++ {
		reduceId := ihash(kva[i].Key) % mapTask.NReduce
		keyStorePos[reduceId] = append(keyStorePos[reduceId], KeyValue{kva[i].Key, kva[i].Value})
	}
	for fileId := 0; fileId < mapTask.NReduce; fileId++ {
		filePath := fmt.Sprintf("mr-%v-%v", mapTask.MapTaskId, fileId)
		ofile, err := os.Create(filePath)
		if err != nil {
			log.Fatalf("cannot create %v: %v", filePath, err)
		}
		for _, keyValue := range keyStorePos[fileId] {
			fmt.Fprintf(ofile, "%v %v\n", keyValue.Key, keyValue.Value)
		}
		ofile.Close()
	}
}

func handleReduceTask(reduceTask ReduceTask, reducef func(string, []string) string) {
	intermediate := []KeyValue{}
	for fileId := 0; fileId < reduceTask.FileNum; fileId++ {
		filePath := fmt.Sprintf("mr-%v-%v", fileId, reduceTask.ReduceTaskId)
		file, err := os.Open(filePath)
		if err != nil {
			log.Fatalf("cannot open2 %v: %v", filePath, err)
		}
		content, err := ioutil.ReadAll(file)
		if err != nil {
			log.Fatalf("cannot read %v: %v", filePath, err)
		}
		lines := strings.Split(string(content), "\n")
		for _, line := range lines {
			words := strings.Split(line, " ")
			if len(words) > 1 {
				intermediate = append(intermediate, KeyValue{words[0], words[1]})
			}
		}
	}
	sort.Sort(ByKey(intermediate))
	result := []KeyValue{}
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
		sum := reducef(intermediate[i].Key, values)
		result = append(result, KeyValue{intermediate[i].Key, sum})
		i = j
	}
	oname := fmt.Sprintf("mr-out-%v", reduceTask.ReduceTaskId)
	ofile, _ := os.Create(oname)
	for _, keyValue := range result {
		fmt.Fprintf(ofile, "%v %v\n", keyValue.Key, keyValue.Value)
	}
	ofile.Close()
}

func CallRequestMapTask() MapTask {
	mapTaskId := -1
	mapTask := MapTask{}
	call("Master.SolveMapTask", &mapTaskId, &mapTask)
	return mapTask
}

func CallResponseMapTask(mapTaskId int) {
	mapTask := MapTask{}
	call("Master.SolveMapTask", &mapTaskId, &mapTask)
}

func CallRequestReduceTask() ReduceTask {
	reduceTaskId := -1
	reduceTask := ReduceTask{}
	call("Master.SolveReduceTask", &reduceTaskId, &reduceTask)
	return reduceTask
}

func CallResponseReduceTask(reduceTaskId int) {
	reduceTask := MapTask{}
	call("Master.SolveReduceTask", &reduceTaskId, &reduceTask)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	// 0->require map task
	// if handle over, then goto 0
	// if map status over, then goto 1
	// 1->require reduce task
	// if handle over, then goto 1
	// if map status over, then goto 2
	// 2->end
	status := 0
	for {
		if status == 0 {
			Logger.Info("now prepare to require map task")
			mapTask := CallRequestMapTask()
			Logger.Info("mapTask: ", mapTask)
			if mapTask.AllocSucc {
				Logger.Info("require map task successfully")
				Logger.Info("prepare to handle map task")
				// handle map task code here
				handleMapTask(mapTask, mapf)
				CallResponseMapTask(mapTask.MapTaskId)
				Logger.Info("handle map task successfully")
			} else if mapTask.MapStatusOver {
				status = 1
				Logger.Info("===========================")
			} else {
				time.Sleep(time.Millisecond * 500)
			}
		} else if status == 1 {
			Logger.Info("now prepare to require reduce task")
			reduceTask := CallRequestReduceTask()
			Logger.Info("reduceTask: ", reduceTask)
			if reduceTask.AllocSucc {
				Logger.Info("require reduce task successfully")
				Logger.Info("prepare to handle reduce task")
				// handle map task code here
				handleReduceTask(reduceTask, reducef)
				CallResponseReduceTask(reduceTask.ReduceTaskId)
				Logger.Info("handle reduce task successfully")
			} else if reduceTask.ReduceStatusOver {
				status = 2
				// TODO: maybe we should kill this process(now we just return)
				return
			}
		}
		//time.Sleep(time.Millisecond * 500)
	}
}

// send an RPC request to the master, wait for the response.
// usually returns true.
// returns false if something goes wrong.
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
