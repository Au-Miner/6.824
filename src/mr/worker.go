package mr

import (
	"fmt"
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

// use ihash(key) % NReduce to choose the reduce
// task number for each KeyValue emitted by Map.
func ihash(key string) int {
	h := fnv.New32a()
	h.Write([]byte(key))
	return int(h.Sum32() & 0x7fffffff)
}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	// Your worker implementation here.

	// uncomment to send the Example RPC to the master.
	//CallExample()

	status := 0
	// 0->require map task
	// if handle over, then goto 0
	// if map status over, then goto 1
	// 1->require reduce task
	// if handle over, then goto 1
	// if map status over, then goto 2
	// 2->end
	for {
		if status == 0 {
			log.Println("now prepare to require map task")
			mapTask := CallRequireMapTask()
			log.Println("mapTask: ", mapTask)
			if mapTask.AllocSucc {
				log.Println("require map task successfully")
				log.Println("now prepare to handle map task")
				// handle map task code here
				log.Println("handle map task successfully")
			} else if mapTask.MapStatusOver {
				status = 1
			}
		} else if status == 1 {
			log.Println("now prepare to require reduce task")
		}
		time.Sleep(time.Second)
	}
}

// example function to show how to make an RPC call to the master.
//
// the RPC argument and reply types are defined in rpc.go.
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

func CallRequireMapTask() MapTask {
	args := ""
	mapTask := MapTask{}
	call("Master.RequireMapTask", &args, &mapTask)
	return mapTask
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
