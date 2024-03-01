package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	files       []string // save the file name
	filesStatus []int    // whether file has been handled
	// 0->not allocated 1->has been allocated 2->over
	status int // master status
	// 0->map status 1->reduce status 2->end
}

// Your code here -- RPC handlers for the worker to call.
func (m *Master) RequireMapTask(args *string, mapTask *MapTask) error {
	mapTask.MapStatusOver = m.status != 0
	mapTask.AllocSucc = false
	for idx, fileName := range m.files {
		if m.filesStatus[idx] == 0 {
			mapTask.FileName = fileName
			mapTask.FileIdx = idx
			mapTask.AllocSucc = true
			m.filesStatus[idx] = 1
			break
		}
	}
	if mapTask.AllocSucc == false {
		log.Println("all the files have been allocated!!!")
	}
	if mapTask.MapStatusOver == true {
		log.Println("all the files have been handled over!!!")
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (m *Master) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
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

// main/mrmaster.go calls Done() periodically to find out
// if the entire job has finished.
func (m *Master) Done() bool {
	return m.status == 2
}

// create a Master.
// main/mrmaster.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeMaster(files []string, nReduce int) *Master {
	m := Master{}

	// Your code here.
	m.status = 0
	m.files = append(m.files, files...)
	for i := 0; i < len(m.files); i++ {
		m.filesStatus = append(m.filesStatus, 0)
	}
	log.Printf("the len of is m.files %d", len(m.files))
	log.Printf("the len of is m.filesStatus %d", len(m.filesStatus))

	m.server()
	return &m
}
