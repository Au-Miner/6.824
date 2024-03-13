package mr

import (
	"6.824/raft"
	"log"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Master struct {
	// Your definitions here.
	mu            sync.Mutex
	files         []string // save the file name
	mapTaskStatus []int    // mapTaskStatus == filesStatus(whether file has been handled)
	// 0->not allocated 1->has been allocated 2->over
	status int // master status
	// 0->map status 1->reduce status 2->end
	nReduce int // nReduce

	reduceTaskStatus []int // reduceTaskStatus == whether file mr-len(files)-idx has been handled
	// 0->not allocated 1->has been allocated 2->over
}

func (m *Master) UpdateStatus() {
	flag := true
	for _, value := range m.mapTaskStatus {
		if value != 2 {
			flag = false
			break
		}
	}
	m.status = func() int {
		if flag {
			return 1
		}
		return 0
	}()
	flag = true
	for _, value := range m.reduceTaskStatus {
		if value != 2 {
			flag = false
			break
		}
	}
	m.status = func() int {
		if flag {
			return 2
		}
		return m.status
	}()
}

const (
	timeout = 10 * time.Second
)

// Your code here -- RPC handlers for the worker to call.
func (m *Master) SolveMapTask(mapTaskId *int, mapTask *MapTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if *mapTaskId == -1 {
		mapTask.MapStatusOver = m.status != 0
		mapTask.AllocSucc = false
		for idx, fileName := range m.files {
			if m.mapTaskStatus[idx] == 0 {
				mapTask.FileName = fileName
				mapTask.AllocSucc = true
				mapTask.MapTaskId = idx
				mapTask.NReduce = m.nReduce
				m.mapTaskStatus[idx] = 1
				break
			}
		}
		if mapTask.MapStatusOver == true {
			raft.Logger.Info("all the files have been handled over!!!")
		}
		if mapTask.AllocSucc == false {
			raft.Logger.Info("all the files have been allocated!!!")
		} else {
			go func(mapTaskId int) {
				time.Sleep(timeout)
				if m.mapTaskStatus[mapTaskId] != 2 {
					m.mapTaskStatus[mapTaskId] = 0
				}
			}(mapTask.MapTaskId)
		}
	} else {
		m.mapTaskStatus[*mapTaskId] = 2
		m.UpdateStatus()
	}
	return nil
}

func (m *Master) SolveReduceTask(reduceTaskId *int, reduceTask *ReduceTask) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	if *reduceTaskId == -1 {
		reduceTask.ReduceStatusOver = m.status == 2
		reduceTask.AllocSucc = false
		for reduceTaskId := 0; reduceTaskId < m.nReduce; reduceTaskId++ {
			if m.reduceTaskStatus[reduceTaskId] == 0 {
				reduceTask.FileNum = len(m.files)
				reduceTask.AllocSucc = true
				reduceTask.ReduceTaskId = reduceTaskId
				m.reduceTaskStatus[reduceTaskId] = 1
				break
			}
		}
		if reduceTask.ReduceStatusOver == true {
			raft.Logger.Info("all the files have been handled over!!!")
		}
		if reduceTask.AllocSucc == false {
			raft.Logger.Info("all the files have been allocated!!!")
		} else {
			go func(ReduceTaskId int) {
				time.Sleep(timeout)
				if m.reduceTaskStatus[ReduceTaskId] != 2 {
					m.reduceTaskStatus[ReduceTaskId] = 0
				}
			}(reduceTask.ReduceTaskId)
		}
	} else {
		m.reduceTaskStatus[*reduceTaskId] = 2
		m.UpdateStatus()
	}
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
	m.nReduce = nReduce
	m.files = append(m.files, files...)
	for i := 0; i < len(m.files); i++ {
		m.mapTaskStatus = append(m.mapTaskStatus, 0)
	}
	for i := 0; i < nReduce; i++ {
		m.reduceTaskStatus = append(m.reduceTaskStatus, 0)
	}
	raft.Logger.Info("the len of is m.files %d", len(m.files))
	raft.Logger.Info("the len of is m.mapTaskStatus %d", len(m.mapTaskStatus))
	raft.Logger.Info("the len of is m.reduceTaskStatus %d", len(m.reduceTaskStatus))

	m.server()
	return &m
}
