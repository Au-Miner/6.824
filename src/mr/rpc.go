package mr

import "os"
import "strconv"

// Add your RPC definitions here.
type MapTask struct {
	FileName      string
	AllocSucc     bool // whether allocated successfully
	MapStatusOver bool // whether handle over. if over, we need go to next status
	MapTaskId     int  // mapTaskId == fileId
	NReduce       int  // nReduce
}

type ReduceTask struct {
	FileNum          int  // the total number of files == mr-max-*
	AllocSucc        bool // whether allocated successfully
	ReduceStatusOver bool // whether handle over. if over, we need go to close the job
	ReduceTaskId     int  // handle mr-*-ReduceTaskId
}

// Cook up a unique-ish UNIX-domain socket name
// in /var/tmp, for the master.
// Can't use the current directory since
// Athena AFS doesn't support UNIX-domain sockets.
func masterSock() string {
	s := "/var/tmp/824-mr-"
	s += strconv.Itoa(os.Getuid())
	return s
}
