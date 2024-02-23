package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

import (
	"encoding/json"
	"log"
	"os"
	"time"
)
import "strconv"

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

// 通过共享文件处理消息，应该不需要设置锁，需要学习读写文件的语法
// Add your RPC definitions here.

type RPCArgs struct {
	Socket     string `json:"socket"`
	HasConnect bool   `json:"has_connect"`
	//if connected, index of worked
	Index int32 `json:"index"`
}

type RPCReply struct {
	Socket string `json:"socket"`
	// allocate index when connected
	Index     int32 `json:"index"`
	Task      *Task `json:"task"`
	Online    bool  `json:"online"`
	ReduceNum int   `json:"reduce_n_um"`
}

type FileState struct {
	Socket  string `json:"socket"`
	LastEnd int64  `json:"last_end"`
}

type RPCTaskArgs struct {
	Type         int32
	TaskLocation string
	Index        int32
	Offset       int32
}

type RPCTaskReply struct {
	Task []*Task
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

func CreateSocketWorker2Master() (int64, error) {
	fileName := coordinatorSock()
	file, err := os.Create(fileName)
	if err != nil {
		log.Fatal(err)
		return 0, err
	}
	defer file.Close()
	req := RPCArgs{
		fileName, false, 0,
	}
	writeByte, err := json.Marshal(req)
	if err != nil {
		log.Fatal(err)
		return 0, err
	}
	_, err = file.Write(writeByte)
	if err != nil {
		log.Fatal(err)
		return 0, err
	}
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
		return 0, err
	}
	fileSize := fileInfo.Size()
	return fileSize, nil
}

func FileHasWrite(fileName string) (bool, error) {
	file, err := os.Open(fileName)

	if err != nil {
		log.Fatal(err)
		return false, err
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	if err != nil {
		log.Fatal(err)
		return false, err
	}
	fileSizeBefore := fileInfo.Size()

	for i := 0; i < 10; i++ {
		fileStat, err := file.Stat()
		if err != nil {
			log.Fatal(err)
			return false, err
		}
		fileSize := fileStat.Size()
		if fileSize > fileSizeBefore {
			return true, nil
		} else {
			time.Sleep(1 * time.Second)
		}
	}
	return false, nil
}
