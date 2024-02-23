package mr

import (
	"bufio"
	"encoding/json"
	"fmt"
	"io/ioutil"
	_ "net/http/pprof"
	"os"
	"sort"
	"strconv"
	"time"
)
import "log"
import "net/rpc"
import "hash/fnv"

type WorkerData struct {
	Index     int32
	Status    int32 // 0默认值，1表示已注册待分配任务，2表示运行中，-1表示已经关闭
	ReduceNum int
	Map       func(string, string) []KeyValue
	Reduce    func(string, []string) string
	Task      *Task
}

// Map functions return a slice of KeyValue.
type KeyValue struct {
	Key   string
	Value string
}
type ByKey []KeyValue

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

func (w *WorkerData) ReportStatus() {
	for {
		args := RPCArgs{
			Index: w.Index,
		}
		reply := RPCReply{}
		call("Coordinator.ReportStatus", &args, &reply)
		time.Sleep(4 * time.Second)
	}

}

// main/mrworker.go calls this function.
func Worker(mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {
	reply := Register()
	if reply == nil {
		//		fmt.Printf("Register failed, exit")
		return
	}
	//	fmt.Println("注册成功 且首次任务id:" + strconv.Itoa(int(reply.Index)))
	worker := WorkerData{
		Index:     reply.Index,
		Map:       mapf,
		Reduce:    reducef,
		Task:      reply.Task,
		ReduceNum: reply.ReduceNum,
	}
	f, _ := os.OpenFile("WorkerLog.log"+strconv.Itoa(int(reply.Index)), os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)
	defer f.Close()
	log.SetOutput(f)
	go worker.ReportStatus()
	//go func() {
	//	_ = http.ListenAndServe("localhost:6060", nil)
	//}()
	//runtime.SetCPUProfileRate(10000)
	//f, _ := os.Create("cpu.pprof")
	//pprof.StartCPUProfile(f)

	// Your worker implementation here.
	for !worker.ShutDown() {
		if worker.Task != nil && worker.Task.TaskID != -1 {
			log.Println("worker" + strconv.Itoa(int(worker.Index)) + "分配任务" + strconv.Itoa(int(worker.Task.TaskID)))
			err := worker.Working(worker.Task)
			if err == nil {
				worker.Task = RunningForNext(worker.Index)
			} else {
				//fmt.Printf(err.Error())
				log.Println("err not null")
				errArgs := RPCTaskArgs{
					Type:   worker.Task.Type,
					Index:  worker.Task.TaskID,
					Offset: worker.Index,
				}
				errReply := RPCTaskReply{}
				worker.DeleteFile(reply.Task)
				call("Coordinator.WorkerCreateFileError", &errArgs, &errReply)
				log.Println("call worker create file error")
				time.Sleep(1 * time.Second)
				worker.Task = nil
			}
		} else {
			//			println("sleep")
			log.Println(strconv.Itoa(int(worker.Index)) + "not allocated task, sleep")
			time.Sleep(2 * time.Second)
			args := RPCArgs{
				Index: worker.Index,
			}
			reply := RPCReply{}

			call("Coordinator.CheckHasAllocatedTask", &args, &reply)
			worker.Task = reply.Task
		}
	}
	//pprof.StopCPUProfile()
	time.Sleep(20 * time.Second)
	//	fmt.Println("结束" + strconv.Itoa(int(worker.Index)))
	// uncomment to send the Example RPC to the coordinator.

}

func (w *WorkerData) DeleteFile(task *Task) {
	println("delete")
	if task.Type == MapTaskType {
		for i := 0; i < w.ReduceNum; i++ {
			if _, err := os.Stat("mr-" + strconv.Itoa(int(task.TaskID)) + "-" + strconv.Itoa(i)); err != nil {
				if os.IsNotExist(err) {
					return
				}
			} else {
				os.Remove("mr-" + strconv.Itoa(int(task.TaskID)) + "-" + strconv.Itoa(i))
			}
		}
	} else {
		if _, err := os.Stat(task.OutPutLocation); err != nil {
			if os.IsNotExist(err) {

			}
		} else {
			os.Remove(task.OutPutLocation)
		}

	}

}

func (w *WorkerData) Working(task *Task) error {
	taskType := task.Type
	taskInput := task.TaskLocation

	if taskType == MapTaskType {
		if task.Status == TaskFinished || task.Status == TaskError {
			//			fmt.Printf(task.OutPutLocation + "任务已完成，退出")
			finishArgs := RPCTaskArgs{
				Type:  MapTaskType,
				Index: task.TaskID,
			}
			call("Coordinator.UpdateFinished", &finishArgs, &RPCReply{})
			return nil
		}
		file, err := os.Open(taskInput[0])
		if err != nil {
			println("1")
			return err
		}
		defer file.Close()
		var files []*os.File
		for k := 0; k < w.ReduceNum; k++ {
			writeFile, err := os.OpenFile("mr-"+strconv.Itoa(int(task.TaskID))+"-"+strconv.Itoa(k), os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)
			defer writeFile.Close()
			files = append(files, writeFile)
			if err != nil {

				return err
			}
		}

		content, err := ioutil.ReadAll(file)
		if err != nil {
			return err
		}

		kva := w.Map(taskInput[0], string(content))
		size := w.ReduceNum
		keys := make([][]KeyValue, size)
		for i := 0; i < len(kva); i++ {
			hashKey := ihash(kva[i].Key) % w.ReduceNum
			keys[hashKey] = append(keys[hashKey], kva[i])
		}
		for k := 0; k < len(keys); k++ {
			subKva := keys[k]
			data, err := json.Marshal(subKva)
			if err != nil {
				return err
			}
			writer := bufio.NewWriter(files[k])
			//for i := 0; i < len(string(data)); i++ {
			//	writer.WriteByte(data[i])
			//}
			_, err = writer.Write(data)
			writer.Flush()

		}

		args := RPCTaskArgs{
			Type:   ReduceTaskType,
			Index:  task.TaskID,
			Offset: w.Index,
		}
		reply := RPCReply{}

		_, err = call("Coordinator.CheckTaskStatusFinished", &args, &reply)
		log.Println("完成map 并且提交任务" + strconv.Itoa(int(w.Task.TaskID)))
		if err != nil {
			return err
		}

	} else {
		if task.Status == TaskFinished || task.Status == TaskError {
			//			fmt.Printf(task.OutPutLocation + "任务早已完成")
			finishArgs := RPCTaskArgs{
				Type:  ReduceTaskType,
				Index: task.TaskID,
			}
			call("Coordinator.UpdateFinished", &finishArgs, &RPCReply{})
			return nil
		}
		files := make([]*os.File, 0)
		//		println("taskinput" + strconv.Itoa(len(taskInput)))
		//		println(taskInput[0])
		log.Println("---------------   reduce task" + strconv.Itoa(int(w.Task.TaskID)) + "-------------")
		for k := 0; k < len(taskInput); k++ {
			log.Println(taskInput[k])
		}
		log.Println("---------------   reduce task" + strconv.Itoa(int(w.Task.TaskID)) + "-------------")
		for i := 0; i < len(taskInput); i++ {
			file, err := os.Open(taskInput[i])
			defer file.Close()
			if err != nil {
				log.Println("reduce taskID" + strconv.Itoa(int(w.Task.TaskID)) + "输入文件" + taskInput[i] + "出错")
				return err
			}
			log.Println("reduce taskID" + strconv.Itoa(int(w.Task.TaskID)) + "输入文件" + taskInput[i])
			files = append(files, file)
		}
		writeFile, err := os.OpenFile(task.OutPutLocation, os.O_RDWR|os.O_CREATE|os.O_TRUNC, 0777)

		//		println("________________________________________writeFIle")
		if err != nil {
			return err
		}
		defer writeFile.Close()
		var list []KeyValue
		//		println(len(files))
		for i := 0; i < len(files); i++ {
			content, err := ioutil.ReadAll(files[i])
			if err != nil {
				return err
			}
			var intermediate []KeyValue
			err = json.Unmarshal(content, &intermediate)
			if err != nil {
				return err
			}
			list = append(list, intermediate...)

		}
		sort.Sort(ByKey(list))
		i := 0
		for i < len(list) {
			j := i + 1
			for j < len(list) && list[j].Key == list[i].Key {
				j++
			}
			values := []string{}
			for k := i; k < j; k++ {
				values = append(values, list[k].Value)
			}
			output := w.Reduce(list[i].Key, values)

			// this is the correct format for each line of Reduce output.
			fmt.Fprintf(writeFile, "%v %v\n", list[i].Key, output)

			i = j
		}

		args := RPCTaskArgs{
			Index:  task.TaskID,
			Offset: w.Index,
		}
		reply := RPCReply{}
		_, err = call("Coordinator.CheckTaskStatusFinished", &args, &reply)
		log.Println("完成reduce 并且提交任务" + strconv.Itoa(int(w.Task.TaskID)))
		if err != nil {
			return err
		}
		return nil
	}
	return nil
}

// example function to show how to make an RPC call to the coordinator.
//
// the RPC argument and reply types are defined in rpc.go.
func Register() *RPCReply {

	// declare an argument structure.
	args := RPCArgs{}

	// fill in the argument(s).

	// declare a reply structure.
	reply := RPCReply{}

	// send the RPC request, wait for the reply.
	call("Coordinator.Register", &args, &reply)
	if reply.Index == -1 {
		return nil
	}
	return &reply
}

func RunningForNext(index int32) *Task {
	args := RPCArgs{
		Index: index,
	}
	reply := RPCReply{}
	//	println("running for next")
	call("Coordinator.RunningForNext", &args, &reply)
	//	println("has resign a task")
	return reply.Task
}

func (w *WorkerData) ShutDown() bool {
	// master 如果没联系上，自我关闭
	args := RPCArgs{
		Index: w.Index,
	}
	reply := RPCReply{}
	result, err := call("Coordinator.CheckStatus", &args, &reply)
	if err != nil || !result {
		return true
	}
	return false
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) (bool, error) {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	sockname := coordinatorSock()
	c, err := rpc.DialHTTP("unix", sockname)
	if err != nil {
		log.Fatal("dialing:", err)
		return false, err
	}
	defer c.Close()

	err = c.Call(rpcname, args, reply)
	if err == nil {
		return true, nil
	}

	fmt.Println(err)
	return false, err
}
