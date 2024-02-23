package mr

import (
	"errors"
	"log"
	"strconv"
	"sync"
	"time"
)
import "net"
import "os"
import "net/rpc"
import "net/http"

type Coordinator struct {
	// Your definitions here.
	RegisterMutex         sync.Mutex `json:"-"`
	TaskNotAllocatedMutex sync.Mutex
	TaskNotAllocated      map[int32]int32
	TotalWorkers          int32

	WorkersIdle         sync.Map // 闲置线程
	WorkersDisconnected sync.Map // 失联线程
	WorkersRunning      sync.Map // 工作中的线程
	Worker              sync.Map // 记录每个在线工作线程的联系时间
	WorkerToTask        sync.Map
	TaskPool            []*Task
	TaskMutex           sync.Mutex
	NMapNum             int
	NReduceNum          int
}

type Task struct {
	TaskID          int32
	TaskLocation    []string
	AllocatedWorker int32
	Status          int32 // 0 wait for allocated, 1 unfinished, 2 finished
	OutPutLocation  string
	Type            int32 // 0 map  1 reduce
}

const (
	TaskUnAllocated    = 1
	TaskUnFinished     = 2
	TaskFinished       = 3
	TaskNotCreate      = 0
	TaskError          = 3
	MapTaskType        = 0
	ReduceTaskType     = 1
	WorkerDisconnected = 1
	WorkerIdle         = 0
)

// Your code here -- RPC handlers for the worker to call.

// an example RPC handler.
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1

	return nil
}

func (c *Coordinator) UpdateConnectTime(index int32) {
	c.Worker.Store(index, time.Now().Unix())
}

func (c *Coordinator) ReportStatus(args *RPCArgs, reply *RPCReply) error {
	c.CheckDisconnected(args.Index)
	c.UpdateConnectTime(args.Index)
	return nil
}

func (c *Coordinator) CheckStatus(args *RPCArgs, reply *RPCReply) error {
	c.CheckDisconnected(args.Index)
	c.UpdateConnectTime(args.Index)
	if args == nil || args.Index == -1 {
		return nil
	}
	c.CheckDisconnected(args.Index)
	reply.Online = true

	return nil
}

func (c *Coordinator) CheckHasAllocatedTask(args *RPCArgs, reply *RPCReply) error {
	index := args.Index
	value, ok := c.WorkerToTask.Load(index)
	if ok {
		reply.Task = value.(*Task)
	} else {
		task := int32(-1)
		task = c.AllocateTask()
		if task != -1 {
			reply.Task = c.StartUpTask(index, task)
			return nil
		}
	}

	return nil
}

func (c *Coordinator) CreateTask(taskType int32, taskLocation string) *Task {

	taskID := len(c.TaskPool)
	task := &Task{
		TaskID:       int32(taskID),
		TaskLocation: []string{},
		Type:         taskType,
		Status:       TaskUnAllocated,
	}
	if taskLocation != "" {
		task.TaskLocation = append(task.TaskLocation, taskLocation)
	}
	if taskType == ReduceTaskType {
		task.OutPutLocation = "mr-out-" + strconv.Itoa(taskID-c.NMapNum)
		task.Status = TaskNotCreate
	}
	c.TaskPool = append(c.TaskPool, task)
	if taskType == MapTaskType {
		log.Println("in create add" + strconv.Itoa(taskID))
		c.TaskNotAllocatedMutex.Lock()
		c.TaskNotAllocated[int32(taskID)] = 1
		c.TaskNotAllocatedMutex.Unlock()
	}

	//		fmt.Println("ID :" + strconv.Itoa(taskID))

	return task
}

// 检查任务有无完成
func (c *Coordinator) CheckTaskStatusFinished(args *RPCTaskArgs, reply *RPCReply) error {
	if args.Type == ReduceTaskType {
		c.RPCCreateTask(args, reply)
	}
	c.WorkerToTask.Delete(args.Offset)
	c.TaskMutex.Lock()
	c.TaskPool[args.Index].Status = TaskFinished
	c.TaskMutex.Unlock()
	return nil
}

// 更新reduce任务地址
func (c *Coordinator) RPCCreateTask(args *RPCTaskArgs, reply *RPCReply) error {

	//	taskLocation := args.TaskLocation
	//	println("has into create Task")
	//	fmt.Println("请求创建任务 task location" + taskLocation)

	c.TaskMutex.Lock()
	defer c.TaskMutex.Unlock()
	//		println("has into reduce task")
	for i := c.NMapNum; i < len(c.TaskPool); i++ {
		task := c.TaskPool[i]
		task.TaskLocation = append(task.TaskLocation, "mr-"+strconv.Itoa(int(args.Index))+"-"+strconv.Itoa(i-c.NMapNum))
		log.Println("mr-" + strconv.Itoa(int(args.Index)) + "-" + strconv.Itoa(i-c.NMapNum))
		log.Println("----------------------------------------")
		for j := 0; j < len(task.TaskLocation); j++ {
			log.Println(task.TaskLocation[j])
		}
		log.Println("----------------------------------------")

		if len(task.TaskLocation) == c.NMapNum {
			task.Status = TaskUnAllocated
			log.Println("add reduce task")
			c.TaskNotAllocatedMutex.Lock()
			c.TaskNotAllocated[task.TaskID] = 1
			c.TaskNotAllocatedMutex.Unlock()
		}

	}
	return nil
}

func (c *Coordinator) WorkerCreateFileError(args *RPCTaskArgs, reply *RPCTaskReply) error {
	taskID := args.Index
	workerID := args.Offset

	//println("in  workerCreateFileError")
	c.CheckDisconnected(args.Index)

	c.UpdateConnectTime(args.Index)

	c.WorkerToTask.Delete(workerID)

	c.TaskMutex.Lock()
	log.Println("___________________________")
	log.Println("taskId" + strconv.Itoa(int(taskID)) + "location")
	for i := 0; i < len(c.TaskPool[taskID].TaskLocation); i++ {
		log.Println(c.TaskPool[taskID].TaskLocation[i])
	}
	log.Println("-----------------------------")
	c.TaskPool[taskID].Status = TaskUnAllocated
	c.TaskMutex.Unlock()

	c.TaskNotAllocatedMutex.Lock()
	c.TaskNotAllocated[taskID] = 1
	c.TaskNotAllocatedMutex.Unlock()
	c.WorkersRunning.Delete(workerID)
	c.AddToIdlePool(workerID)
	//println("out of workerCreateFileError")
	return nil
}

// 工作线程运行完后请求不进入idle线程池直接分配任务
func (c *Coordinator) RunningForNext(args *RPCArgs, reply *RPCReply) error {
	if args == nil || args.Index == -1 {
		return nil
	}
	c.CheckDisconnected(args.Index)
	c.UpdateConnectTime(args.Index)
	///	println("in running for next")
	taskId := int32(-1)
	taskId = c.AllocateTask()
	if taskId != -1 {
		//		fmt.Println("继续做任务 taskID" + strconv.Itoa(int(taskId)))
		reply.Task = c.ContinueWork(c.TaskPool, args.Index, taskId, &c.TaskMutex)
		c.WorkersRunning.Store(args.Index, 1)
		c.WorkerToTask.Store(args.Index, c.TaskPool[taskId])
		return nil
	}
	reply.Task = &Task{
		TaskID: -1,
	}
	c.AddToIdlePool(args.Index)
	c.WorkersRunning.Delete(args.Index)

	return nil
}

func (c *Coordinator) Register(args *RPCArgs, reply *RPCReply) error {
	// 检查参数，包括确定信道存在
	if args == nil {
		return errors.New("args is nil")
	}

	//	fmt.Println("have a request")
	// 注册worker，分配号码
	index := c.AllocateIndex()
	c.Worker.Store(index, time.Now().Unix())
	//	fmt.Println("分配号码 index ：" + strconv.Itoa(int(index)))
	// 分配任务
	taskID := c.AllocateTask()
	pool := c.TaskPool
	reply.ReduceNum = c.NReduceNum
	log.Println("register ")
	// 如果taskID是负数，表示暂时无闲置任务，将线程加入闲置线程组
	if taskID < 0 {
		//		fmt.Println("注册时无任务 index ：" + strconv.Itoa(int(index)))
		c.AddToIdlePool(index)
		return nil
	}
	//	fmt.Printf("allocate index %d\n", index)

	reply.Task = pool[taskID]
	reply.Task.AllocatedWorker = index
	//	fmt.Println("分配给task 的workerID" + strconv.Itoa(int(reply.Task.AllocatedWorker)))
	reply.Index = index
	reply.Socket = args.Socket
	c.WorkerToTask.Store(index, pool[taskID])
	c.WorkersRunning.Store(index, 1)
	return nil
}

func (c *Coordinator) AddWorkerToDisconnected(index int32) {

	c.WorkersDisconnected.Store(index, 1)
}

func (c *Coordinator) AddTaskToUnAllocated(task *Task) {
	c.TaskMutex.Lock()
	task.Status = TaskUnAllocated
	c.TaskMutex.Unlock()
	c.TaskNotAllocatedMutex.Lock()
	c.TaskNotAllocated[task.TaskID] = 1
	c.TaskNotAllocatedMutex.Unlock()

	println("out addTask To un Allocated")
}

// 检查工作节点是否为断连设备，是的话重新加入闲置节点
func (c *Coordinator) CheckDisconnected(index int32) {
	if _, ok := c.WorkersDisconnected.Load(index); ok {
		c.WorkersDisconnected.Delete(index)
		c.AddToIdlePool(index)
	}
}

func (c *Coordinator) UpdateFinished(args *RPCTaskArgs, reply *RPCReply) error {

	taskId := args.Index

	c.TaskNotAllocatedMutex.Lock()
	defer c.TaskNotAllocatedMutex.Unlock()
	delete(c.TaskNotAllocated, taskId)

	return nil
}

func (c *Coordinator) ContinueWork(pool []*Task, index int32, taskID int32, mutex *sync.Mutex) *Task {
	mutex.Lock()
	defer mutex.Unlock()
	pool[taskID].Status = 1
	pool[taskID].AllocatedWorker = index
	pool[taskID].Status = TaskUnFinished
	return pool[taskID]
}

func (c *Coordinator) StartUpTask(index int32, task int32) *Task {
	c.WorkersIdle.Delete(index)

	var result *Task

	c.TaskMutex.Lock()
	c.TaskPool[task].AllocatedWorker = index
	c.TaskPool[task].Status = TaskUnFinished
	result = c.TaskPool[task]
	c.TaskMutex.Unlock()

	c.WorkerToTask.Store(index, result)
	return result
}

func (c *Coordinator) AddToIdlePool(index int32) {

	c.WorkersIdle.Store(index, 1)

}

func (c *Coordinator) AllocateIndex() int32 {

	c.RegisterMutex.Lock()

	c.TotalWorkers = c.TotalWorkers + 1
	result := c.TotalWorkers
	c.RegisterMutex.Unlock()

	c.WorkersIdle.Store(result, 1)

	return result
}

func (c *Coordinator) AllocateTask() int32 {

	c.TaskNotAllocatedMutex.Lock()
	defer c.TaskNotAllocatedMutex.Unlock()
	if len(c.TaskNotAllocated) != 0 {
		taskId := int32(-1)
		for key, _ := range c.TaskNotAllocated {
			taskId = key
			break
		}
		delete(c.TaskNotAllocated, taskId)
		return taskId
	}
	return -1

}

// start a thread that listens for RPCs from worker.go
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

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	ret := false

	// Your code here.

	lens := 0
	c.WorkerToTask.Range(
		func(key, value interface{}) bool {
			lens++
			if lens > 0 {
				return false
			}
			return true
		})
	if lens != 0 {
		return false
	}
	lens = 0
	c.WorkersRunning.Range(
		func(key, value interface{}) bool {
			lens++
			//if lens > 0 {
			//	return false
			//}
			log.Println(key)
			return true
		})
	//println("here")
	log.Println("lens" + strconv.Itoa(lens))
	if lens == 0 {
		if c.CheckAllTaskFinished() {
			return true
		} else {
			return false
		}
	}
	return ret
}

func (c *Coordinator) CheckAllTaskFinished() bool {

	c.TaskMutex.Lock()
	for i := 0; i < len(c.TaskPool); i++ {
		if c.TaskPool[i].Status != TaskFinished {
			c.TaskMutex.Unlock()
			return false
		}
	}
	c.TaskMutex.Unlock()
	log.Println("exit")
	return true
}

func (c *Coordinator) CheckHeartBeat() {
	for {
		needDelete := make([]int32, 0)
		c.WorkersRunning.Range(
			func(key, _ interface{}) bool {
				nowTime := time.Now().Unix()
				lastTime, _ := c.Worker.Load(key)

				if nowTime-lastTime.(int64) >= 6 {

					c.AddWorkerToDisconnected(key.(int32))
					log.Println(strconv.Itoa(int(key.(int32))) + "has disconnected ")
					needDelete = append(needDelete, key.(int32))
					if value, ok := c.WorkerToTask.Load(key); ok {
						c.AddTaskToUnAllocated(value.(*Task))
						log.Println(strconv.Itoa(int(key.(int32))) + "has disconnected task is" + strconv.Itoa(int(value.(*Task).TaskID)))
						c.WorkersDisconnected.Store(key, 1)
						c.DeleteFile(value.(*Task))
						c.WorkerToTask.Delete(key)
					}

				}
				return true
			})
		for j := 0; j < len(needDelete); j++ {
			c.WorkersRunning.Delete(needDelete[j])
		}
		needDelete = []int32{}
		c.WorkersIdle.Range(
			func(key, _ interface{}) bool {
				nowTime := time.Now().Unix()
				lastTime, _ := c.Worker.Load(key)
				if nowTime-lastTime.(int64) >= 6 {
					needDelete = append(needDelete, key.(int32))
					log.Println(strconv.Itoa(int(key.(int32))) + "has disconnected ")
					c.AddWorkerToDisconnected(key.(int32))
					c.WorkersDisconnected.Store(key, 1)
				}
				return true
			})
		for j := 0; j < len(needDelete); j++ {
			c.WorkersIdle.Delete(needDelete[j])
		}
		time.Sleep(4 * time.Second)
	}
}

func (c *Coordinator) DeleteFile(task *Task) {
	c.TaskMutex.Lock()
	index := task.TaskID
	taskType := task.Type
	output := task.OutPutLocation
	c.TaskMutex.Unlock()
	if taskType == MapTaskType {
		for i := 0; i < c.NReduceNum; i++ {
			if _, err := os.Stat("mr-" + strconv.Itoa(int(index)) + "-" + strconv.Itoa(i)); err != nil {
				if os.IsNotExist(err) {
					return
				}
			} else {
				os.Remove("mr-" + strconv.Itoa(int(index)) + "-" + strconv.Itoa(i))
			}
		}
	} else {
		os.Remove(output)
	}
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(files []string, nReduce int) *Coordinator {
	c := Coordinator{

		TaskNotAllocated: make(map[int32]int32),
		TaskPool:         make([]*Task, 0),
		TotalWorkers:     0,
		NReduceNum:       nReduce,
	}
	f, _ := os.OpenFile("CoordinatorLog.log", os.O_CREATE|os.O_APPEND|os.O_RDWR, os.ModePerm)

	log.SetOutput(f)
	lens := 0
	for i := 0; i < len(files); i++ {
		_, err := os.Stat(files[i])
		if err != nil && os.IsNotExist(err) {
			continue
		}
		lens++
		c.CreateTask(MapTaskType, files[i])
	}
	c.NMapNum = lens
	for i := 0; i < nReduce; i++ {
		c.CreateTask(ReduceTaskType, "")
	}
	for key, _ := range c.TaskNotAllocated {
		log.Println(strconv.Itoa(int(key)) + "has add to unallcated")
	}
	//	fmt.Println("Start______
	//	_____________________")
	// Your code here.
	go c.CheckHeartBeat()
	c.server()

	log.Println("map num" + strconv.Itoa(len(files)))
	log.Println("reduce num" + strconv.Itoa(nReduce))
	return &c
}
