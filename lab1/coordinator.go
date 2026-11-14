package mr

import "log"
import "net"
import "os"
import "net/rpc"
import "net/http"
import "sync"
import "time"

// 一个任务在 coordinator 眼里的三种状态。
type TaskState int

const (
	Idle       TaskState = iota // 没派出去
	InProgress                  // 派出去了，等 worker 汇报
	Finished                    // 做完了
)

// 每个任务的状态 + 什么时候派出去的（用来判超时）。
type TaskInfo struct {
	state TaskState
	start time.Time
}

// 整个 job 的阶段。
type Phase int

const (
	phaseMap    Phase = iota // 正在发 map 任务
	phaseReduce              // map 全做完，正在发 reduce 任务
	phaseDone                // 全做完
)

// 一个任务超过这么久还没汇报，就认为干活的 worker 挂了，重新派发。
// 10s 是 handout 的建议值；不能更小，因为 mrapps/jobcount.go 的 map 正常就要睡 2-5s。
const taskTimeout = 10 * time.Second

type Coordinator struct {
	mu sync.Mutex // 保护下面所有字段：RPC handler 是并发调用的

	files   []string
	nMap    int
	nReduce int

	mapTasks    []TaskInfo
	reduceTasks []TaskInfo
	phase       Phase
	done        bool
}

// pickIdleLocked 找到第一个空闲任务，标记成 InProgress 并返回下标；没有就返回 -1。
// 调用者必须持有 c.mu。
func pickIdleLocked(tasks []TaskInfo) int {
	now := time.Now()
	for i := range tasks {
		if tasks[i].state == Idle {
			tasks[i].state = InProgress
			tasks[i].start = now
			return i
		}
	}
	return -1
}

// expireStaleLocked 把超时未汇报的任务打回 Idle，这样它会被重新派发。
// 调用者必须持有 c.mu。
func expireStaleLocked(tasks []TaskInfo) {
	now := time.Now()
	for i := range tasks {
		if tasks[i].state == InProgress && now.Sub(tasks[i].start) > taskTimeout {
			tasks[i].state = Idle
		}
	}
}

// anyInProgressLocked 还有没有正在跑的任务。
func anyInProgressLocked(tasks []TaskInfo) bool {
	for i := range tasks {
		if tasks[i].state == InProgress {
			return true
		}
	}
	return false
}

// allFinishedLocked 是不是所有任务都做完了。
func allFinishedLocked(tasks []TaskInfo) bool {
	for i := range tasks {
		if tasks[i].state != Finished {
			return false
		}
	}
	return true
}

// GetTask 是 worker 领活的入口：有活就派，没活让它等，全干完让它退出。
func (c *Coordinator) GetTask(args *GetTaskArgs, reply *GetTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	// 先把超时的任务回收，这样下面的 pickIdle 就能重新派出去。
	expireStaleLocked(c.mapTasks)
	expireStaleLocked(c.reduceTasks)

	if c.phase == phaseMap {
		if i := pickIdleLocked(c.mapTasks); i >= 0 {
			reply.Type = MapTask
			reply.TaskID = i
			reply.File = c.files[i]
			reply.NReduce = c.nReduce
			return nil
		}
		if anyInProgressLocked(c.mapTasks) {
			reply.Type = WaitTask
			return nil
		}
		// 没有空闲、也没有在跑的 map 任务 => map 阶段结束，进入 reduce。
		c.phase = phaseReduce
	}

	if c.phase == phaseReduce {
		if i := pickIdleLocked(c.reduceTasks); i >= 0 {
			reply.Type = ReduceTask
			reply.TaskID = i
			reply.ReduceIdx = i
			reply.NMap = c.nMap
			return nil
		}
		if anyInProgressLocked(c.reduceTasks) {
			reply.Type = WaitTask
			return nil
		}
		c.phase = phaseDone
		c.done = true
		reply.Type = ExitTask
		return nil
	}

	reply.Type = ExitTask
	return nil
}

// ReportTask 是 worker 干完活的汇报。必须幂等：同一个任务可能被汇报两次
// （超时重派后，前一个 worker 又活过来了），重复汇报不能把状态搞坏。
func (c *Coordinator) ReportTask(args *ReportTaskArgs, reply *ReportTaskReply) error {
	c.mu.Lock()
	defer c.mu.Unlock()

	switch args.Type {
	case MapTask:
		if args.TaskID < 0 || args.TaskID >= len(c.mapTasks) {
			return nil
		}
		if c.mapTasks[args.TaskID].state != Finished {
			c.mapTasks[args.TaskID].state = Finished
		}
		if c.phase == phaseMap && allFinishedLocked(c.mapTasks) {
			c.phase = phaseReduce
		}
	case ReduceTask:
		if args.TaskID < 0 || args.TaskID >= len(c.reduceTasks) {
			return nil
		}
		if c.reduceTasks[args.TaskID].state != Finished {
			c.reduceTasks[args.TaskID].state = Finished
		}
		if c.phase == phaseReduce && allFinishedLocked(c.reduceTasks) {
			c.phase = phaseDone
			c.done = true
		}
	}
	return nil
}

// an example RPC handler.
//
// the RPC argument and reply types are defined in rpc.go.
func (c *Coordinator) Example(args *ExampleArgs, reply *ExampleReply) error {
	reply.Y = args.X + 1
	return nil
}

// start a thread that listens for RPCs from worker.go
func (c *Coordinator) server(sockname string) {
	rpc.Register(c)
	rpc.HandleHTTP()
	os.Remove(sockname)
	l, e := net.Listen("unix", sockname)
	if e != nil {
		log.Fatalf("listen error %s: %v", sockname, e)
	}
	go http.Serve(l, nil)
}

// main/mrcoordinator.go calls Done() periodically to find out
// if the entire job has finished.
func (c *Coordinator) Done() bool {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.done
}

// create a Coordinator.
// main/mrcoordinator.go calls this function.
// nReduce is the number of reduce tasks to use.
func MakeCoordinator(sockname string, files []string, nReduce int) *Coordinator {
	c := Coordinator{
		files:   files,
		nMap:    len(files),
		nReduce: nReduce,
		phase:   phaseMap,
	}
	c.mapTasks = make([]TaskInfo, len(files))
	c.reduceTasks = make([]TaskInfo, nReduce)

	c.server(sockname)
	return &c
}
