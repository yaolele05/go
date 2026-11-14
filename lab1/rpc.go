package mr

//
// RPC definitions.
//
// remember to capitalize all names.
//

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

// Add your RPC definitions here.

// TaskType 告诉 worker 这一次拿到的任务是什么。
type TaskType int

const (
	// 长轮询之后不再需要 WaitTask：没活时 GetTask 会挂起而不是立刻返回。
	MapTask    TaskType = iota // 去跑一个 map 任务
	ReduceTask                 // 去跑一个 reduce 任务
	ExitTask                   // 整个 job 做完了，可以退出
)

// GetTaskArgs 是空结构：worker 只说"我来领活"，不需要带参数。
type GetTaskArgs struct{}

// GetTaskReply 描述"领到的是什么活"。
//
// 注意：所有字段必须大写，因为 RPC 用 gob 编码，只有导出字段才会被传输。
type GetTaskReply struct {
	Type      TaskType // 任务类型
	TaskID    int      // 任务编号（map 是文件下标，reduce 是分区下标）
	File      string   // map 任务：读哪个输入文件
	ReduceIdx int      // reduce 任务：负责第几个分区
	NMap      int      // reduce 任务：一共有多少个 map 输出要读
	NReduce   int      // map 任务：中间结果要分成几个桶
}

// ReportTaskArgs 是 worker 干完活之后的汇报。
type ReportTaskArgs struct {
	Type   TaskType
	TaskID int
}

type ReportTaskReply struct{}
