package mr

import "encoding/json"
import "fmt"
import "hash/fnv"
import "io"
import "log"
import "net/rpc"
import "os"
import "sort"
import "time"

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

var coordSockName string // socket for coordinator

// 为了让 reduce 阶段能按 key 排序分组，需要一个 sort.Interface。
type ByKey []KeyValue

func (a ByKey) Len() int           { return len(a) }
func (a ByKey) Swap(i, j int)      { a[i], a[j] = a[j], a[i] }
func (a ByKey) Less(i, j int) bool { return a[i].Key < a[j].Key }

// main/mrworker.go calls this function.
func Worker(sockname string, mapf func(string, string) []KeyValue,
	reducef func(string, []string) string) {

	coordSockName = sockname

	// 主循环：领活 -> 干活 -> 汇报 -> 再领。
	// coordinator 不在了（call 失败）就退出；整个 job 做完（ExitTask）也退出。
	for {
		args := GetTaskArgs{}
		reply := GetTaskReply{}
		if !call("Coordinator.GetTask", &args, &reply) {
			return
		}

		switch reply.Type {
		case ExitTask:
			return
		case WaitTask:
			// 现在没活，但还有别的 worker 在跑。睡一小会儿再来，别忙等。
			time.Sleep(50 * time.Millisecond)
		case MapTask:
			doMap(reply, mapf)
			reportTask(MapTask, reply.TaskID)
		case ReduceTask:
			doReduce(reply, reducef)
			reportTask(ReduceTask, reply.TaskID)
		}
	}
}

// doMap 对一个输入文件跑 mapf，把结果按 ihash(key)%nReduce 分桶，
// 每个桶写成一个中间文件 mr-<taskID>-<bucket>。
//
// 关键：先写临时文件，写完再 rename。rename 在同一个文件系统内是原子的，
// 所以 reduce 永远不可能读到"写了一半"的中间文件。
func doMap(t GetTaskReply, mapf func(string, string) []KeyValue) {
	content, err := os.ReadFile(t.File)
	if err != nil {
		log.Fatalf("map %d: read %v: %v", t.TaskID, t.File, err)
	}

	kva := mapf(t.File, string(content))
	if len(kva) == 0 {
		// 即使没有输出，也要把空文件建出来：reduce 会按约定去读它们。
	}

	files := make([]*os.File, t.NReduce)
	names := make([]string, t.NReduce)
	encs := make([]*json.Encoder, t.NReduce)
	for i := 0; i < t.NReduce; i++ {
		f, err := os.CreateTemp(".", "mr-map-tmp-*")
		if err != nil {
			log.Fatalf("map %d: create temp: %v", t.TaskID, err)
		}
		files[i] = f
		names[i] = f.Name()
		encs[i] = json.NewEncoder(f)
	}

	for _, kv := range kva {
		y := ihash(kv.Key) % t.NReduce
		if err := encs[y].Encode(kv); err != nil {
			log.Fatalf("map %d: encode: %v", t.TaskID, err)
		}
	}

	for i := 0; i < t.NReduce; i++ {
		if err := files[i].Close(); err != nil {
			log.Fatalf("map %d: close: %v", t.TaskID, err)
		}
		final := fmt.Sprintf("mr-%d-%d", t.TaskID, i)
		if err := os.Rename(names[i], final); err != nil {
			log.Fatalf("map %d: rename: %v", t.TaskID, err)
		}
	}
}

// doReduce 读入所有 map 任务产生的第 ReduceIdx 个桶，排序、按 key 分组、
// 调 reducef，最后原子地写出 mr-out-<ReduceIdx>。
func doReduce(t GetTaskReply, reducef func(string, []string) string) {
	var kvs []KeyValue
	for x := 0; x < t.NMap; x++ {
		path := fmt.Sprintf("mr-%d-%d", x, t.TaskID)
		f, err := os.Open(path)
		if err != nil {
			// 正常情况下 reduce 开始前所有 map 都完成了，这里不该失败。
			log.Fatalf("reduce %d: open %v: %v", t.TaskID, path, err)
		}
		dec := json.NewDecoder(f)
		for {
			var kv KeyValue
			if err := dec.Decode(&kv); err == io.EOF {
				break
			} else if err != nil {
				f.Close()
				log.Fatalf("reduce %d: decode %v: %v", t.TaskID, path, err)
			}
			kvs = append(kvs, kv)
		}
		f.Close()
	}

	sort.Sort(ByKey(kvs))

	out, err := os.CreateTemp(".", "mr-out-tmp-*")
	if err != nil {
		log.Fatalf("reduce %d: create temp: %v", t.TaskID, err)
	}
	outName := out.Name()

	for i := 0; i < len(kvs); {
		j := i + 1
		for j < len(kvs) && kvs[j].Key == kvs[i].Key {
			j++
		}
		values := make([]string, 0, j-i)
		for k := i; k < j; k++ {
			values = append(values, kvs[k].Value)
		}
		// 输出格式必须和 mrsequential.go 一致："key value\n"
		fmt.Fprintf(out, "%v %v\n", kvs[i].Key, reducef(kvs[i].Key, values))
		i = j
	}

	if err := out.Close(); err != nil {
		log.Fatalf("reduce %d: close: %v", t.TaskID, err)
	}
	if err := os.Rename(outName, fmt.Sprintf("mr-out-%d", t.TaskID)); err != nil {
		log.Fatalf("reduce %d: rename: %v", t.TaskID, err)
	}
}

// reportTask 告诉 coordinator "这个任务我做完了"。
// 汇报失败不用处理：下次 GetTask 失败时 worker 自然会退出。
func reportTask(typ TaskType, id int) {
	args := ReportTaskArgs{Type: typ, TaskID: id}
	reply := ReportTaskReply{}
	call("Coordinator.ReportTask", &args, &reply)
}

// example function to show how to make an RPC call to the coordinator.
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
	// the "Coordinator.Example" tells the
	// receiving server that we'd like to call
	// the Example() method of struct Coordinator.
	ok := call("Coordinator.Example", &args, &reply)
	if ok {
		// reply.Y should be 100.
		fmt.Printf("reply.Y %v\n", reply.Y)
	} else {
		fmt.Printf("call failed!\n")
	}
}

// send an RPC request to the coordinator, wait for the response.
// usually returns true.
// returns false if something goes wrong.
func call(rpcname string, args interface{}, reply interface{}) bool {
	// c, err := rpc.DialHTTP("tcp", "127.0.0.1"+":1234")
	c, err := rpc.DialHTTP("unix", coordSockName)
	if err != nil {
		log.Fatal("dialing:", err)
	}
	defer c.Close()

	if err := c.Call(rpcname, args, reply); err == nil {
		return true
	}
	log.Printf("%d: call failed err %v", os.Getpid(), err)
	return false
}
