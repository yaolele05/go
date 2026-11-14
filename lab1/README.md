# 6.5840 Lab 1（MapReduce）参考实现

来源：`go-day5/src/mr/` 里 2026-11-12 写的那版实现。
课程树里的这三个文件已经恢复成原始骨架，完整实现保存在这个目录。

## 文件

| 文件 | 内容 |
|---|---|
| `rpc.go` | `TaskType`（MapTask/ReduceTask/WaitTask/ExitTask）、`GetTaskArgs/GetTaskReply`、`ReportTaskArgs` |
| `coordinator.go` | 任务状态机 + 超时重派（`taskTimeout = 10 * time.Second`）+ `Done()` |
| `worker.go` | 拉任务循环、`doMap`（按 `ihash(key)%NReduce` 分区写 `mr-X-Y`）、`doReduce`（读一列 `mr-*-Y` → 排序 → 输出 `mr-out-Y`） |

## 怎么跑

这三个文件属于课程 `mr` 包，不能单独运行，需要配套的 `mr/util.go`（测试脚手架）、
`mr/mr_test.go`、`main/mr{coordinator,worker,sequential}.go` 和 `mrapps/*.so`。

```bash
# 把三份文件拷回课程树（覆盖骨架）
cp coordinator.go rpc.go worker.go <6.5840>/src/mr/

cd <6.5840>/src
make mr                      # 构建二进制 + 插件，然后 go test -v -race 跑全部
make RUN="-run Wc" mr        # 只跑单个测试
```

## 测试结果（2026-11-12）

- 7/7 通过：Wc 12.72s / Indexer 6.60s / MapParallel 8.05s / ReduceParallel 9.04s /
  JobCount 13.04s / EarlyExit 7.04s / CrashWorker 45.18s，合计 102.709s
- `go test -race -count=3` 整套：`ok 263.689s`，0 次 DATA RACE
- `gofmt -l mr/` 无输出，`go vet ./mr/` 干净

## 设计要点

1. 任务状态 `Idle → InProgress → Finished`，`InProgress` 带 `start time`；
   `GetTask` 入口先惰性回收超时任务（不使用后台 goroutine），10s 无结果就重派。
2. `ReportTask` 幂等：已 `Finished` 的任务再报一次直接忽略——同一个任务被两个
   worker 完成时只计一次。
3. `Done()` 只在所有 reduce 任务 `Finished` 后才为 true（提前为 true 会让
   `TestEarlyExit` 读到半成品输出）。
4. map/reduce 都写临时文件再 `rename`，保证别的进程永远读不到半个文件。
5. 所有共享状态一把 `sync.Mutex`，持锁期间不做文件 IO。
