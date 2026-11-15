#!/usr/bin/env bash
# 编译 MapReduce 的 coordinator / worker / sequential 和 mrapps 插件
# 说明：课程测试在 src/mr 里运行时，用 "../main" 找 coordinator、
#      用 "../../main" 和 "../../mrapps" 找 worker 与插件，
#      所以编译产物要同时放到 lab1/main 与 lab1/mrapps。
set -euo pipefail
cd "$(dirname "$0")/src"

go build -o main/mrcoordinator main/mrcoordinator.go
go build -o main/mrworker      main/mrworker.go
go build -o main/mrsequential  main/mrsequential.go

(cd mrapps && for p in *.go; do go build -buildmode=plugin "$p"; done)

mkdir -p ../main ../mrapps
cp main/mrcoordinator main/mrworker main/mrsequential ../main/
cp mrapps/*.so ../mrapps/
echo "build ok: main/ 与 mrapps/ 已就绪"
