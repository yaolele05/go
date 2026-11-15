package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	tester "6.5840/tester1"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

// entry 是 map 里存的一条记录：值和当前版本号。
type entry struct {
	Value   string
	Version rpc.Tversion
}

type KVServer struct {
	mu sync.Mutex
	m  map[string]entry
}

func MakeKVServer() *KVServer {
	kv := &KVServer{
		m: make(map[string]entry)}
	return kv
}

// Get returns the value and version for args.Key, if args.Key
// exists. Otherwise, Get returns ErrNoKey.
// Get 返回 args.Key 对应的值和版本号；如果键不存在，则返回 ErrNoKey。
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	e, ok := kv.m[args.Key]
	if !ok {
		reply.Err = rpc.ErrNoKey
		return

	}
	reply.Value = e.Value
	reply.Version = e.Version
	reply.Err = rpc.OK

}

// Update the value for a key if args.Version matches the version of
// the key on the server. If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the
// args.Version is 0, and returns ErrNoKey otherwise.
// 仅当 args.Version 与服务器上该键的版本号一致时，Put 才更新键值；
// 如果版本号不匹配，则返回 ErrVersion。键不存在时，只有 args.Version 为 0
// 才会创建该键；否则返回 ErrNoKey。
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	e, ok := kv.m[args.Key]
	if !ok {

		if args.Version != 0 {
			reply.Err = rpc.ErrNoKey
			return
		}

		kv.m[args.Key] = entry{Value: args.Value, Version: 1}
		reply.Err = rpc.OK
		return
	}

	if args.Version != e.Version {
		reply.Err = rpc.ErrVersion
		return
	}

	kv.m[args.Key] = entry{
		Value:   args.Value,
		Version: e.Version + 1,
	}

	reply.Err = rpc.OK

}

// You can ignore all arguments; they are for replicated KVservers
// 这些参数供复制型 KV 服务器使用；本实验可以忽略。
func StartKVServer(tc *tester.TesterClnt, ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []any {
	kv := MakeKVServer()
	return []any{kv}
}
