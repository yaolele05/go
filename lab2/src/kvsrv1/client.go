package kvsrv

import (
	"6.5840/kvsrv1/rpc"
	kvtest "6.5840/kvtest1"
	tester "6.5840/tester1"
)

type Clerk struct {
	clnt   *tester.Clnt
	server string
	// 这个 lab 不需要额外字段
}

func MakeClerk(clnt *tester.Clnt, server string) kvtest.IKVClerk {
	ck := &Clerk{clnt: clnt, server: server}
	return ck
}

// Get fetches the current value and version for a key. It returns
// ErrNoKey if the key does not exist. It keeps trying forever in the
// face of all other errors.
// 中文：Get 获取指定键当前的值和版本号。键不存在时返回 ErrNoKey；遇到其他错误时会持续重试。
func (ck *Clerk) Get(key string) (string, rpc.Tversion, rpc.Err) {

	args := rpc.GetArgs{Key: key}
	for {
		var reply rpc.GetReply
		ok := ck.clnt.Call(ck.server, "KVServer.Get", &args, &reply)
		if ok && (reply.Err == rpc.OK || reply.Err == rpc.ErrNoKey) {

			return reply.Value, reply.Version, reply.Err
		}
	}

}

// Put updates key with value only if the version in the request
// matches the version of the key at the server. If the versions don't
// match, the server returns ErrVersion. If Put receives an ErrVersion
// on its first RPC, Put returns ErrVersion. If the server returns
// ErrVersion on a resend RPC, Put returns ErrMaybe: the earlier RPC
// might have been processed successfully but the response was lost.
// 中文：只有请求版本号与服务器上该键的版本号一致时，Put 才会更新键值；否则返回 ErrVersion。
// 第一次 RPC 收到 ErrVersion 时，Put 返回 ErrVersion；重试 RPC 收到 ErrVersion 时，
// Put 返回 ErrMaybe，因为第一次请求可能已执行成功，只是响应丢失了。
func (ck *Clerk) Put(key, value string, version rpc.Tversion) rpc.Err {
	args := rpc.PutArgs{Key: key, Value: value, Version: version}

	var reply rpc.PutReply
	ok := ck.clnt.Call(ck.server, "KVServer.Put", &args, &reply)
	if ok {
		if reply.Err == rpc.ErrVersion {
			return rpc.ErrVersion
		}
		return reply.Err
	}
	for {
		var retryReply rpc.PutReply
		if ck.clnt.Call(ck.server, "KVServer.Put", &args, &retryReply) {
			if retryReply.Err == rpc.ErrVersion {
				return rpc.ErrMaybe
			}
			return retryReply.Err
		}
	}
}
