package lock

import (
	"time"

	kvtest "6.5840/kvtest1"
	"6.5840/kvsrv1/rpc"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get. The tester passes the clerk in when calling
	// MakeLock().
	// IKVClerk 是 KV 客户端的 Go 接口：它隐藏 ck 的具体 Clerk 类型，
	// 但保证 ck 支持 Put 和 Get。测试框架调用 MakeLock() 时会传入该客户端。
	ck       kvtest.IKVClerk
	lockname string

	id string
}

// The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// This interface supports multiple locks by means of the
// lockname argument; locks with different names should be
// independent.
// 测试框架调用 MakeLock() 并传入一个 KV 客户端；代码可以通过 lk.ck.Put() 或 lk.ck.Get() 发起操作。
// lockname 参数用于区分多个锁；名称不同的锁应互不影响。
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname, id: kvtest.RandValue(20)}

	return lk
}

// Acquire blocks until this client holds the lock.
// Acquire 会阻塞，直到当前客户端成功持有该锁。
func (lk *Lock) Acquire() {
	for {
		value, version, err := lk.ck.Get(lk.lockname)

		if err == rpc.ErrNoKey {
			version = 0
			value = ""
		} else if err != rpc.OK {
			time.Sleep(10 * time.Millisecond)
			continue
		}

		// A previous Put may have succeeded even if its reply was lost.
		// 之前的 Put 可能已经成功，只是响应丢失了。
		if value == lk.id {
			return
		}

		if value == "" {
			err = lk.ck.Put(lk.lockname, lk.id, version)
			if err == rpc.OK {
				return
			}
		}
		time.Sleep(10 * time.Millisecond)

	}
}

// Release releases the lock. Calling it without holding the lock
// must not break another client's lock.
// Release 释放该锁。即使调用者当前没有持有锁，也不能破坏其他客户端持有的锁。
func (lk *Lock) Release() {
	value, version, err := lk.ck.Get(lk.lockname)
	if err != rpc.OK || value != lk.id {
		return
	}

	lk.ck.Put(lk.lockname, "", version)
}
