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
func MakeLock(ck kvtest.IKVClerk, lockname string) *Lock {
	lk := &Lock{ck: ck, lockname: lockname, id: kvtest.RandValue(20)}

	return lk
}

// Acquire blocks until this client holds the lock.
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
func (lk *Lock) Release() {
	value, version, err := lk.ck.Get(lk.lockname)
	if err != rpc.OK || value != lk.id {
		return
	}

	lk.ck.Put(lk.lockname, "", version)
}
