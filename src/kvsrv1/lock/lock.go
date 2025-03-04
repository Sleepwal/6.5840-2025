package lock

import (
	"6.5840/kvsrv1/rpc"
	"6.5840/kvtest1"
	"time"
)

type Lock struct {
	// IKVClerk is a go interface for k/v clerks: the interface hides
	// the specific Clerk type of ck but promises that ck supports
	// Put and Get.  The tester passes the clerk in when calling
	// MakeLock().
	ck kvtest.IKVClerk
	// You may add code here

	// The lock service should use a specific key to store the "lock state"
	//The key to be used is passed through the parameter l of MakeLock in src/kvsrv1/lock/lock.go.
	clientId string
	lockKey  string
}

// MakeLock The tester calls MakeLock() and passes in a k/v clerk; your code can
// perform a Put or Get by calling lk.ck.Put() or lk.ck.Get().
//
// Use l as the key to store the "lock state" (you would have to decide
// precisely what the lock state is).
func MakeLock(ck kvtest.IKVClerk, l string) *Lock {
	// You may add code here
	lk := &Lock{
		ck:       ck,
		lockKey:  l,                   //  The key to be used is passed through the parameter l of MakeLock
		clientId: kvtest.RandValue(8), // You will need a unique identifier for each lock client; call kvtest.RandValue(8) to generate a random string.
	}

	return lk
}

// Acquire only one client can successfully acquire the lock at a time;
func (lk *Lock) Acquire() {
	// Your code here
	i := 10
	for {
		val, version, err := lk.ck.Get(lk.lockKey)
		if val == lk.clientId { // 锁的持有者再次获取锁
			return
		}

		if err == rpc.ErrNoKey || (err == rpc.OK && val == "") { // 锁未被占用
			err = lk.ck.Put(lk.lockKey, lk.clientId, version)
			if err == rpc.OK { // 成功获取锁
				return
			}
		}

		// 锁被其他客户端占用，等待锁释放
		//fmt.Println(err)
		time.Sleep(time.Duration(i) * time.Millisecond)
		i += 10
	}

}

// Release other clients must wait until the first client has released the lock using Release.
func (lk *Lock) Release() {
	// Your code here
	i := 10
	for {
		value, version, err := lk.ck.Get(lk.lockKey)
		if err == rpc.OK && value == lk.clientId { // 锁的持有者释放锁
			lk.ck.Put(lk.lockKey, "", version)
			break
		}

		time.Sleep(time.Duration(i) * time.Millisecond)
		i += 10
	}

}
