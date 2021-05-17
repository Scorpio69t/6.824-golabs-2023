package kvraft

import (
	"math/rand"
	"sync"
	"time"

	"6.824/labrpc"
)

const timeout = 1000 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	mu              sync.Mutex
	id              int64
	leaderId        int
	nextSequenceNum int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.leaderId = rand.Intn(len(ck.servers))
	ck.nextSequenceNum = 1
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := GetArgs{
		ClientId:    ck.id,
		SequenceNum: ck.nextSequenceNum,
		Key:         key,
	}
	for {
		i := ck.leaderId
		for {
			reply := GetReply{}
			ok := make(chan bool)
			err := make(chan bool)
			go func() {
				if ck.servers[i].Call("KVServer.Get", &args, &reply) {
					ok <- true
				} else {
					err <- true
				}
			}()
			select {
			case <-ok:
				if reply.Status == success {
					ck.leaderId = i
					ck.nextSequenceNum++
					return reply.Value
				}
			case <-err:
			case <-time.After(timeout):
			}
			i = (i + 1) % len(ck.servers)
			if i == ck.leaderId {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := PutArgs{
		ClientId:    ck.id,
		SequenceNum: ck.nextSequenceNum,
		Key:         key,
		Value:       value,
	}
	for {
		i := ck.leaderId
		for {
			reply := PutReply{}
			ok := make(chan bool)
			err := make(chan bool)
			go func() {
				if ck.servers[i].Call("KVServer.Put", &args, &reply) {
					ok <- true
				} else {
					err <- true
				}
			}()
			select {
			case <-ok:
				if reply.Status == success {
					ck.leaderId = i
					ck.nextSequenceNum++
					return
				}
			case <-err:
			case <-time.After(timeout):
			}
			i = (i + 1) % len(ck.servers)
			if i == ck.leaderId {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Append(key string, value string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := AppendArgs{
		ClientId:    ck.id,
		SequenceNum: ck.nextSequenceNum,
		Key:         key,
		Value:       value,
	}
	for {
		i := ck.leaderId
		for {
			reply := AppendReply{}
			ok := make(chan bool)
			err := make(chan bool)
			go func() {
				if ck.servers[i].Call("KVServer.Append", &args, &reply) {
					ok <- true
				} else {
					err <- true
				}
			}()
			select {
			case <-ok:
				if reply.Status == success {
					ck.leaderId = i
					ck.nextSequenceNum++
					return
				}
			case <-err:
			case <-time.After(timeout):
			}
			i = (i + 1) % len(ck.servers)
			if i == ck.leaderId {
				break
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
