package kvraft

import (
	"math/rand"
	"sync"

	"6.824/labrpc"
)

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
	reply := GetReply{}
	id := ck.leaderId
	for {
		ok := ck.servers[id].Call("KVServer.Get", &args, &reply)
		if ok && reply.Status {
			ck.leaderId = id
			break
		}
		id = rand.Intn(len(ck.servers))
	}
	ck.nextSequenceNum++
	return reply.Value
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
	reply := PutReply{}
	id := ck.leaderId
	for {
		ok := ck.servers[id].Call("KVServer.Put", &args, &reply)
		if ok && reply.Status {
			ck.leaderId = id
			break
		}
		id = rand.Intn(len(ck.servers))
	}
	ck.nextSequenceNum++
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
	reply := AppendReply{}
	id := ck.leaderId
	for {
		ok := ck.servers[id].Call("KVServer.Append", &args, &reply)
		if ok && reply.Status {
			ck.leaderId = id
			break
		}
		id = rand.Intn(len(ck.servers))
	}
	ck.nextSequenceNum++
}
