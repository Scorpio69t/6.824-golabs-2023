package shardctrler

//
// Shardctrler clerk.
//

import (
	"math/rand"
	"sync"
	"time"

	"6.824/labrpc"
)

const timeout = 1000 * time.Millisecond

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	mu              sync.Mutex
	id              int64
	leaderId        int
	nextSequenceNum int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = nrand()
	ck.leaderId = rand.Intn(len(ck.servers))
	ck.nextSequenceNum = 1
	return ck
}

func (ck *Clerk) Query(num int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := QueryArgs{
		ClientId:    ck.id,
		SequenceNum: ck.nextSequenceNum,
		ConfigNum:   num,
	}
	for {
		i := ck.leaderId
		for {
			reply := QueryReply{}
			ok := make(chan bool)
			err := make(chan bool)
			go func() {
				if ck.servers[i].Call("ShardCtrler.Query", &args, &reply) {
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
					return reply.Config
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

func (ck *Clerk) Join(servers map[int][]string) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := JoinArgs{
		ClientId:    ck.id,
		SequenceNum: ck.nextSequenceNum,
		Servers:     servers,
	}
	for {
		i := ck.leaderId
		for {
			reply := JoinReply{}
			ok := make(chan bool)
			err := make(chan bool)
			go func() {
				if ck.servers[i].Call("ShardCtrler.Join", &args, &reply) {
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

func (ck *Clerk) Leave(gids []int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := LeaveArgs{
		ClientId:    ck.id,
		SequenceNum: ck.nextSequenceNum,
		Gids:        gids,
	}
	for {
		i := ck.leaderId
		for {
			reply := LeaveReply{}
			ok := make(chan bool)
			err := make(chan bool)
			go func() {
				if ck.servers[i].Call("ShardCtrler.Leave", &args, &reply) {
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

func (ck *Clerk) Move(shard int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := MoveArgs{
		ClientId:    ck.id,
		SequenceNum: ck.nextSequenceNum,
		Shard:       shard,
		Gid:         gid,
	}
	for {
		i := ck.leaderId
		for {
			reply := MoveReply{}
			ok := make(chan bool)
			err := make(chan bool)
			go func() {
				if ck.servers[i].Call("ShardCtrler.Move", &args, &reply) {
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
