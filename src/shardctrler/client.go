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
	mu       sync.Mutex
	id       int64
	leaderId int
	nextSeq  int64
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// Your code here.
	ck.id = nrand()
	ck.leaderId = rand.Intn(len(ck.servers))
	ck.nextSeq = 1
	return ck
}

func (ck *Clerk) Query(configNum int) Config {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := QueryArgs{
		Cid:       ck.id,
		Seq:       ck.nextSeq,
		ConfigNum: configNum,
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
					ck.nextSeq++
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
		Cid:     ck.id,
		Seqs:    ck.nextSeq,
		Servers: servers,
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
					ck.nextSeq++
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
		Cid:  ck.id,
		Seq:  ck.nextSeq,
		Gids: gids,
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
					ck.nextSeq++
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

func (ck *Clerk) Move(sid int, gid int) {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	args := MoveArgs{
		Cid: ck.id,
		Seq: ck.nextSeq,
		Sid: sid,
		Gid: gid,
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
					ck.nextSeq++
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
