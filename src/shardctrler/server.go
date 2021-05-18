package shardctrler

import (
	"sort"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type opType string

const (
	joinOp  opType = "Join"
	leaveOp opType = "Leave"
	moveOp  opType = "Move"
	queryOp opType = "Query"
)

type Op struct {
	// Your data here.
	Cid  int64
	Seq  int64
	Type opType

	// For join op.
	Servers map[int][]string
	// For leave op.
	Gids []int
	// For move op.
	Sid int
	Gid int
	// For query op.
	ConfigNum int
	Config    Config
}

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid
	Groups map[int][]string // gid -> servers[]
}

type ShardCtrler struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32

	// Your data here.
	lastSeqs map[int64]int64
	waitChs  map[int]chan Op

	configs []Config // indexed by config num
}

//
// the tester calls Kill() when a ShardCtrler instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sc *ShardCtrler) Kill() {
	atomic.StoreInt32(&sc.dead, 1)
	sc.rf.Kill()
	// Your code here, if desired.
}

func (sc *ShardCtrler) killed() bool {
	return atomic.LoadInt32(&sc.dead) == 1
}

// needed by shardkv tester
func (sc *ShardCtrler) Raft() *raft.Raft {
	return sc.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant shardctrler service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardCtrler {
	sc := new(ShardCtrler)
	sc.me = me

	sc.configs = make([]Config, 1)
	sc.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sc.applyCh = make(chan raft.ApplyMsg)
	sc.rf = raft.Make(servers, me, persister, sc.applyCh)
	sc.lastSeqs = make(map[int64]int64)
	sc.waitChs = make(map[int]chan Op)

	// Your code here.
	go sc.reader()

	return sc
}

func (sc *ShardCtrler) reader() {
	for !sc.killed() {
		cmd := <-sc.applyCh
		if cmd.CommandValid {
			op := cmd.Command.(Op)
			if op.Type == queryOp {
				sc.query(&op)
			}

			sc.mu.Lock()
			if op.Seq > sc.lastSeqs[op.Cid] {
				switch op.Type {
				case joinOp:
					sc.join(&op)
				case leaveOp:
					sc.leave(&op)
				case moveOp:
					sc.move(&op)
				}
				sc.lastSeqs[op.Cid] = op.Seq
			}
			ch, ok := sc.waitChs[cmd.CommandIndex]
			sc.mu.Unlock()

			if ok {
				ch <- op
			}
		}
	}
}

func (sc *ShardCtrler) query(op *Op) {
	lastNum := len(sc.configs) - 1
	if op.ConfigNum == -1 || op.ConfigNum > lastNum {
		op.Config = sc.configs[lastNum]
	} else {
		op.Config = sc.configs[op.ConfigNum]
	}
}

func (sc *ShardCtrler) join(op *Op) {
	lastNum := len(sc.configs) - 1
	oldShards := sc.configs[lastNum].Shards
	oldGroup := sc.configs[lastNum].Groups
	newShards := [NShards]int{}
	newGroup := make(map[int][]string)
	m := make(map[int][]int)

	for sid, gid := range oldShards {
		m[gid] = append(m[gid], sid)
	}

	for gid, names := range oldGroup {
		newGroup[gid] = names
	}

	for gid, names := range op.Servers {
		m[gid] = []int{}
		newGroup[gid] = names
	}

	unassigned := m[0]
	delete(m, 0)

	avg := NShards / len(m)

	gids := []int{}
	for gid := range m {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	for _, gid := range gids {
		for len(m[gid]) > avg {
			unassigned = append(unassigned, m[gid][0])
			m[gid] = m[gid][1:]
		}
	}

	for _, gid := range gids {
		for len(m[gid]) < avg {
			m[gid] = append(m[gid], unassigned[0])
			unassigned = unassigned[1:]
		}
	}

	for _, gid := range gids {
		if len(unassigned) == 0 {
			break
		}
		m[gid] = append(m[gid], unassigned[0])
		unassigned = unassigned[1:]
	}

	for gid, sids := range m {
		for _, sid := range sids {
			newShards[sid] = gid
		}
	}

	newConfig := Config{
		Num:    lastNum + 1,
		Shards: newShards,
		Groups: newGroup,
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) leave(op *Op) {
	lastNum := len(sc.configs) - 1
	oldShards := sc.configs[lastNum].Shards
	oldGroup := sc.configs[lastNum].Groups
	newShards := [NShards]int{}
	newGroup := make(map[int][]string)
	m := make(map[int][]int)

	for sid, gid := range oldShards {
		m[gid] = append(m[gid], sid)
	}

	for gid, names := range oldGroup {
		newGroup[gid] = names
	}

	unassigned := []int{}

	for _, gid := range op.Gids {
		unassigned = append(unassigned, m[gid]...)
		delete(m, gid)
		delete(newGroup, gid)
	}

	if len(m) == 0 {
		newConfig := Config{
			Num:    lastNum + 1,
			Shards: newShards,
			Groups: newGroup,
		}
		sc.configs = append(sc.configs, newConfig)
		return
	}

	avg := NShards / len(m)

	gids := []int{}
	for gid := range m {
		gids = append(gids, gid)
	}
	sort.Ints(gids)

	for _, gid := range gids {
		for len(m[gid]) < avg && len(unassigned) > 0 {
			m[gid] = append(m[gid], unassigned[0])
			unassigned = unassigned[1:]
		}
	}

	for _, gid := range gids {
		if len(unassigned) == 0 {
			break
		}
		m[gid] = append(m[gid], unassigned[0])
		unassigned = unassigned[1:]
	}

	for gid, sids := range m {
		for _, sid := range sids {
			newShards[sid] = gid
		}
	}

	newConfig := Config{
		Num:    lastNum + 1,
		Shards: newShards,
		Groups: newGroup,
	}
	sc.configs = append(sc.configs, newConfig)
}

func (sc *ShardCtrler) move(op *Op) {
	lastNum := len(sc.configs) - 1
	oldShards := sc.configs[lastNum].Shards
	oldGroup := sc.configs[lastNum].Groups

	newShards := [NShards]int{}
	for sid, gid := range oldShards {
		newShards[sid] = gid
	}
	newGroup := make(map[int][]string)
	for gid, names := range oldGroup {
		newGroup[gid] = names
	}

	newShards[op.Sid] = op.Gid

	newConfig := Config{
		Num:    lastNum + 1,
		Shards: newShards,
		Groups: newGroup,
	}
	sc.configs = append(sc.configs, newConfig)
}
