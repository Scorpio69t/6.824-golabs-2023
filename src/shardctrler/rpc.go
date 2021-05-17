package shardctrler

//
// Shard controler: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

type status int

const (
	success = iota
	wrongLeader
)

type JoinArgs struct {
	ClientId    int64
	SequenceNum int64
	Servers     map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Status status
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	sequenceNum, ok := sc.lastSequenceNums[args.ClientId]
	sc.mu.Unlock()
	if ok && args.SequenceNum <= sequenceNum {
		reply.Status = success
		return
	}

	reply.Status = wrongLeader

	op := Op{
		Type:        joinOp,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Servers:     args.Servers,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}

	ch := make(chan Op)
	sc.mu.Lock()
	sc.waitChs[index] = ch
	sc.mu.Unlock()
	o := <-ch
	if o.ClientId == args.ClientId && o.SequenceNum == args.SequenceNum {
		reply.Status = success
		DPrintf("ShardCtrler join servers %v clientId %v sequenceNum %v\n", args.Servers, args.ClientId, args.SequenceNum)
	}
	sc.mu.Lock()
	delete(sc.waitChs, index)
	sc.mu.Unlock()
}

type LeaveArgs struct {
	ClientId    int64
	SequenceNum int64
	Gids        []int
}

type LeaveReply struct {
	Status status
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	sequenceNum, ok := sc.lastSequenceNums[args.ClientId]
	sc.mu.Unlock()
	if ok && args.SequenceNum <= sequenceNum {
		reply.Status = success
		return
	}

	reply.Status = wrongLeader

	op := Op{
		Type:        leaveOp,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Gids:        args.Gids,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}

	ch := make(chan Op)
	sc.mu.Lock()
	sc.waitChs[index] = ch
	sc.mu.Unlock()
	o := <-ch
	if o.ClientId == args.ClientId && o.SequenceNum == args.SequenceNum {
		reply.Status = success
		DPrintf("ShardCtrler leave gids %v clientId %v sequenceNum %v\n", args.Gids, args.ClientId, args.SequenceNum)
	}
	sc.mu.Lock()
	delete(sc.waitChs, index)
	sc.mu.Unlock()
}

type MoveArgs struct {
	ClientId    int64
	SequenceNum int64
	Shard       int
	Gid         int
}

type MoveReply struct {
	Status status
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	sequenceNum, ok := sc.lastSequenceNums[args.ClientId]
	sc.mu.Unlock()
	if ok && args.SequenceNum <= sequenceNum {
		reply.Status = success
		return
	}

	reply.Status = wrongLeader

	op := Op{
		Type:        moveOp,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Shard:       args.Shard,
		Gid:         args.Gid,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}

	ch := make(chan Op)
	sc.mu.Lock()
	sc.waitChs[index] = ch
	sc.mu.Unlock()
	o := <-ch
	if o.ClientId == args.ClientId && o.SequenceNum == args.SequenceNum {
		reply.Status = success
		DPrintf("ShardCtrler move shard %v to gid %v clientId %v sequenceNum %v\n", args.Shard, args.Gid, args.ClientId, args.SequenceNum)
	}
	sc.mu.Lock()
	delete(sc.waitChs, index)
	sc.mu.Unlock()
}

type QueryArgs struct {
	ClientId    int64
	SequenceNum int64
	ConfigNum   int // desired config number
}

type QueryReply struct {
	Status status
	Config Config
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.Status = wrongLeader

	op := Op{
		Type:        queryOp,
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		ConfigNum:   args.ConfigNum,
	}
	index, _, isLeader := sc.rf.Start(op)
	if !isLeader {
		return
	}

	ch := make(chan Op)
	sc.mu.Lock()
	sc.waitChs[index] = ch
	sc.mu.Unlock()
	o := <-ch
	if o.ClientId == args.ClientId && o.SequenceNum == args.SequenceNum {
		reply.Status = success
		reply.Config = o.Config
	}
	sc.mu.Lock()
	delete(sc.waitChs, index)
	sc.mu.Unlock()
}
