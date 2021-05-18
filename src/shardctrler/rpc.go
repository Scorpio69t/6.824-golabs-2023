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

const (
	success = iota
	wrongLeader
)

type JoinArgs struct {
	Cid     int64
	Seqs    int64
	Servers map[int][]string // new GID -> servers mappings
}

type JoinReply struct {
	Status int
}

func (sc *ShardCtrler) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sc.mu.Lock()
	seq, ok := sc.lastSeqs[args.Cid]
	sc.mu.Unlock()
	if ok && args.Seqs <= seq {
		reply.Status = success
		return
	}

	reply.Status = wrongLeader

	op := Op{
		Type:    joinOp,
		Cid:     args.Cid,
		Seq:     args.Seqs,
		Servers: args.Servers,
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
	if o.Cid == args.Cid && o.Seq == args.Seqs {
		reply.Status = success
		DPrintf("ShardCtrler join servers %v clientId %v seq %v\n", args.Servers, args.Cid, args.Seqs)
	}
	sc.mu.Lock()
	delete(sc.waitChs, index)
	sc.mu.Unlock()
}

type LeaveArgs struct {
	Cid  int64
	Seq  int64
	Gids []int
}

type LeaveReply struct {
	Status int
}

func (sc *ShardCtrler) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sc.mu.Lock()
	seq, ok := sc.lastSeqs[args.Cid]
	sc.mu.Unlock()
	if ok && args.Seq <= seq {
		reply.Status = success
		return
	}

	reply.Status = wrongLeader

	op := Op{
		Type: leaveOp,
		Cid:  args.Cid,
		Seq:  args.Seq,
		Gids: args.Gids,
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
	if o.Cid == args.Cid && o.Seq == args.Seq {
		reply.Status = success
		DPrintf("ShardCtrler leave gids %v clientId %v seq %v\n", args.Gids, args.Cid, args.Seq)
	}
	sc.mu.Lock()
	delete(sc.waitChs, index)
	sc.mu.Unlock()
}

type MoveArgs struct {
	Cid int64
	Seq int64
	Sid int
	Gid int
}

type MoveReply struct {
	Status int
}

func (sc *ShardCtrler) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sc.mu.Lock()
	sequenceNum, ok := sc.lastSeqs[args.Cid]
	sc.mu.Unlock()
	if ok && args.Seq <= sequenceNum {
		reply.Status = success
		return
	}

	reply.Status = wrongLeader

	op := Op{
		Type: moveOp,
		Cid:  args.Cid,
		Seq:  args.Seq,
		Sid:  args.Sid,
		Gid:  args.Gid,
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
	if o.Cid == args.Cid && o.Seq == args.Seq {
		reply.Status = success
		DPrintf("ShardCtrler move shard %v to gid %v clientId %v seq %v\n", args.Sid, args.Gid, args.Cid, args.Seq)
	}
	sc.mu.Lock()
	delete(sc.waitChs, index)
	sc.mu.Unlock()
}

type QueryArgs struct {
	Cid       int64
	Seq       int64
	ConfigNum int // desired config number
}

type QueryReply struct {
	Status int
	Config Config
}

func (sc *ShardCtrler) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	reply.Status = wrongLeader

	op := Op{
		Type:      queryOp,
		Cid:       args.Cid,
		Seq:       args.Seq,
		ConfigNum: args.ConfigNum,
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
	if o.Cid == args.Cid && o.Seq == args.Seq {
		reply.Status = success
		reply.Config = o.Config
	}
	sc.mu.Lock()
	delete(sc.waitChs, index)
	sc.mu.Unlock()
}
