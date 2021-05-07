package kvraft

import (
	"time"
)

const waitTimeout = 600 * time.Millisecond

type PutArgs struct {
	ClientId    int64
	SequenceNum int64
	Key         string
	Value       string
}

type PutReply struct {
	Status bool
}

type AppendArgs struct {
	ClientId    int64
	SequenceNum int64
	Key         string
	Value       string
}

type AppendReply struct {
	Status bool
}

type GetArgs struct {
	ClientId    int64
	SequenceNum int64
	Key         string
}

type GetReply struct {
	Status bool
	Value  string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Status = false

	op := Op{
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Type:        getOp,
		Key:         args.Key,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	ch := make(chan Op)
	kv.waitChs.Store(index, ch)
	select {
	case o := <-ch:
		if o.ClientId == args.ClientId && o.SequenceNum == args.SequenceNum {
			DPrintf("Server %v get key %v clientId %v sequenceNum %v index %v\n", kv.me, args.Key, args.ClientId, args.SequenceNum, index)
			reply.Status = true
			reply.Value = o.Value
		}
	case <-time.After(waitTimeout):
	}
	kv.waitChs.Delete(index)
}

func (kv *KVServer) Put(args *PutArgs, reply *PutReply) {
	// Your code here.
	sequenceNum, ok := kv.lastOps.Load(args.ClientId)
	if ok && args.SequenceNum == sequenceNum {
		reply.Status = true
		return
	}

	reply.Status = false

	op := Op{
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Type:        putOp,
		Key:         args.Key,
		Value:       args.Value,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	ch := make(chan Op)
	kv.waitChs.Store(index, ch)
	select {
	case o := <-ch:
		if o.ClientId == args.ClientId && o.SequenceNum == args.SequenceNum {
			DPrintf("Server %v put key %v value %v clientId %v sequenceNum %v index %v\n", kv.me, args.Key, args.Value, args.ClientId, args.SequenceNum, index)
			reply.Status = true
		}
	case <-time.After(waitTimeout):
	}
	kv.waitChs.Delete(index)
}

func (kv *KVServer) Append(args *AppendArgs, reply *AppendReply) {
	// Your code here.
	sequenceNum, ok := kv.lastOps.Load(args.ClientId)
	if ok && args.SequenceNum == sequenceNum {
		reply.Status = true
		return
	}

	reply.Status = false

	op := Op{
		ClientId:    args.ClientId,
		SequenceNum: args.SequenceNum,
		Type:        appendOp,
		Key:         args.Key,
		Value:       args.Value,
	}
	index, _, isLeader := kv.rf.Start(op)
	if !isLeader {
		return
	}

	ch := make(chan Op)
	kv.waitChs.Store(index, ch)
	select {
	case o := <-ch:
		if o.ClientId == args.ClientId && o.SequenceNum == args.SequenceNum {
			DPrintf("Server %v append key %v value %v clientId %v sequenceNum %v index %v\n", kv.me, args.Key, args.Value, args.ClientId, args.SequenceNum, index)
			reply.Status = true
		}
	case <-time.After(waitTimeout):
	}
	kv.waitChs.Delete(index)
}
