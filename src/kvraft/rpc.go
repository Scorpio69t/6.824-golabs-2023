package kvraft

type status int

const (
	success = iota
	wrongLeader
)

type PutArgs struct {
	ClientId    int64
	SequenceNum int64
	Key         string
	Value       string
}

type PutReply struct {
	Status status
}

type AppendArgs struct {
	ClientId    int64
	SequenceNum int64
	Key         string
	Value       string
}

type AppendReply struct {
	Status status
}

type GetArgs struct {
	ClientId    int64
	SequenceNum int64
	Key         string
}

type GetReply struct {
	Status status
	Value  string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	DPrintf("Client %v wanna get key %v from server %v seq %v\n", args.ClientId, args.Key, kv.me, args.SequenceNum)
	reply.Status = wrongLeader

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
	kv.mu.Lock()
	kv.waitChs[index] = ch
	kv.mu.Unlock()
	o := <-ch
	if o.ClientId == args.ClientId && o.SequenceNum == args.SequenceNum {
		DPrintf("Client %v get key %v from server %v seq %v\n", args.ClientId, args.Key, kv.me, args.SequenceNum)
		reply.Status = success
		reply.Value = o.Value
	}
	kv.mu.Lock()
	delete(kv.waitChs, index)
	kv.mu.Unlock()
	DPrintf("Client %v fail to get key %v from server %v seq %v\n", args.ClientId, args.Key, kv.me, args.SequenceNum)
}

func (kv *KVServer) Put(args *PutArgs, reply *PutReply) {
	// Your code here.
	kv.mu.Lock()
	sequenceNum, ok := kv.lastSequenceNums[args.ClientId]
	kv.mu.Unlock()
	if ok && args.SequenceNum <= sequenceNum {
		reply.Status = success
		return
	}
	DPrintf("Client %v wanna put key %v to server %v seq %v\n", args.ClientId, args.Key, kv.me, args.SequenceNum)
	reply.Status = wrongLeader

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
	kv.mu.Lock()
	kv.waitChs[index] = ch
	kv.mu.Unlock()
	o := <-ch
	if o.ClientId == args.ClientId && o.SequenceNum == args.SequenceNum {
		DPrintf("Client %v put key %v to server %v seq %v\n", args.ClientId, args.Key, kv.me, args.SequenceNum)
		reply.Status = success
	}
	kv.mu.Lock()
	delete(kv.waitChs, index)
	kv.mu.Unlock()
	DPrintf("Client %v fail to put key %v to server %v seq %v\n", args.ClientId, args.Key, kv.me, args.SequenceNum)
}

func (kv *KVServer) Append(args *AppendArgs, reply *AppendReply) {
	// Your code here.
	kv.mu.Lock()
	sequenceNum, ok := kv.lastSequenceNums[args.ClientId]
	kv.mu.Unlock()
	if ok && args.SequenceNum <= sequenceNum {
		reply.Status = success
		return
	}
	DPrintf("Client %v wanna append key %v to server %v seq %v\n", args.ClientId, args.Key, kv.me, args.SequenceNum)
	reply.Status = wrongLeader

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
	kv.mu.Lock()
	kv.waitChs[index] = ch
	kv.mu.Unlock()
	o := <-ch
	if o.ClientId == args.ClientId && o.SequenceNum == args.SequenceNum {
		DPrintf("Client %v append key %v to server %v seq %v\n", args.ClientId, args.Key, kv.me, args.SequenceNum)
		reply.Status = success
	}
	kv.mu.Lock()
	delete(kv.waitChs, index)
	kv.mu.Unlock()
	DPrintf("Client %v fail to append key %v to server %v seq %v\n", args.ClientId, args.Key, kv.me, args.SequenceNum)
}
