package kvraft

const (
	success = iota
	wrongLeader
)

type PutArgs struct {
	Cid   int64
	Seq   int64
	Key   string
	Value string
}

type PutReply struct {
	Status int
}

type AppendArgs struct {
	Cid   int64
	Seq   int64
	Key   string
	Value string
}

type AppendReply struct {
	Status int
}

type GetArgs struct {
	Cid int64
	Seq int64
	Key string
}

type GetReply struct {
	Status int
	Value  string
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	reply.Status = wrongLeader

	op := Op{
		Cid:  args.Cid,
		Seq:  args.Seq,
		Type: getOp,
		Key:  args.Key,
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
	if o.Cid == args.Cid && o.Seq == args.Seq {
		DPrintf("Client %v get key %v from server %v seq %v\n", args.Cid, args.Key, kv.me, args.Seq)
		reply.Status = success
		reply.Value = o.Value
	} else {
		DPrintf("Client %v fail to get key %v from server %v seq %v\n", args.Cid, args.Key, kv.me, args.Seq)
	}
	kv.mu.Lock()
	delete(kv.waitChs, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutArgs, reply *PutReply) {
	// Your code here.
	kv.mu.Lock()
	seq := kv.lastSeqs[args.Cid]
	kv.mu.Unlock()
	if args.Seq <= seq {
		reply.Status = success
		return
	}
	reply.Status = wrongLeader

	op := Op{
		Cid:   args.Cid,
		Seq:   args.Seq,
		Type:  putOp,
		Key:   args.Key,
		Value: args.Value,
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
	if o.Cid == args.Cid && o.Seq == args.Seq {
		DPrintf("Client %v put key %v to server %v seq %v\n", args.Cid, args.Key, kv.me, args.Seq)
		reply.Status = success
	} else {
		DPrintf("Client %v fail to put key %v to server %v seq %v\n", args.Cid, args.Key, kv.me, args.Seq)
	}
	kv.mu.Lock()
	delete(kv.waitChs, index)
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *AppendArgs, reply *AppendReply) {
	// Your code here.
	kv.mu.Lock()
	seq := kv.lastSeqs[args.Cid]
	kv.mu.Unlock()
	if args.Seq <= seq {
		reply.Status = success
		return
	}
	reply.Status = wrongLeader

	op := Op{
		Cid:   args.Cid,
		Seq:   args.Seq,
		Type:  appendOp,
		Key:   args.Key,
		Value: args.Value,
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
	if o.Cid == args.Cid && o.Seq == args.Seq {
		DPrintf("Client %v append key %v to server %v seq %v\n", args.Cid, args.Key, kv.me, args.Seq)
		reply.Status = success
	} else {
		DPrintf("Client %v fail to append key %v to server %v seq %v\n", args.Cid, args.Key, kv.me, args.Seq)
	}
	kv.mu.Lock()
	delete(kv.waitChs, index)
	kv.mu.Unlock()
}
