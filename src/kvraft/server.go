package kvraft

import (
	"bytes"
	"log"
	"sync"
	"sync/atomic"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

type opType string

const (
	getOp    opType = "Get"
	putOp    opType = "Put"
	appendOp opType = "Append"
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId    int64
	SequenceNum int64
	Type        opType
	Key         string
	Value       string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db               map[string]string
	lastSequenceNums map[int64]int64
	waitChs          map[int]chan Op

	persister *raft.Persister
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.
	kv.persister = persister

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.db = make(map[string]string)
	kv.lastSequenceNums = make(map[int64]int64)
	kv.waitChs = make(map[int]chan Op)
	kv.restore()

	go kv.reader()

	return kv
}

func (kv *KVServer) reader() {
	for !kv.killed() {
		cmd := <-kv.applyCh
		if cmd.CommandValid {
			op := cmd.Command.(Op)
			kv.get(&op)

			kv.mu.Lock()
			if op.SequenceNum > kv.lastSequenceNums[op.ClientId] {
				switch op.Type {
				case putOp:
					kv.put(&op)
				case appendOp:
					kv.append(&op)
				}
				kv.lastSequenceNums[op.ClientId] = op.SequenceNum
			}

			if ch, ok := kv.waitChs[cmd.CommandIndex]; ok {
				ch <- op
			}
			kv.mu.Unlock()

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.rf.Snapshot(cmd.CommandIndex, kv.serialize())
			}
		} else if cmd.SnapshotValid {
			ok := kv.rf.CondInstallSnapshot(cmd.SnapshotTerm, cmd.SnapshotIndex, cmd.Snapshot)
			if ok {
				kv.mu.Lock()
				kv.switchTo(cmd.Snapshot)
				kv.mu.Unlock()
			}
		}
	}
}

func (kv *KVServer) serialize() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(kv.db)
	e.Encode(kv.lastSequenceNums)
	return w.Bytes()
}

func (kv *KVServer) switchTo(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[string]string
	var sequenceNums map[int64]int64
	if d.Decode(&db) != nil || d.Decode(&sequenceNums) != nil {
		log.Fatalln("KVServer: cannot deserialize")
	}
	kv.db = db
	kv.lastSequenceNums = sequenceNums
}

func (kv *KVServer) restore() {
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	kv.switchTo(snapshot)
}

func (kv *KVServer) get(op *Op) {
	if op.Type == getOp {
		val, ok := kv.db[op.Key]
		if ok {
			op.Value = val
		} else {
			op.Value = ""
		}
	}
}

func (kv *KVServer) put(op *Op) {
	if op.Value == "" {
		delete(kv.db, op.Key)
	} else {
		kv.db[op.Key] = op.Value
	}
}

func (kv *KVServer) append(op *Op) {
	if op.Value != "" {
		val, ok := kv.db[op.Key]
		if ok {
			kv.db[op.Key] = val + op.Value
		} else {
			kv.db[op.Key] = op.Value
		}
	}
}
