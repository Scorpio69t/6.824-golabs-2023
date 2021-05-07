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
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	db      sync.Map
	lastOps sync.Map
	waitChs sync.Map

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
	kv.restore()
	go kv.receiver()

	return kv
}

func (kv *KVServer) receiver() {
	for !kv.killed() {
		cmd := <-kv.applyCh
		if cmd.CommandValid {
			op := cmd.Command.(Op)
			if op.Type == getOp {
				val, ok := kv.db.Load(op.Key)
				if ok {
					op.Value = val.(string)
				} else {
					op.Value = ""
				}
			}
			sequenceNum, _ := kv.lastOps.LoadOrStore(op.ClientId, int64(0))
			if op.SequenceNum > sequenceNum.(int64) {
				switch op.Type {
				case putOp:
					if op.Value == "" {
						kv.db.Delete(op.Key)
					} else {
						kv.db.Store(op.Key, op.Value)
					}
				case appendOp:
					if op.Value != "" {
						val, ok := kv.db.Load(op.Key)
						if ok {
							kv.db.Store(op.Key, val.(string)+op.Value)
						} else {
							kv.db.Store(op.Key, op.Value)
						}
					}
				}
				kv.lastOps.Store(op.ClientId, op.SequenceNum)
			}
			if ch, ok := kv.waitChs.Load(cmd.CommandIndex); ok {
				ch.(chan Op) <- op
			}

			if kv.maxraftstate != -1 && kv.persister.RaftStateSize() > kv.maxraftstate {
				kv.rf.Snapshot(cmd.CommandIndex, kv.serialize())
			}
		} else if cmd.SnapshotValid {
			ok := kv.rf.CondInstallSnapshot(cmd.SnapshotTerm, cmd.SnapshotIndex, cmd.Snapshot)
			if ok {
				kv.switchTo(cmd.Snapshot)
			}
		}
	}
}

func (kv *KVServer) serialize() []byte {
	db := make(map[interface{}]interface{})
	kv.db.Range(func(k, v interface{}) bool {
		db[k] = v
		return true
	})
	ops := make(map[interface{}]interface{})
	kv.lastOps.Range(func(k, v interface{}) bool {
		ops[k] = v
		return true
	})
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(db)
	e.Encode(ops)
	return w.Bytes()
}

func (kv *KVServer) switchTo(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	var db map[interface{}]interface{}
	var ops map[interface{}]interface{}
	if d.Decode(&db) != nil || d.Decode(&ops) != nil {
		log.Fatalln("KVServer: cannot deserialize")
	}
	kv.db = sync.Map{}
	for k, v := range db {
		kv.db.Store(k, v)
	}
	kv.lastOps = sync.Map{}
	for k, v := range ops {
		kv.lastOps.Store(k, v)
	}
}

func (kv *KVServer) restore() {
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	kv.switchTo(snapshot)
}
