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
	Cid   int64 // Client id
	Seq   int64 // Sequence number
	Type  opType
	Key   string
	Value string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	data     map[string]string
	lastSeqs map[int64]int64
	waitChs  map[int]chan Op

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
	kv.data = make(map[string]string)
	kv.lastSeqs = make(map[int64]int64)
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
			if op.Type == getOp {
				op.Value = kv.data[op.Key]
			}

			kv.mu.Lock()
			if op.Seq > kv.lastSeqs[op.Cid] {
				switch op.Type {
				case putOp:
					if op.Value == "" {
						delete(kv.data, op.Key)
					} else {
						kv.data[op.Key] = op.Value
					}
				case appendOp:
					kv.data[op.Key] = kv.data[op.Key] + op.Value
				}

				kv.lastSeqs[op.Cid] = op.Seq
			}
			ch, ok := kv.waitChs[cmd.CommandIndex]
			kv.mu.Unlock()

			if ok {
				ch <- op
			}

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
	e.Encode(kv.data)
	e.Encode(kv.lastSeqs)
	return w.Bytes()
}

func (kv *KVServer) switchTo(snapshot []byte) {
	r := bytes.NewBuffer(snapshot)
	d := labgob.NewDecoder(r)
	data := make(map[string]string)
	seqs := make(map[int64]int64)
	if d.Decode(&data) != nil || d.Decode(&seqs) != nil {
		log.Fatalln("KVServer: cannot deserialize")
	}
	kv.data = data
	kv.lastSeqs = seqs
}

func (kv *KVServer) restore() {
	snapshot := kv.persister.ReadSnapshot()
	if snapshot == nil || len(snapshot) < 1 {
		return
	}
	kv.switchTo(snapshot)
}
