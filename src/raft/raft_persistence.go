package raft

import (
	"bytes"
	"log"

	"6.824/labgob"
)

func (rf *Raft) serializeState() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persistState() {
	// Your code here (2C).
	// Example:
	state := rf.serializeState()
	rf.persister.SaveRaftState(state)
}

//
// restore previously persisted state.
//
func (rf *Raft) restoreState() {
	state := rf.persister.ReadRaftState()
	if state == nil || len(state) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	// Example:
	r := bytes.NewBuffer(state)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var raftLog raftLog
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&raftLog) != nil {
		log.Fatalln("Cannot decode persisted state")
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = raftLog
		rf.lastApplied = raftLog.LastIncludedIndex
		rf.commitIndex = raftLog.LastIncludedIndex
	}
}

func (rf *Raft) persistStateAndSnapshot(snapshot []byte) {
	state := rf.serializeState()
	rf.persister.SaveStateAndSnapshot(state, snapshot)
}
