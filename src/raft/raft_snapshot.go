package raft

type InstallSnapshotArgs struct {
	Term              int
	LeaderId          int
	LastIncludedIndex int
	LastIncludedTerm  int
	Data              []byte
}

type InstallSnapshotReply struct {
	Term int
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Leader %v install snapshot to %v %v at term %v\n", args.LeaderId, rf.state, rf.me, rf.currentTerm)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	rf.setNextElectionTime()

	msg := ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotIndex: args.LastIncludedIndex,
		SnapshotTerm:  args.LastIncludedTerm,
	}
	rf.messages = append(rf.messages, msg)
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if !rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm != args.Term {
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
	rf.matchIndex[server] = args.LastIncludedIndex
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedTerm < rf.log.LastIncludedTerm ||
		(lastIncludedTerm == rf.log.LastIncludedTerm && lastIncludedIndex <= rf.log.LastIncludedIndex) {
		return false
	}

	DPrintf("Server %v condInstalled snapshot with lastIncludedTerm %v lastIncludedIndex %v\n", rf.me, lastIncludedTerm, lastIncludedIndex)
	rf.log.make(lastIncludedIndex, lastIncludedTerm)
	rf.persistStateAndSnapshot(snapshot)
	rf.lastApplied = lastIncludedIndex
	rf.commitIndex = lastIncludedIndex

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("%v %v take a snapshot at index %v\n", rf.state, rf.me, index)
	rf.log = *rf.log.slice(index+1, -1)
	rf.persistStateAndSnapshot(snapshot)
}
