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
	DPrintf("Leader %v send snapshot to %v %v at term %v\n", args.LeaderId, rf.state, rf.me, rf.currentTerm)

	reply.Term = rf.currentTerm

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persistState()
	} else {
		rf.state = follower
	}

	rf.setNextElectionTime()

	rf.snapshot = args.Data
	rf.snapshotIndex = args.LastIncludedIndex
	rf.snapshotTerm = args.LastIncludedTerm
	rf.applyCond.Signal()
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	if !rf.peers[server].Call("Raft.InstallSnapshot", args, reply) {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if reply.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = reply.Term
		rf.votedFor = -1
		rf.persistState()
		rf.setNextElectionTime()
		return
	}

	if rf.currentTerm != args.Term {
		return
	}

	rf.nextIndex[server] = args.LastIncludedIndex + 1
}

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if lastIncludedIndex <= rf.commitIndex {
		return false
	}

	DPrintf("Server %v condInstall snapshot with lastIncludedTerm %v lastIncludedIndex %v\n", rf.me, lastIncludedTerm, lastIncludedIndex)
	if lastIncludedIndex <= rf.log.lastLogIndex() && lastIncludedTerm == rf.log.termOf(lastIncludedIndex) {
		rf.log = *rf.log.slice(lastIncludedIndex+1, -1)
	} else {
		rf.log = *rf.log.new(lastIncludedIndex, lastIncludedTerm)
	}
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

	if index <= rf.log.LastIncludedIndex {
		return
	}
	DPrintf("%v %v take a snapshot at index %v\n", rf.state, rf.me, index)
	rf.log = *rf.log.slice(index+1, -1)
	rf.persistStateAndSnapshot(snapshot)
}
