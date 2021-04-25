package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"sync"
	"sync/atomic"
	"time"

	"6.824/labrpc"
)

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

type serverState string

const (
	follower  serverState = "Follower"
	candidate serverState = "Candidate"
	leader    serverState = "Leader"
)

//
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh   chan ApplyMsg
	applyCond *sync.Cond

	currentTerm int
	votedFor    int
	log         raftLog

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	state         serverState
	nextElection  time.Time
	nextHeartbeat time.Time
	nVotes        int
	messages      []ApplyMsg
}

//
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (2A, 2B, 2C).

	rf.applyCh = applyCh
	rf.applyCond = sync.NewCond(&rf.mu)

	rf.log.make(0, 0)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))

	rf.state = follower
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.setNextElectionTime()

	// initialize from state persisted before a crash
	rf.restoreState()

	// start ticker goroutine to start elections
	go rf.ticker()

	go rf.messageListener()

	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.currentTerm, rf.state == leader
}

//
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state == leader {
		DPrintf("%v %v receive a command\n", rf.state, rf.me)
		term = rf.currentTerm
		index = rf.log.lastLogIndex() + 1
		isLeader = true
		rf.log.append(entry{Term: term, Command: command})
		rf.persistState()
		rf.broadcastHeartbeat()
	}

	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		time.Sleep(50 * time.Millisecond)
		rf.mu.Lock()
		switch rf.state {
		case follower:
			if time.Now().After(rf.nextElection) {
				rf.state = candidate
				rf.currentTerm++
				rf.votedFor = rf.me
				rf.persistState()
				rf.startElection()
			}
		case candidate:
			if time.Now().After(rf.nextElection) {
				rf.currentTerm++
				rf.persistState()
				rf.startElection()
			}
		case leader:
			if time.Now().After(rf.nextHeartbeat) {
				rf.broadcastHeartbeat()
			}
		}
		rf.mu.Unlock()
	}
}

func (rf *Raft) messageListener() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	for !rf.killed() {
		if len(rf.messages) > 0 {
			ms := rf.messages
			rf.messages = []ApplyMsg{}
			rf.mu.Unlock()
			for _, m := range ms {
				rf.applyCh <- m
				if m.CommandValid {
					DPrintf("Server %v applied a command %v\n", rf.me, m.CommandIndex)
				}
				if m.SnapshotValid {
					DPrintf("Server %v applied a snapshot with index %v term %v\n", rf.me, m.SnapshotIndex, m.SnapshotTerm)
				}
			}
			rf.mu.Lock()
		} else {
			rf.applyCond.Wait()
		}
	}
}
