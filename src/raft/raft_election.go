package raft

import (
	"math/rand"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Candidate %v request %v %v to vote at term %v\n", args.CandidateId, rf.state, rf.me, rf.currentTerm)

	reply.Term = rf.currentTerm
	reply.VoteGranted = false

	if args.Term < rf.currentTerm {
		return
	}

	if args.Term > rf.currentTerm {
		rf.state = follower
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persistState()
	}

	if args.LastLogTerm < rf.log.lastLogTerm() || (args.LastLogTerm == rf.log.lastLogTerm() && args.LastLogIndex < rf.log.lastLogIndex()) {
		return
	}

	if rf.votedFor == -1 {
		DPrintf("%v %v vote for candidate %v at term %v\n", rf.state, rf.me, args.CandidateId, rf.currentTerm)
		rf.votedFor = args.CandidateId
		rf.persistState()
		rf.setNextElectionTime()
		reply.VoteGranted = true
	} else if rf.votedFor == args.CandidateId {
		DPrintf("%v %v vote for candidate %v at term %v\n", rf.state, rf.me, args.CandidateId, rf.currentTerm)
		rf.setNextElectionTime()
		reply.VoteGranted = true
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	if !ok {
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

	if args.Term != rf.currentTerm {
		return
	}

	if reply.VoteGranted {
		rf.nVotes++
		if rf.nVotes == len(rf.peers)/2+1 {
			DPrintf("%v %v win election at term %v\n", rf.state, rf.me, rf.currentTerm)
			for i := range rf.nextIndex {
				rf.nextIndex[i] = rf.log.lastLogIndex() + 1
				rf.matchIndex[i] = 0
			}
			rf.state = leader
			rf.broadcastHeartbeat()
		}
	}
}

func (rf *Raft) startElection() {
	DPrintf("%v %v start electtion at term %v\n", rf.state, rf.me, rf.currentTerm)
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.log.lastLogIndex(),
		LastLogTerm:  rf.log.lastLogTerm(),
	}
	rf.nVotes = 1
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		reply := RequestVoteReply{}
		go rf.sendRequestVote(i, &args, &reply)
	}
	rf.setNextElectionTime()
}

func (rf *Raft) setNextElectionTime() {
	rf.nextElection = time.Now().Add(time.Duration(350+rand.Intn(150)) * time.Millisecond)
}
