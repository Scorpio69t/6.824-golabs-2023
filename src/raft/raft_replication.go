package raft

import (
	"time"
)

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []entry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term          int
	Success       bool
	ConflictIndex int
	ConflictTerm  int
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("Leader %v append %v entries to %v %v at term %v\n", args.LeaderId, len(args.Entries), rf.state, rf.me, rf.currentTerm)

	reply.Term = rf.currentTerm
	reply.Success = false

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

	if args.PrevLogIndex > rf.log.lastLogIndex() || args.PrevLogIndex < rf.log.LastIncludedIndex {
		reply.ConflictIndex = rf.log.lastLogIndex()
		reply.ConflictTerm = -1
		return
	}

	if args.PrevLogTerm != rf.log.termOf(args.PrevLogIndex) {
		reply.ConflictTerm = rf.log.termOf(args.PrevLogIndex)
		i := args.PrevLogIndex - 1
		for i >= rf.log.LastIncludedIndex {
			if rf.log.termOf(i) != rf.log.termOf(args.PrevLogIndex) {
				break
			}
			i--
		}
		reply.ConflictIndex = i + 1
		return
	}

	reply.Success = true

	for i, e := range args.Entries {
		j := i + args.PrevLogIndex + 1
		if j > rf.log.lastLogIndex() {
			rf.log.append(args.Entries[i:]...)
			rf.persistState()
			break
		}
		if rf.log.termOf(j) != e.Term {
			rf.log = *rf.log.slice(-1, j)
			rf.log.append(args.Entries[i:]...)
			rf.persistState()
			break
		}
	}

	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = minInt(args.LeaderCommit, rf.log.lastLogIndex())
		rf.apply()
		rf.applyCond.Signal()
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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

	if reply.Success {
		if rf.matchIndex[server] < args.PrevLogIndex+len(args.Entries) {
			rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
			rf.nextIndex[server] = rf.matchIndex[server] + 1

			n := -1
			for i := rf.commitIndex + 1; i <= rf.log.lastLogIndex(); i++ {
				if rf.log.termOf(i) != rf.currentTerm {
					continue
				}
				count := 1
				for j := range rf.peers {
					if j == rf.me {
						continue
					}
					if rf.matchIndex[j] >= i {
						count++
					}
				}
				if count > len(rf.peers)/2 {
					n = i
				}
			}
			if n != -1 {
				rf.commitIndex = n
				rf.apply()
				rf.applyCond.Signal()
			}
		}
	} else {
		if reply.ConflictTerm == -1 {
			rf.nextIndex[server] = reply.ConflictIndex
		} else {
			i := -1
			for j := rf.log.lastLogIndex(); j >= rf.log.LastIncludedIndex; j-- {
				if rf.log.termOf(j) == reply.ConflictTerm {
					i = j
					break
				}
			}
			if i != -1 {
				rf.nextIndex[server] = i + 1
			} else {
				rf.nextIndex[server] = reply.ConflictIndex
			}
		}
	}
}

func (rf *Raft) broadcastHeartbeat() {
	DPrintf("%v %v broadcast heartbeat at term %v\n", rf.state, rf.me, rf.currentTerm)
	for i := range rf.peers {
		if i == rf.me {
			continue
		}
		if rf.nextIndex[i] > rf.log.LastIncludedIndex {
			args := AppendEntriesArgs{
				Term:         rf.currentTerm,
				LeaderId:     rf.me,
				PrevLogIndex: rf.nextIndex[i] - 1,
				PrevLogTerm:  rf.log.termOf(rf.nextIndex[i] - 1),
				LeaderCommit: rf.commitIndex,
			}
			if rf.log.lastLogIndex() >= rf.nextIndex[i] {
				args.Entries = rf.log.slice(rf.nextIndex[i], -1).clone().Entries
			}
			reply := AppendEntriesReply{}
			go rf.sendAppendEntries(i, &args, &reply)
		} else {
			args := InstallSnapshotArgs{
				Term:              rf.currentTerm,
				LeaderId:          rf.me,
				LastIncludedIndex: rf.log.LastIncludedIndex,
				LastIncludedTerm:  rf.log.LastIncludedTerm,
				Data:              rf.persister.ReadSnapshot(),
			}
			reply := InstallSnapshotReply{}
			go rf.sendInstallSnapshot(i, &args, &reply)
		}
	}
	rf.setNextHeartbeatTime()
}

func (rf *Raft) setNextHeartbeatTime() {
	rf.nextHeartbeat = time.Now().Add(100 * time.Millisecond)
}

func (rf *Raft) apply() {
	for i := rf.lastApplied + 1; i <= rf.commitIndex; i++ {
		m := ApplyMsg{
			CommandValid: true,
			Command:      rf.log.at(i).Command,
			CommandIndex: i,
		}
		rf.messages = append(rf.messages, m)
		rf.lastApplied++
	}
}
