package raft

type entry struct {
	Term    int
	Command interface{}
}

type raftLog struct {
	LastIncludedIndex int
	LastIncludedTerm  int
	Entries           []entry
}

func (rl *raftLog) new(lastIncludedIndex int, lastIncludedTerm int) *raftLog {
	newLog := raftLog{
		LastIncludedIndex: lastIncludedIndex,
		LastIncludedTerm:  lastIncludedTerm,
		Entries:           []entry{},
	}
	return &newLog
}

func (rl *raftLog) lastLogIndex() int {
	return rl.LastIncludedIndex + len(rl.Entries)
}

func (rl *raftLog) lastLogTerm() int {
	return rl.termOf(rl.lastLogIndex())
}

func (rl *raftLog) transformIndex(index int) int {
	return index - rl.LastIncludedIndex - 1
}

func (rl *raftLog) termOf(index int) int {
	if index == rl.LastIncludedIndex {
		return rl.LastIncludedTerm
	}
	i := rl.transformIndex(index)
	return rl.Entries[i].Term
}

func (rl *raftLog) append(entry ...entry) {
	rl.Entries = append(rl.Entries, entry...)
}

func (rl *raftLog) slice(start int, end int) *raftLog {
	if start < 1 {
		if end < 1 {
			newLog := raftLog{
				LastIncludedIndex: rl.LastIncludedIndex,
				LastIncludedTerm:  rl.LastIncludedTerm,
				Entries:           rl.Entries,
			}
			return &newLog
		} else {
			e := rl.transformIndex(end)
			newLog := raftLog{
				LastIncludedIndex: rl.LastIncludedIndex,
				LastIncludedTerm:  rl.LastIncludedTerm,
				Entries:           rl.Entries[:e],
			}
			return &newLog
		}
	} else {
		s := rl.transformIndex(start)
		if end < 1 {
			newLog := raftLog{
				LastIncludedIndex: start - 1,
				LastIncludedTerm:  rl.termOf(start - 1),
				Entries:           rl.Entries[s:],
			}
			return &newLog
		} else {
			e := rl.transformIndex(end)
			newLog := raftLog{
				LastIncludedIndex: start - 1,
				LastIncludedTerm:  rl.termOf(start - 1),
				Entries:           rl.Entries[s:e],
			}
			return &newLog
		}
	}
}

func (rl *raftLog) clone() *raftLog {
	newEntries := []entry{}
	for _, e := range rl.Entries {
		newEntries = append(newEntries, entry{e.Term, e.Command})
	}
	newLog := raftLog{
		LastIncludedIndex: rl.LastIncludedIndex,
		LastIncludedTerm:  rl.LastIncludedTerm,
		Entries:           newEntries,
	}
	return &newLog
}

func (rl *raftLog) at(index int) *entry {
	i := rl.transformIndex(index)
	return &rl.Entries[i]
}
