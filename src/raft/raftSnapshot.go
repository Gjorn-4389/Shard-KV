package raft

//
// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
//
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {
	// Your code here (2D).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// stale snapshot
	if lastIncludedIndex <= rf.commitIndex {
		DPrintf(Lab2D, "[server-%d] [CondInstallSnapshot] reject: receive stale snapshot", rf.me)
		return false
	}

	if rf.lastApplied < rf.commitIndex {
		DPrintf(Lab2D, "[server-%d] [CondInstallSnapshot] reject: applying message", rf.me)
		return false
	}

	// overwrite logs
	if lastIncludedIndex > rf.getLastEntry().Index {
		rf.Log = make([]LogEntry, 1)
	} else {
		// add remain log entries
		rf.Log = append(make([]LogEntry, 0), rf.Log[lastIncludedIndex-rf.getFirstEntry().Index:]...)
	}
	rf.Log[0].Index, rf.Log[0].Term = lastIncludedIndex, lastIncludedTerm
	rf.commitIndex = Max(rf.commitIndex, lastIncludedIndex)
	rf.lastApplied = Max(rf.lastApplied, lastIncludedIndex)
	// persist snapshot & raft state
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
	DPrintf(Lab2D, "[server-%v] [CondInstallSnapshot] state: {term=%v, commitIndex=%v, lastApplied=%v, snapshot=%v, log=%v}", rf.me, rf.CurrentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstEntry(), rf.Log)
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

	if index <= rf.getFirstEntry().Index {
		DPrintf(Lab2D, "[server-%d] last included index = %d, reject replicate", rf.me, rf.getFirstEntry().Index)
		return
	}

	// overwrite logs
	rf.Log = append(make([]LogEntry, 0), rf.Log[index-rf.getFirstEntry().Index:]...)
	rf.Log[0].Index, rf.Log[0].Term = index, rf.getEntry(index).Term
	rf.Log[0].Command = nil
	rf.persister.SaveStateAndSnapshot(rf.persistData(), snapshot)
	DPrintf(Lab2D, "[server-%v] after snapshot: {term=%v, commitIndex=%v, lastApplied=%v, snapshot=%v, lastLog=%v}", rf.me, rf.CurrentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstEntry(), rf.getLastEntry())
}

func (rf *Raft) sendSnapshot(serverIndex int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// leader is outdated
	if reply.Term > rf.CurrentTerm {
		rf.convertToFollower(reply.Term)
		return
	}

	if rf.state != Leader || rf.CurrentTerm != args.Term {
		return
	}

	// set leader's nextIndex[], matchIndex[]
	rf.setNextIndex(serverIndex, args.LastIncludedIndex+1)
	rf.setMatchIndex(serverIndex, args.LastIncludedIndex)
}
