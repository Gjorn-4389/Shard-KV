package raft

func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendInstallSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.InstallSnapshot", args, reply)
	return ok
}

// compare which server is up-to-date
func (rf *Raft) logUpToDate(args *RequestVoteArgs) bool {
	curLastEntry := rf.getLastEntry()

	// compare last log entry term
	if args.LastLogTerm != curLastEntry.Term {
		return args.LastLogTerm > curLastEntry.Term
	}

	return args.LastLogIndex >= curLastEntry.Index
}

func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	rf.mu.Lock()
	defer func() {
		DPrintf(Lab2A, "[server-%d] [RequestVote] [Reply] state: {state=%v, term=%v, commitIndex=%v, lastApplied=%v, firstLog=%v, lastLog=%v}, args = %v, reply = %v",
			rf.me, rf.state, rf.CurrentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstEntry(), rf.getLastEntry(), args, reply)
		rf.persist()
		rf.mu.Unlock()
	}()

	// previous term RequestVote || has voted to others
	if args.Term < rf.CurrentTerm || (args.Term == rf.CurrentTerm && rf.VotedFor != -1 && rf.VotedFor != args.CandidateId) {
		reply.Term, reply.VoteGranted = rf.CurrentTerm, false
		return
	}
	// receive new term server's RequestVote
	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term)
	}

	if !rf.logUpToDate(args) {
		reply.Term, reply.VoteGranted = rf.CurrentTerm, false
		return
	}

	rf.VotedFor = args.CandidateId
	rf.updateCommunicateTimeToNow()
	reply.Term, reply.VoteGranted = rf.CurrentTerm, true
}

// handle AppendEntries RPC
func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer func() {
		if len(args.Entries) != 0 {
			DPrintf(Lab2B, "[server-%d] [AppendEntries] [Reply] state is {state=%v, term=%v, commitIndex=%v, lastApplied=%v, firstLog=%v, lastLog=%v}, args = %v, reply = %v",
				rf.me, rf.state, rf.CurrentTerm, rf.commitIndex, rf.lastApplied, rf.getFirstEntry(), rf.getLastEntry(), args, reply)
		}
		rf.persist()
		rf.mu.Unlock()
	}()

	// previous term AppendEntries
	if args.Term < rf.CurrentTerm {
		reply.Term, reply.Success = rf.CurrentTerm, false
		return
	}
	// receive new term server's AppendEntries
	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.state = Follower
	rf.updateCommunicateTimeToNow()

	// stale AE RPC after install new snapshot
	if args.PrevLogIndex < rf.getFirstEntry().Index {
		reply.Term, reply.Success = 0, false
		reply.ConflictIndex = rf.getFirstEntry().Index + 1
		// DPrintf(Lab2D, "[server-%d] [leader-%d] [AppendEntries] stale AE RPC after install new snapshot, lastIncludedIndex = %d", rf.me, args.LeaderId, rf.getFirstEntry().Index)
		return
	}

	// log doesn't contain an entry at prevLogIndex
	if rf.getLastEntry().Index < args.PrevLogIndex {
		reply.Term, reply.Success = rf.CurrentTerm, false
		reply.ConflictIndex, reply.ConflictTerm = rf.getLastEntry().Index+1, -1
		return
	}

	// compare previous Log entry in server with leader
	if rf.getEntry(args.PrevLogIndex).Term != args.PrevLogTerm {
		reply.Term, reply.Success = rf.CurrentTerm, false
		reply.ConflictIndex, reply.ConflictTerm = args.PrevLogIndex, rf.getEntry(args.PrevLogIndex).Term
		// find the first index which term = ConflictTerm
		for i := args.PrevLogIndex - 1; i >= rf.getFirstEntry().Index; i-- {
			if rf.getEntry(i).Term == reply.ConflictTerm {
				reply.ConflictIndex = i
			} else {
				break
			}
		}
		return
	}

	// add entries from AppendEntries RPC
	for index, entry := range args.Entries {
		if entry.Index-rf.getFirstEntry().Index >= len(rf.Log) || rf.getEntry(entry.Index).Term != entry.Term {
			rf.addEntries(entry.Index-1, args.Entries[index:])
			break
		}
	}

	// update commitIndex
	if args.LeaderCommit > rf.commitIndex {
		rf.setCommitIndex(args.LeaderCommit)
	}

	// success appendEntry || heartbeat
	reply.Success, reply.ConflictIndex = true, rf.getLastEntry().Index+1

}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer func() {
		reply.Term = rf.CurrentTerm
		rf.persist()
		rf.mu.Unlock()
	}()

	// previous term AppendEntries
	if args.Term < rf.CurrentTerm {
		return
	}
	// receive new term server's AppendEntries
	if args.Term > rf.CurrentTerm {
		rf.convertToFollower(args.Term)
	}

	rf.state = Follower
	rf.updateCommunicateTimeToNow()

	// stale snapshot
	if args.LastIncludedIndex <= rf.commitIndex {
		return
	}

	go func() {
		// send snapshot to applyCh
		rf.applyCh <- ApplyMsg{
			SnapshotValid: true,
			Snapshot:      args.Snapshot,
			SnapshotTerm:  args.LastIncludedTerm,
			SnapshotIndex: args.LastIncludedIndex,
		}
	}()
}
