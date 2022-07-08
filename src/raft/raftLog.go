package raft

import (
	"sort"
	"time"
)

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
	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index, term, isLeader := rf.getLastEntry().Index+1, rf.CurrentTerm, rf.state == Leader
	if !isLeader {
		return -1, term, isLeader
	}
	// add the new log entry to leader's log
	curLogEntry := LogEntry{
		Index:   index,
		Term:    term,
		Command: command,
	}

	rf.addEntry(curLogEntry)
	DPrintf(Lab2B, "[Leader-%d] add log entry: {%v}", rf.me, curLogEntry)

	return index, term, isLeader
}

// rf get agreement with server-i, no need for lock
func (rf *Raft) reachAgreementRound(heartbeat bool) {
	for i := 0; i < len(rf.peers); i++ {
		if rf.me == i {
			continue
		}
		go rf.reachAgreement(i, heartbeat)
	}
}

func (rf *Raft) reachAgreement(serverIndex int, heartbeat bool) {
	rf.mu.Lock()
	// only leader send AppendEntries RPC
	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	// curIndex = follower's next index
	curIndex := rf.nextIndex[serverIndex]

	// if previous log entry has in snapshot, can't get it
	// curIndex - 1 < lastIncludedIndex  => curIndex <= lastIncludedIndex(rf.getFirstEntry().Index)
	if curIndex <= rf.getFirstEntry().Index {
		args := InstallSnapshotArgs{
			Term:              rf.CurrentTerm,
			LeaderId:          rf.me,
			LastIncludedIndex: rf.getFirstEntry().Index,
			LastIncludedTerm:  rf.getFirstEntry().Term,
			Snapshot:          rf.persister.ReadSnapshot(),
		}
		rf.mu.Unlock()

		reply := InstallSnapshotReply{}
		if rf.sendInstallSnapshot(serverIndex, &args, &reply) {
			// send install snapshot
			rf.sendSnapshot(serverIndex, &args, &reply)
		}
	} else {
		// not heartbeat && leader has appended all entries
		if !heartbeat && curIndex > rf.getLastEntry().Index {
			rf.mu.Unlock()
			return
		}

		// construct AppendEntriesArgs
		args := AppendEntriesArgs{
			Term:         rf.CurrentTerm,
			LeaderId:     rf.me,
			LeaderCommit: rf.commitIndex,
			Entries:      append([]LogEntry{}, rf.Log[curIndex-rf.getFirstEntry().Index:]...),
			PrevLogIndex: rf.getEntry(curIndex - 1).Index,
			PrevLogTerm:  rf.getEntry(curIndex - 1).Term,
		}
		rf.mu.Unlock()

		// send AE RPC
		reply := AppendEntriesReply{}
		if rf.sendAppendEntries(serverIndex, &args, &reply) {
			rf.handleAppendEntriesReply(serverIndex, &args, &reply)
		}
	}
}

func (rf *Raft) handleAppendEntriesReply(serverIndex int, args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// update communicate time with followers
	rf.followerReplyTime[serverIndex] = time.Now()

	// receive higher Term
	if reply.Term > rf.CurrentTerm {
		rf.convertToFollower(reply.Term)
		return
	}

	// receive stale reply
	if args.Term != rf.CurrentTerm || rf.state != Leader {
		return
	}

	// success := agree with same entry
	if reply.Success {
		// ignore previous AE RPC reply, confirm matchIndex increase strictly
		if reply.ConflictIndex-1 > rf.matchIndex[serverIndex] {
			rf.setNextIndex(serverIndex, reply.ConflictIndex)
			rf.setMatchIndex(serverIndex, reply.ConflictIndex-1)
			DPrintf(Lab2B, "[follower-%d] -> [leader-%d]: AE RPC success, nextIndex = %d, matchIndex = %d", serverIndex, rf.me, rf.nextIndex[serverIndex], rf.matchIndex[serverIndex])
		}
	} else {
		// find the last Index at conflict term
		lastIndexWithTerm := -1
		for i := rf.getFirstEntry().Index + 1; i <= rf.getLastEntry().Index; i++ {
			if rf.getEntry(i).Term == reply.ConflictTerm {
				lastIndexWithTerm = i
			}
		}
		if lastIndexWithTerm == -1 {
			// there is no entry with ConflictIndex in leader's logs
			rf.setNextIndex(serverIndex, reply.ConflictIndex)
		} else {
			// nextIndex = first entry Index whose term > conflictTerm
			rf.setNextIndex(serverIndex, lastIndexWithTerm+1)
		}
		DPrintf(Lab2B, "[follower-%d] -> [leader-%d]: AE RPC fail, next time try index = %d", serverIndex, rf.me, reply.ConflictIndex)
	}
}

func (rf *Raft) checkCommitIndex() {
	// get matchIndex copy
	rf.matchIndex[rf.me] = rf.getLastEntry().Index
	curMatchIndex := make([]int, len(rf.peers))
	copy(curMatchIndex, rf.matchIndex)

	// sort matchIndex to get N(mid number)
	sort.Ints(curMatchIndex)
	N := curMatchIndex[len(curMatchIndex)/2]

	// update commitIndex
	if N > rf.commitIndex {
		DPrintf(Lab2B, "[leader-%d] update commitIndex = %d, current match Index: %v", rf.me, N, rf.matchIndex)
		rf.setCommitIndex(N)
	}
}

func (rf *Raft) ApplyMessage() {
	for !rf.killed() {
		rf.mu.Lock()
		for rf.lastApplied >= rf.commitIndex {
			rf.applyCond.Wait()
		}
		entry := rf.getEntry(rf.lastApplied + 1)
		rf.mu.Unlock()

		rf.applyCh <- ApplyMsg{
			CommandValid: true,
			Command:      entry.Command,
			CommandTerm:  entry.Term,
			CommandIndex: entry.Index,
		}

		rf.mu.Lock()
		DPrintf(PrintAlways, "[server-%d] [Apply] apply entry: %v, rf.CurrentTerm = %v", rf.me, entry, rf.CurrentTerm)
		// use entry.Index because rf.lastApplied may change during the Unlock() and Lock()
		rf.lastApplied = Max(rf.lastApplied, entry.Index)
		rf.mu.Unlock()
	}
}
