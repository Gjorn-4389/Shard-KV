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
	//	"bytes"

	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

	applyCh chan ApplyMsg

	// server state: leader candidate follower
	state           ServerState
	lastCommunicate time.Time

	// persistent state on all servers
	CurrentTerm int
	VotedFor    int
	Log         []LogEntry

	// volatile state on all servers
	commitIndex int
	lastApplied int
	applyCond   sync.Cond

	// volatile state on leader
	nextIndex         []int
	matchIndex        []int
	followerReplyTime []time.Time
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

	// avoid convertFollower persist
	rf.state = Follower
	rf.CurrentTerm = 0
	rf.VotedFor = -1
	rf.Log = make([]LogEntry, 1)
	rf.updateCommunicateTimeToNow()

	rf.applyCh = applyCh
	rf.applyCond = *sync.NewCond(&rf.mu)

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// apply message loop
	go rf.ApplyMessage()

	// start ticker goroutine to start elections
	go rf.ticker()
	return rf
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term, isleader = rf.CurrentTerm, rf.state == Leader
	return term, isleader
}

func (rf *Raft) HasLogInCurrentTerm() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.getLastEntry().Term == rf.CurrentTerm
}

// add log entry to rf.log
func (rf *Raft) addEntry(entry LogEntry) {
	rf.Log = append(rf.Log, entry)
	// update log need persist
	rf.persist()
	go rf.reachAgreementRound(false)
}

// add log entries to rf.log
func (rf *Raft) addEntries(prevIndex int, entries []LogEntry) {
	// delete entries after pervIndex
	rf.Log = append([]LogEntry{}, rf.Log[:prevIndex+1-rf.getFirstEntry().Index]...)
	rf.Log = append(rf.Log, entries...)
	// update log need persist
	rf.persist()
	go rf.reachAgreementRound(false)
}

// happen in RPC handler
func (rf *Raft) updateCommunicateTimeToNow() {
	rf.lastCommunicate = time.Now()
}

func (rf *Raft) setCommitIndex(newCommitIndex int) {
	// new commitIndex = min(index of last log entry,  the median of match index)
	if rf.state == Leader {
		if rf.checkIndexValid(newCommitIndex) && rf.getEntry(newCommitIndex).Term == rf.CurrentTerm {
			rf.commitIndex = Min(rf.getLastEntry().Index, newCommitIndex)
			rf.applyCond.Signal()
		}
	} else {
		rf.commitIndex = Min(rf.getLastEntry().Index, newCommitIndex)
		rf.applyCond.Signal()
	}
}

func (rf *Raft) setNextIndex(serverIndex int, newNextIndex int) {
	// nextIndex <= lastEntryIndex + 1
	rf.nextIndex[serverIndex] = Min(newNextIndex, rf.getLastEntry().Index+1)
}

func (rf *Raft) setMatchIndex(serverIndex int, newMatchIndex int) {
	rf.matchIndex[serverIndex] = newMatchIndex
	rf.checkCommitIndex()
}

func (rf *Raft) convertToFollower(newTerm int) {
	DPrintf(Lab2A, "[server-%d] becomes follower at term = %d", rf.me, newTerm)
	rf.state = Follower
	rf.CurrentTerm = newTerm
	rf.VotedFor = -1
	rf.persist()
}

func (rf *Raft) convertToCandidate() {
	rf.state = Candidate
	rf.CurrentTerm++
	rf.VotedFor = rf.me
	rf.persist()
	DPrintf(Lab2A, "[server-%d] becomes candidate at term = %d", rf.me, rf.CurrentTerm)
	go rf.kickOffElection()
}

func (rf *Raft) convertToLeader() {
	rf.state = Leader
	rf.updateCommunicateTimeToNow()

	// initialize after election win
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	rf.followerReplyTime = make([]time.Time, len(rf.peers))

	newLogEntryIndex := rf.getLastEntry().Index + 1
	snapshotIndex := rf.getFirstEntry().Index
	for i := 0; i < len(rf.peers); i++ {
		rf.nextIndex[i] = newLogEntryIndex
		rf.matchIndex[i] = snapshotIndex
		rf.followerReplyTime[i] = time.Now()
	}

	DPrintf(PrintAlways, "[server-%d] becomes leader at term = %d\n\t\t\tcurrent log ====> %v", rf.me, rf.CurrentTerm, rf.Log)
	// send heartbeat periodly
	go rf.sendHeartbeatLoop()
}

func (rf *Raft) getFirstEntry() LogEntry {
	return rf.Log[0]
}

func (rf *Raft) getLastEntry() LogEntry {
	return rf.Log[len(rf.Log)-1]
}

func (rf *Raft) checkIndexValid(idx int) bool {
	return idx >= rf.getFirstEntry().Index && idx <= rf.getLastEntry().Index
}

func (rf *Raft) getEntry(idx int) LogEntry {
	return rf.Log[idx-rf.getFirstEntry().Index]
}

func (rf *Raft) GetLogLength() int {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return len(rf.Log)
}

func (rf *Raft) GetLogs() []LogEntry {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	return rf.Log
}

func (rf *Raft) PrintRatSize() {
	for !rf.killed() {
		DPrintf(PrintAlways, "[server-%d] [RaftSize] %d", rf.me, rf.persister.RaftStateSize())
		time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
	}
}
