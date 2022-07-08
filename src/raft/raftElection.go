package raft

import (
	"math/rand"
	"sync/atomic"
	"time"
)

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	rand.Seed(time.Now().UnixNano())
	for !rf.killed() {

		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		sleepTime := time.Duration(ElectionInterval+rand.Int63n(ElectionInterval)) * time.Millisecond
		time.Sleep(sleepTime)

		rf.mu.Lock()
		// leader won't kick off election
		if rf.state == Leader {
			rf.mu.Unlock()
			continue
		}

		// check whether has receive heatbeat when ticker() in sleep
		noCommunicateTime := rf.lastCommunicate.Add(sleepTime)
		if noCommunicateTime.Before(time.Now()) {
			rf.convertToCandidate()
		}

		rf.mu.Unlock()
	}
}

// called by candidate
func (rf *Raft) kickOffElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// the number of servers who votes itself
	var votes int32 = 1

	args := RequestVoteArgs{
		Term:         rf.CurrentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastEntry().Index,
		LastLogTerm:  rf.getLastEntry().Term,
	}

	// send vote RPC to other servers
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		go rf.countVote(i, &args, &votes)
	}
}

func (rf *Raft) countVote(serverIndex int, args *RequestVoteArgs, votes *int32) {
	// Request vote RPC
	reply := RequestVoteReply{}
	if ok := rf.sendRequestVote(serverIndex, args, &reply); !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf(Lab2A, "[candidate-%d] get vote reply from [server-%d] where Term = %d, current state = %s, reply = %v", rf.me, serverIndex, args.Term, rf.state, reply)

	// there is higher term
	if reply.Term > rf.CurrentTerm {
		rf.convertToFollower(reply.Term)
		return
	}

	// receive previous vote
	if rf.state != Candidate || rf.CurrentTerm != args.Term {
		return
	}

	// increase vote
	if reply.VoteGranted {
		atomic.AddInt32(votes, 1)
	}

	// have enough votes
	if atomic.LoadInt32(votes) > int32(len(rf.peers)/2) {
		rf.convertToLeader()
	}
}

// send Heartbeat via AppendEntries RPC
func (rf *Raft) sendHeartbeatLoop() {
	for !rf.killed() {
		rf.mu.Lock()
		// check self still be a leader
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		// leader in the minority partition, should ends up
		if rf.checkInMinorityPartition() {
			DPrintf(Lab2C, "[leader-%d] has lost communicate with majority servers", rf.me)
			rf.convertToFollower(rf.CurrentTerm)
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()

		// send heartbeat to other servers
		go rf.reachAgreementRound(true)

		time.Sleep(time.Duration(HeartBeatInterval) * time.Millisecond)
	}
}

// leader will be awared that it not leader after 3 heartbeat interval
func (rf *Raft) checkInMinorityPartition() bool {
	lostConnectCount := 0
	for _, lastTime := range rf.followerReplyTime {
		if time.Since(lastTime) > time.Millisecond*3*time.Duration(HeartBeatInterval) {
			lostConnectCount++
		}
	}
	return lostConnectCount > len(rf.peers)/2+1
}
