package raft

import (
	"bytes"

	"6.824/labgob"
)

func (rf *Raft) persistData() []byte {
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.CurrentTerm)
	e.Encode(rf.VotedFor)
	e.Encode(rf.Log)
	return w.Bytes()
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	rf.persister.SaveRaftState(rf.persistData())
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}

	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)

	var currentTerm int
	var votedFor int
	var log []LogEntry

	if d.Decode(&currentTerm) != nil || d.Decode(&votedFor) != nil || d.Decode(&log) != nil {
		return
	}

	rf.mu.Lock()
	rf.CurrentTerm, rf.VotedFor, rf.Log = currentTerm, votedFor, log
	rf.commitIndex = Max(rf.commitIndex, rf.getFirstEntry().Index)
	rf.lastApplied = Max(rf.lastApplied, rf.getFirstEntry().Index)
	rf.mu.Unlock()

	DPrintf(PrintAlways, "[server-%d] after read persist, currentTerm = %d, votedFor = %d, log = %v", rf.me, rf.CurrentTerm, rf.VotedFor, rf.Log)
}
