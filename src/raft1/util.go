package raft

import (
	"log"
	"math/rand"
	"time"
)

// Debugging
const Debug = false

//const Debug = true

func DPrintf(format string, a ...interface{}) {
	if Debug {
		log.Printf(format, a...)
	}
}

const (
	MaxTimeout = 350
	MinTimeout = 150
)

func (rf *Raft) resetTimeout() {
	// The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds.
	rf.timeout = time.Now().Add(time.Duration(rand.Intn(MaxTimeout-MinTimeout)+MinTimeout) * time.Millisecond)
}

func (rf *Raft) transitionToFollower(term int) {
	rf.state = Follower
	rf.currentTerm = term
	rf.votedFor = -1
	rf.votedCnt = 0
}

func (rf *Raft) transitionToCandidate() {
	rf.currentTerm++     // increment currentTerm
	rf.state = Candidate // transition to candidate
	rf.votedFor = rf.me  // vote for self
	rf.votedCnt = 1
	rf.resetTimeout()
}

func (rf *Raft) transitionToLeader() {
	rf.state = Leader
	rf.votedFor = -1
	rf.votedCnt = 0

	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
	}
	rf.matchIndex = make([]int, len(rf.peers))
	rf.matchIndex[rf.me] = rf.getLastLogIndex()
}

func (rf *Raft) getLastLogIndex() int {
	return len(rf.log) - 1
}

func (rf *Raft) getLastLogTerm() int {
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getPrevLogIndexAndTerm(server int) (int, int) {
	return rf.nextIndex[server] - 1, rf.log[rf.nextIndex[server]-1].Term
}

func (rf *Raft) isUpdateToDate(lastLogTerm, lastLogIndex int) bool {
	return lastLogTerm > rf.getLastLogTerm() ||
		(lastLogTerm == rf.getLastLogTerm() && lastLogIndex >= rf.getLastLogIndex())
}
