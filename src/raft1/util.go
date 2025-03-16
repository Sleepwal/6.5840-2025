package raft

import (
	tester "6.5840/tester1"
	"fmt"
	"log"
	"math/rand"
	"strconv"
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

// const DebugTest = false
const DebugTest = true

func (rf *Raft) DTestPrintf(format string, a ...interface{}) {
	if DebugTest {
		tester.Annotate("Server "+strconv.Itoa(rf.me),
			fmt.Sprintf(format, a...),
			fmt.Sprintf("lastIncludedIndex:%v lastIncludedTerm:%v commtIndex:%v lastApplied:%v log:%v, ", rf.lastIncludedIndex, rf.currentTerm, rf.commitIndex, rf.lastApplied, rf.log))
	}

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

	rf.persist()
}

func (rf *Raft) transitionToLeader() {
	rf.state = Leader
	rf.votedFor = -1
	rf.votedCnt = 0
	rf.persist()

	for i := range rf.peers {
		rf.nextIndex[i] = rf.getLastLogIndex() + 1
		rf.matchIndex[i] = rf.lastIncludedIndex
	}
}

func (rf *Raft) getLastLogIndex() int {
	return rf.getVirtualIndex(len(rf.log) - 1)
}

func (rf *Raft) getLastLogTerm() int {
	if len(rf.log)-1 == 0 {
		return rf.lastIncludedTerm
	}
	return rf.log[len(rf.log)-1].Term
}

func (rf *Raft) getPrevLogIndex(server int) int {
	index := rf.nextIndex[server] - 1
	//if index == rf.lastIncludedIndex+1 {
	//	index = rf.getLastLogIndex()
	//}
	return index
}

func (rf *Raft) isUpdateToDate(lastLogTerm, lastLogIndex int) bool {
	return lastLogTerm > rf.getLastLogTerm() ||
		(lastLogTerm == rf.getLastLogTerm() && lastLogIndex >= rf.getLastLogIndex())
}

func (rf *Raft) getRealIndex(virtualIndex int) int {
	return virtualIndex - rf.lastIncludedIndex
}

func (rf *Raft) getRealTerm(virtualIndex int) int {
	// 与快照一致
	if virtualIndex <= rf.lastIncludedIndex {
		return rf.lastIncludedTerm
	}

	return rf.log[rf.getRealIndex(virtualIndex)].Term
}

func (rf *Raft) getVirtualIndex(realIndex int) int {
	return realIndex + rf.lastIncludedIndex
}
