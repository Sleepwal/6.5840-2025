package raft

import (
	tester "6.5840/tester1"
	"fmt"
	"strconv"
)

// RequestVote example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply false if term < currentTerm (§5.2)
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		//DPrintf("server%d term%d > server%d term%d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	}

	// if RPC request or response contains term T > currentTerm, set currentTerm = T, convert to follower (§5.1)
	if args.Term > rf.currentTerm {
		rf.transitionToFollower(args.Term)
		// 往下投票
	}

	// if votedFor is null or candidateId, and candidate’s log is at least as up-to-date as receiver’s log, grant vote (§5.2, §5.4)
	if rf.votedFor == -1 || rf.votedFor == args.CandidateId {
		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.resetTimeout() // 投了票才需要reset

		reply.VoteGranted = true
		reply.Term = args.Term
		DPrintf("Server%d Term:%d vote for Server%d Term:%d", rf.me, rf.currentTerm, args.CandidateId, args.Term)
		return
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm
		DPrintf("Server%d Term:%d don't vote for Server%d Term:%d", rf.me, rf.currentTerm, rf.votedFor, rf.currentTerm)
	}
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	// reply false if term < currentTerm (§5.1)
	if args.Term < rf.currentTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	tester.Annotate("Server "+strconv.Itoa(rf.me), strconv.Itoa(rf.me)+" receive heartbeat",
		fmt.Sprintf("Server%d Term:%d", rf.me, rf.currentTerm))
	rf.transitionToFollower(args.Term)
	rf.resetTimeout()
	reply.Term, reply.Success = args.Term, true
}
