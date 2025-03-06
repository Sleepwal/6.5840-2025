package raft

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
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isUpdateToDate(args.LastLogTerm, args.LastLogIndex) {

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

	// if RPC request or response contains term T > currentTerm, set currentTerm = T, convert to follower (§5.1)
	//if args.Term > rf.currentTerm {
	//	rf.currentTerm = args.Term
	//}

	rf.transitionToFollower(args.Term)
	rf.resetTimeout()
	reply.Term, reply.Success = args.Term, true

	//tester.Annotate("Server "+strconv.Itoa(rf.me),
	//	fmt.Sprintf("Server%d Term:%d receive heartbeat", rf.me, rf.currentTerm),
	//	fmt.Sprintf("log:%v", rf.log))

	// Reply false if log doesn't contain an entry at prevLogIndex
	// whose term matches prevLogTerm (§5.3)
	if args.PrevLogIndex > len(rf.log)-1 || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		reply.Term, reply.Success = rf.currentTerm, false
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)

	if len(args.Entries) != 0 {
		// Append any new entries not already in the log
		rf.log = append(rf.log[:args.PrevLogIndex+1], args.Entries...)

		//tester.Annotate("Server "+strconv.Itoa(rf.me),
		//	fmt.Sprintf("Server%d Term:%d append entries log:%v", rf.me, rf.currentTerm, rf.log),
		//	fmt.Sprintf("log:%v", rf.log))
	}

	// If LeaderCommit > commitIndex,
	// set commitIndex = min(LeaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, len(rf.log)-1)
	}
}
