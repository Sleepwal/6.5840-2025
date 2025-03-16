package raft

import (
	"6.5840/raftapi"
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
		rf.persist()
		// 往下投票
	}

	// if votedFor is null or candidateId, and candidate’s log is at least as up-to-Data as receiver’s log, grant vote (§5.2, §5.4)
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) &&
		rf.isUpdateToDate(args.LastLogTerm, args.LastLogIndex) {

		rf.votedFor = args.CandidateId
		rf.currentTerm = args.Term
		rf.persist()

		rf.resetTimeout() // 投了票才需要reset

		reply.VoteGranted = true
		reply.Term = args.Term

		rf.DTestPrintf("+++++++Server%d Term:%d vote for Server%d", rf.me, rf.currentTerm, args.CandidateId)
		return
	} else {
		reply.VoteGranted = false
		reply.Term = rf.currentTerm

		rf.DTestPrintf("----------Server%d Term:%d don't vote for Server%d", rf.me, rf.currentTerm, args.CandidateId)
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
	if args.Term > rf.currentTerm {
		rf.transitionToFollower(args.Term)
		rf.persist()
	}

	rf.resetTimeout()

	//tester.Annotate("Server "+strconv.Itoa(rf.me),
	//	fmt.Sprintf("Server%d Term:%d receive heartbeat", rf.me, rf.currentTerm),
	//	fmt.Sprintf("log:%v", rf.log))

	isConflict := false

	// 过时的RPC
	if args.PrevLogIndex < rf.lastIncludedIndex {
		reply.Success = true
		reply.Term = rf.currentTerm
		return

		// Case 3: follower's log is too short
	} else if args.PrevLogIndex > rf.getLastLogIndex() {
		reply.XLen = rf.getLastLogIndex()
		reply.XTerm = -1
		isConflict = true
		rf.DTestPrintf("Case3: %v > %v too short server %v receive AE from server%v, XLen:%v", args.PrevLogIndex, rf.getLastLogIndex(), rf.me, args.LeaderId, reply.XLen)

		// Reply false if log doesn't contain an entry at prevLogIndex
		// whose term matches prevLogTerm (§5.3)
	} else if rf.log[rf.getRealIndex(args.PrevLogIndex)].Term != args.PrevLogTerm {
		//  Case 1 2: has conflict,
		// XTerm is the conflicting term
		reply.XTerm = rf.log[rf.getRealIndex(args.PrevLogIndex)].Term

		i := args.PrevLogIndex
		for i > rf.commitIndex && rf.log[rf.getRealIndex(i)].Term == reply.XTerm {
			i--
		}
		// XIndex is the first index with that term
		reply.XIndex = i + 1
		reply.XLen = rf.getLastLogIndex()
		isConflict = true
		rf.DTestPrintf("Case1、2: server %v 的log在PrevLogIndex: %v 位置不存在日志项, XTerm:%v XIndex:%v XLen:%v", rf.me, args.PrevLogIndex, reply.XTerm, reply.XIndex, reply.XLen)
	}

	if isConflict {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	// If an existing entry conflicts with a new one (same index but different terms),
	// delete the existing entry and all that follow it (§5.3)

	if len(args.Entries) != 0 {
		// Append any new entries not already in the log
		rf.log = append(rf.log[:rf.getRealIndex(args.PrevLogIndex+1)], args.Entries...)
		rf.persist()

		rf.DTestPrintf("Server%d Term:%d append entries", rf.me, rf.currentTerm)
		//tester.Annotate("Server "+strconv.Itoa(rf.me),
		//	fmt.Sprintf("Server%d Term:%d append entries log:%v", rf.me, rf.currentTerm, rf.log),
		//	fmt.Sprintf("log:%v", rf.log))
	}

	reply.Success = true
	reply.Term = rf.currentTerm

	// If LeaderCommit > commitIndex,
	// set commitIndex = min(LeaderCommit, index of last new entry)
	if args.LeaderCommit > rf.commitIndex {
		rf.commitIndex = min(args.LeaderCommit, rf.getLastLogIndex())
		rf.condApply.Signal()
	}
}

func (rf *Raft) InstallSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	DPrintf("Server%d receive InstallSnapshot from Server%d", rf.me, args.LeaderId)

	// 1. reply immediately if term < currentTerm
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		return
	}

	if args.Term > rf.currentTerm {
		rf.transitionToFollower(args.Term)
	}

	rf.state = Follower
	rf.resetTimeout()

	if args.LastIncludedIndex < rf.lastIncludedIndex || args.LastIncludedIndex < rf.commitIndex {
		// 1. 快照反而比当前的 lastIncludedIndex 更旧, 不需要快照
		// 2. 快照比当前的 commitIndex 更旧, 不能安装快照
		reply.Term = rf.currentTerm
		return
	}

	// 2. create new snapshot file if first chunk(offset is 0)
	// 3. write Data into snapshot file at given offset
	// 4. reply and wait for more Data chunks if Done is false
	// 5. save snapshot file, discard any existing or partial snapshot with a smaller index

	// 6. if existing log entry has same index and term as snapshot’s last included entry,
	// retain log entries following it and reply
	hasEntry := false
	readIndex := 0
	for ; readIndex < len(rf.log); readIndex++ {
		if rf.getVirtualIndex(readIndex) == args.LastIncludedIndex &&
			rf.log[readIndex].Term == args.LastIncludedTerm {
			hasEntry = true
			break
		}
	}

	msg := raftapi.ApplyMsg{
		SnapshotValid: true,
		Snapshot:      args.Data,
		SnapshotTerm:  args.LastIncludedTerm,
		SnapshotIndex: args.LastIncludedIndex,
	}

	if hasEntry {
		// 索引从0开始，保留LastIncludedIndex位置的entry
		rf.log = rf.log[readIndex:]
		//rf.DTestPrintf("retain log entries following lastIncludedIndex: %v", rf.log)
	} else {
		// 7. discard the entire log
		rf.log = make([]Entry, 0)
		rf.log = append(rf.log, Entry{Term: args.LastIncludedTerm})
		//rf.DTestPrintf("discard the entire log")
	}

	// 8. reset state machine using snapshot contents
	// (and load snapshot's cluster configuration)
	rf.snapshot = args.Data
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm

	if rf.commitIndex < args.LastIncludedIndex {
		rf.commitIndex = args.LastIncludedIndex
	}

	if rf.lastApplied < args.LastIncludedIndex {
		rf.lastApplied = args.LastIncludedIndex
	}

	reply.Term = rf.currentTerm
	rf.applyCh <- msg
	rf.persist()
}
