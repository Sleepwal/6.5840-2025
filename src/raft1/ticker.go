package raft

import (
	"6.5840/raftapi"
	"math/rand"
	"time"
)

const (
	// HeartbeatInterval the leader sends heartbeats considerably more often than once per 150 milliseconds (e.g., once per 10 milliseconds).
	HeartbeatInterval = 20
)

//---------------------------------------RequestVotedTicker-------------------------------------------------

func (rf *Raft) ticker() {
	for rf.killed() == false {
		// Your code here (3A)
		rf.mu.Lock()

		// Check if a leader election should be started.
		// 不是Leader & 超时，开始新一轮选举
		if rf.state != Leader && rf.timeout.Before(time.Now()) {
			rf.transitionToCandidate()
			rf.DTestPrintf("Server%d Term:%d start election", rf.me, rf.currentTerm)

			rf.mu.Unlock()

			// issue RequestVote RPCs in parallel to each of other servers
			for i := range rf.peers {
				if i == rf.me {
					continue
				}
				go rf.issueRequestVote(i)
			}
		} else {
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350 milliseconds.
		ms := 50 + (rand.Int63() % 300)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

func (rf *Raft) issueRequestVote(server int) {
	rf.mu.Lock()

	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateId:  rf.me,
		LastLogIndex: rf.getLastLogIndex(),
		LastLogTerm:  rf.getLastLogTerm(),
	}
	rf.mu.Unlock() // 发送会等待，不加锁

	reply := RequestVoteReply{}
	ok := rf.sendRequestVote(server, &args, &reply)

	if !ok {
		//DPrintf("server[%d] sendRequestVote to server[%d] failed\n", rf.me, server)
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// 判断自身是否还是竞选者，且任期不冲突
	if rf.state != Candidate || reply.Term < rf.currentTerm {
		return
	}

	// if RPC request or response contains term T > currentTerm, set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.transitionToFollower(reply.Term)
		rf.persist()
		rf.resetTimeout()
		return
	}

	if reply.VoteGranted {
		rf.votedCnt++

		// if votes received from the majority of servers, convert to leader
		if rf.votedCnt > len(rf.peers)/2 {
			rf.transitionToLeader()

			rf.DTestPrintf("S%d T:%d win the election", rf.me, rf.currentTerm)
		}
	}
}

//---------------------------------------appendEntriesTicker-------------------------------------------------

func (rf *Raft) appendEntriesTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == Leader {
			// 发送heartbeat给所有节点
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				prevLogIndex := rf.getPrevLogIndex(i)
				args := AppendEntriesArgs{
					Term:         rf.currentTerm,
					LeaderId:     rf.me,
					PrevLogIndex: prevLogIndex,
					Entries:      make([]Entry, 0),
					LeaderCommit: rf.commitIndex,
				}

				isSendInstallSnapshot := false

				// 该follower比快照落后，发送快照
				if prevLogIndex < rf.lastIncludedIndex {
					isSendInstallSnapshot = true
					rf.DTestPrintf("Server%d Term:%d Send Snapshot to Server%d", rf.me, rf.currentTerm, i)
				} else if rf.getLastLogIndex() >= prevLogIndex+1 { // if last log index >= nextIndex for a follower
					// send AppendEntries RPC with log entries starting at nextIndex
					args.Entries = append(args.Entries, rf.log[rf.getRealIndex(prevLogIndex+1):]...)
					rf.DTestPrintf("Server%d Term:%d Send appendEntries to Server%d", rf.me, rf.currentTerm, i)
				} else {
					args.Entries = make([]Entry, 0)
					//rf.DTestPrintf("Server%d Term:%d Send heartbeat to Server%d", rf.me, rf.currentTerm, i)
				}
				//tester.Annotate("Server "+strconv.Itoa(rf.me), "Send heartbeat to "+strconv.Itoa(i),
				//	fmt.Sprintf("Server%d Term:%d", rf.me, rf.currentTerm))

				if isSendInstallSnapshot {
					go rf.doInstallSnapshot(i)
				} else {
					args.PrevLogTerm = rf.getRealTerm(prevLogIndex)
					go rf.doAppendEntries(i, args)
				}
			}

		}

		rf.mu.Unlock()
		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) doAppendEntries(server int, args AppendEntriesArgs) {
	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader {
		return
	}

	if reply.Success { // if successful
		// update nextIndex and matchIndex for follower (§5.3)
		rf.matchIndex[server] = max(args.PrevLogIndex+len(args.Entries), rf.matchIndex[server])
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// if there exists an N such that N > commitIndex,
		// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm,
		// set commitIndex = N (§5.3, §5.4)
		for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
			cnt := 1
			if rf.log[rf.getRealIndex(N)].Term != rf.currentTerm {
				continue
			}

			for i, match := range rf.matchIndex {
				if i == rf.me {
					continue
				}

				if match >= N {
					cnt++
				}
			}

			// 从大往小遍历，匹配就直接break
			if cnt > len(rf.peers)/2 {
				rf.commitIndex = N
				break
			}
		}

		rf.condApply.Signal()
		return
	}

	// reply false
	// 退化的情况会 reply false
	// if RPC request or response contains term T > currentTerm, set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.transitionToFollower(reply.Term)
		rf.persist()
		rf.resetTimeout()
		return
	}

	// if AppendEntries fails because of log inconsistency
	// 小于 的情况不用管，接受者会退化成Follower，就剩 等于 的情况
	// 前面可能会退化成Follower，判断是否Leader
	if reply.Term == rf.currentTerm && rf.state == Leader {
		//rf.nextIndex[server]-- // decrease nextIndex and retry (§5.3)

		// no conflict
		// Case 3: follower's log is too short, nextIndex = XLen
		if reply.XTerm == -1 {
			// 已被snapshot截断，follower落后了
			if rf.lastIncludedIndex >= reply.XLen {
				rf.DTestPrintf("Case3 %v >= %v Server%d send InstallSnapshot to Server%d", rf.lastIncludedIndex, reply.XLen, rf.me, server)
				//go rf.doInstallSnapshot(server)
				// 下一次心跳添加InstallSnapshot的处理
				rf.nextIndex[server] = rf.lastIncludedIndex
			} else {
				rf.nextIndex[server] = reply.XLen
				rf.DTestPrintf("Case3 %v < %v Leader%d update nextIndex %v", rf.lastIncludedIndex, reply.XLen, rf.me, rf.nextIndex[server])
			}
			return
		}

		// has conflict
		i := rf.nextIndex[server] - 1
		if i < rf.lastIncludedIndex {
			i = rf.lastIncludedIndex
		}
		for i > rf.lastIncludedIndex && rf.log[rf.getRealIndex(i)].Term > reply.XTerm {
			i--
		}

		// 已被snapshot截断
		if i == rf.lastIncludedIndex && rf.log[rf.getRealIndex(i)].Term > reply.XTerm {
			//go rf.doInstallSnapshot(server)
			// 下一次心跳添加InstallSnapshot的处理
			rf.nextIndex[server] = rf.lastIncludedIndex
		} else if rf.log[rf.getRealIndex(i)].Term != reply.XTerm {
			if reply.XIndex <= rf.lastIncludedIndex { // 已被snapshot截断
				//go rf.doInstallSnapshot(server)
				rf.nextIndex[server] = rf.lastIncludedIndex
				rf.DTestPrintf("Case1 Server%d send InstallSnapshot to Server%d", rf.me, server)
			} else { // Case 1: leader doesn't have XTerm, nextIndex = XIndex
				rf.nextIndex[server] = reply.XIndex
				rf.DTestPrintf("Case1 Server%d doesn't have XTerm, nextIndex = %v", rf.me, reply.XIndex)
			}
		} else if rf.log[rf.getRealIndex(i)].Term == reply.XTerm {
			// Case 2: leader has XTerm, nextIndex = (index of leader's last entry for XTerm) + 1
			rf.nextIndex[server] = i + 1 // i + 1是确保没有被截断的
			rf.DTestPrintf("Case2 Server%d has XTerm %d", rf.me, reply.Term)
		}

		return
	}
}

//---------------------------------------applyTicker-------------------------------------------------

func (rf *Raft) applyTicker() {
	for !rf.killed() {
		rf.mu.Lock()

		for rf.commitIndex <= rf.lastApplied {
			rf.condApply.Wait()
		}

		msgBuffer := make([]raftapi.ApplyMsg, 0, rf.commitIndex-rf.lastApplied)
		tmpApplied := rf.lastApplied

		for rf.commitIndex > tmpApplied {
			tmpApplied++
			// 该日志已被截断
			if tmpApplied <= rf.lastIncludedIndex {
				continue
			}

			//DPrintf("tmeApplied = %v, commitIndex = %v, log = %v, lastIncludedIndex=%v", tmpApplied, rf.commitIndex, rf.log, rf.lastIncludedIndex)
			msg := raftapi.ApplyMsg{
				CommandValid: true,
				Command:      rf.log[rf.getRealIndex(tmpApplied)].Command,
				CommandIndex: tmpApplied,
				SnapshotTerm: rf.log[rf.getRealIndex(tmpApplied)].Term,
			}

			msgBuffer = append(msgBuffer, msg)
			//tester.Annotate("Server "+strconv.Itoa(rf.me),
			//	fmt.Sprintf("Server%d Term:%d apply", rf.me, rf.currentTerm),
			//	fmt.Sprintf("log:%v", rf.log))
		}
		rf.mu.Unlock()

		// 在解锁后可能又出现了SnapShot进而修改了rf.lastApplied
		for _, msg := range msgBuffer {
			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}

			rf.DTestPrintf("server %v commit %v, lastIncludedIndex=%v", rf.me, msg.CommandIndex, rf.lastIncludedIndex)
			rf.mu.Unlock()

			rf.applyCh <- msg

			rf.mu.Lock()
			if msg.CommandIndex != rf.lastApplied+1 {
				rf.mu.Unlock()
				continue
			}
			rf.lastApplied = msg.CommandIndex
			rf.mu.Unlock()
		}

		// pause for a random amount of time between 50 and 350 milliseconds.
		//ms := 50 + (rand.Int63() % 100)
		//time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

//---------------------------------------installSnapshot-------------------------------------------------

func (rf *Raft) doInstallSnapshot(server int) {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}

	args := InstallSnapshotArgs{
		Term:              rf.currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		Data:              rf.snapshot,
	}
	rf.mu.Unlock()

	reply := InstallSnapshotReply{}
	ok := rf.sendInstallSnapshot(server, &args, &reply)
	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.state != Leader || rf.currentTerm != args.Term {
		return
	}

	if reply.Term > rf.currentTerm {
		rf.transitionToFollower(reply.Term)
		rf.resetTimeout()
		rf.persist()
		return
	}

	// LastIncludedIndex可能包括了还没有复制的日志项, 这些日志项可以不用复制了
	if rf.matchIndex[server] < args.LastIncludedIndex {
		rf.matchIndex[server] = args.LastIncludedIndex
	}
	// 更新nextIndex
	rf.nextIndex[server] = rf.lastIncludedIndex + 1
}
