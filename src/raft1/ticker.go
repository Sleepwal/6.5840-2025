package raft

import (
	"6.5840/raftapi"
	tester "6.5840/tester1"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

const (
	// HeartbeatInterval the leader sends heartbeats considerably more often than once per 150 milliseconds (e.g., once per 10 milliseconds).
	HeartbeatInterval = 35
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

			tester.Annotate("Server "+strconv.Itoa(rf.me), "start election",
				fmt.Sprintf("Server%d Term:%d", rf.me, rf.currentTerm))
			//DPrintf("Server %d start election, term %d", rf.me, rf.currentTerm)

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
	if rf.state != Candidate || args.Term < rf.currentTerm {
		return
	}

	// if RPC request or response contains term T > currentTerm, set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.transitionToFollower(reply.Term)
		rf.resetTimeout()
		return
	}

	if reply.VoteGranted {
		rf.votedCnt++

		// if votes received from the majority of servers, convert to leader
		if rf.votedCnt > len(rf.peers)/2 {
			rf.transitionToLeader()

			//DPrintf("Server%d Term:%d win the election", rf.me, rf.currentTerm)
			tester.Annotate("Server "+strconv.Itoa(rf.me), " win the election",
				fmt.Sprintf("Server%d Term:%d", rf.me, rf.currentTerm))
		}
	}
}

//---------------------------------------appendEntriesTicker-------------------------------------------------

func (rf *Raft) appendEntriesTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		if rf.state == Leader {
			rf.mu.Unlock()
			// 发送heartbeat给所有节点
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				go rf.doAppendEntries(i)
			}
		} else {
			rf.mu.Unlock()
		}

		time.Sleep(time.Duration(HeartbeatInterval) * time.Millisecond)
	}
}

func (rf *Raft) doAppendEntries(server int) {
	rf.mu.Lock()

	if rf.state != Leader {
		rf.mu.Unlock()
		return
	}
	//tester.Annotate("Server "+strconv.Itoa(rf.me), "Send heartbeat to "+strconv.Itoa(server),
	//	fmt.Sprintf("Server%d Term:%d", rf.me, rf.currentTerm))
	prevLogIndex, prevLogTerm := rf.getPrevLogIndexAndTerm(server)
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: prevLogIndex,
		PrevLogTerm:  prevLogTerm,
		Entries:      make([]Entry, 0),
		LeaderCommit: rf.commitIndex,
	}

	// if last log index >= nextIndex for a follower
	if rf.getLastLogIndex() >= prevLogIndex+1 {
		// send AppendEntries RPC with log entries starting at nextIndex
		args.Entries = append(args.Entries, rf.log[prevLogIndex+1:]...)
	}
	rf.mu.Unlock()

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

	// 函数调用间隙值变了, 已经不是发起这个调用时的term了
	// 要先判断term是否改变, 否则后续的更改matchIndex等是不安全的
	//if args.Term != rf.currentTerm {
	//	return
	//}

	if reply.Success { // if successful
		// update nextIndex and matchIndex for follower (§5.3)
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Entries)
		rf.nextIndex[server] = rf.matchIndex[server] + 1

		// if there exists an N such that N > commitIndex,
		// a majority of matchIndex[i] ≥ N, and log[N].term == currentTerm,
		// set commitIndex = N (§5.3, §5.4)
		for N := rf.getLastLogIndex(); N > rf.commitIndex; N-- {
			cnt := 1
			if rf.log[N].Term != rf.currentTerm {
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
		return
	}

	// 先更新commitIndex再退化
	// if RPC request or response contains term T > currentTerm, set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.transitionToFollower(reply.Term)
		rf.resetTimeout()
		return
	}

	// if AppendEntries fails because of log inconsistency
	// 小于 的情况不用管，发送者会退化成Follower，就剩 等于 的情况
	// 前面可能会退化成Follower，判断是否Leader
	if reply.Term == rf.currentTerm && rf.state == Leader {
		rf.nextIndex[server]-- // decrease nextIndex and retry (§5.3)

		//tester.Annotate("Server "+strconv.Itoa(rf.me),
		//	fmt.Sprintf("Server%d Term:%d don't match matchIndex:%d", rf.me, rf.currentTerm, rf.nextIndex[server]),
		//	fmt.Sprintf("log:%v", rf.log))
		return
	}
}

//---------------------------------------applyTicker-------------------------------------------------

func (rf *Raft) applyTicker() {
	for rf.killed() == false {
		rf.mu.Lock()
		for rf.commitIndex > rf.lastApplied {
			rf.lastApplied++

			msg := raftapi.ApplyMsg{
				CommandValid:  true,
				Command:       rf.log[rf.lastApplied].Command,
				CommandIndex:  rf.lastApplied,
				SnapshotValid: false,
				Snapshot:      nil,
				SnapshotTerm:  0,
				SnapshotIndex: 0,
			}
			rf.applyCh <- msg

			//tester.Annotate("Server "+strconv.Itoa(rf.me),
			//	fmt.Sprintf("Server%d Term:%d apply", rf.me, rf.currentTerm),
			//	fmt.Sprintf("log:%v", rf.log))
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350 milliseconds.
		ms := 50 + (rand.Int63() % 100)
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}
