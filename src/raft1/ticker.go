package raft

import (
	tester "6.5840/tester1"
	"fmt"
	"math/rand"
	"strconv"
	"time"
)

const (
	// HeartbeatInterval the leader sends heartbeats considerably more often than once per 150 milliseconds (e.g., once per 10 milliseconds).
	HeartbeatInterval = 15
)

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

	// if RPC request or response contains term T > currentTerm, set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.transitionToFollower(reply.Term)
		rf.resetTimeout()
		return
	}

	if rf.state == Candidate && reply.VoteGranted {
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

func (rf *Raft) appendEntriesTicker() {
	for rf.killed() == false {
		if rf.state == Leader {
			// 发送heartbeat给所有节点
			for i := range rf.peers {
				if i == rf.me {
					continue
				}

				go rf.doAppendEntries(i)
			}
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
	args := AppendEntriesArgs{
		Term:         rf.currentTerm,
		LeaderId:     rf.me,
		PrevLogIndex: 0,
		PrevLogTerm:  0,
		Entries:      nil,
		LeaderCommit: rf.commitIndex,
	}
	rf.mu.Unlock()

	reply := AppendEntriesReply{}
	ok := rf.sendAppendEntries(server, &args, &reply)

	if !ok {
		return
	}

	rf.mu.Lock()
	defer rf.mu.Unlock()

	// if RPC request or response contains term T > currentTerm, set currentTerm = T, convert to follower (§5.1)
	if reply.Term > rf.currentTerm {
		rf.transitionToFollower(reply.Term)
		rf.resetTimeout()
		return
	}
}
