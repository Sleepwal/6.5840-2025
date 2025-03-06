## Lab2 KV server

Each client interacts with the key/value server **using a Clerk**, 
which sends RPCs to the server. Clients can send two different RPCs to the server: 
- **Put(key, value, version)** 
- **Get(key)**

The server maintains **an in-memory map** that records for **each key a (value, version) tuple**. 

- Keys and values are **strings**. 
- The version number records **the number of times the key has been written**. 

**Put(key, value, version)** installs or replaces the value for a particular key in the map 
only if the **Put's version number matches the server's version number** for the key. 

- If the version numbers **match**, the server also **increments the version number of the key**. 
- If the version numbers **don't match**, the server should return **rpc.ErrVersion**. 

A client can create a new key by invoking **Put with version number 0** (and the resulting version stored by the server will be 1).
- If the version number of the Put is larger than 0 and the key doesn't exist, the server should return rpc.ErrNoKey.

Get(key) fetches the current value for the key and its associated version. 
- If the key doesn't exist at the server, the server should return rpc.ErrNoKey.

Maintaining a **version number** for each key will be useful for 
implementing locks using Put and ensuring at-most-once semantics for Put's 
when the **network is unreliable** and the **client retransmits**.

When you've finished this lab and passed all the tests, 
you'll have a linearizable key/value service from the point of view of clients calling Clerk.Get and Clerk.Put. 

That is, if client operations aren't concurrent, 
each client Clerk.Get and Clerk.Put will observe the modifications to the state 
implied by the preceding sequence of operations. 
For concurrent operations, 
the return values and final state will be the same as if the operations had executed one at a time in some order. 

Operations are concurrent if they overlap in time: for example, 
if client X calls Clerk.Put(), and client Y calls Clerk.Put(), and then client X's call returns. 

An operation must observe the effects of all operations that have completed before the operation starts. 
See the FAQ on linearizability for more background.

### Get Started

`kvsrv1/client.go` implements a Clerk that 
clients use to manage RPC interactions with the server; 
the Clerk provides Put and Get methods.


`kvsrv1/server.go` contains the server code, 
including the Put and Get handlers that implement the server side of RPC requests.

### Task

Task1: No dropped messages.
- Clerk Put/Get methods in `client.go`
- Put and Get RPC handlers in `server.go`.

```shell
go test -v -run Reliable
```

Task2: implement a lock layered on client Clerk.Put and Clerk.Get calls.

The lock supports two methods: Acquire and Release. 
- The lock's specification is that only one client can successfully acquire the lock at a time; 
- other clients must wait until the first client has released the lock using `Release()`. 
- Your Acquire and Release code can talk to your key/value server by calling `lk.ck.Put()` and `lk.ck.Get()`.


Task3: Dropped RPC requests and replies(modify your `kvsrv1/client.go`)
- A return value of true from the client's `ck.clnt.Call()` indicates that the client received an RPC reply from the server; 
- a return value of false indicates that it did not receive a reply
- Your Clerk should keep re-sending an RPC until it receives a reply.
- if a Clerk receives rpc.ErrVersion for a retransmitted Put RPC, Clerk.Put must return `rpc.ErrMaybe` to the application.
- Your solution shouldn't require any changes to the server.

```shell
 go test -v
```

Task4: Implementing a lock using key/value clerk and unreliable network


## Lab3 Raft

### Introduction

This is the first in a series of labs in which you'll build a fault-tolerant key/value storage system. 

In this lab you'll implement Raft, a replicated state machine protocol. 
In the next lab you'll build a key/value service on top of Raft. 
Then you will “shard” your service over multiple replicated state machines for higher performance.

You should follow the design in the extended Raft paper, with particular attention to Figure 2. 
You'll implement most of what's in the paper, 
including saving persistent state and reading it after a node fails and then restarts. 
You will not implement cluster membership changes (Section 6).

###  Part 3A: Leader Election(moderate)
Feature:
- Raft leader election
  - `RequestVote()` RPCs
  - `AppendEntries()` RPCs with no log entries(heartbeats).
- Handler
  - `RequestVote()` RPC handler.
  - `AppendEntries()` RPC handler.

Goal:
- for a single leader to be elected.
- for the leader to remain the leader if there are no failures. 
- for a new leader to take over 
  - if the old leader fails 
  - if packets to/from the old leader are lost. 

Hint:
- Follow the paper's Figure 2. 
- At this point you care about:
  - sending and receiving RequestVote RPCs.
  - the **Rules** for Servers that relate to elections. 
  - and the **State** related to leader election.
- Add the Figure 2 state for leader election to the Raft struct in `raft.go`. 
- Define a struct to hold information about each **log entry**.
- Fill in the RequestVoteArgs and RequestVoteReply structs. 
- Modify Make()
  - create a background goroutine
    - kick off leader election periodically by sending out RequestVote RPCs 
    - when it hasn't heard from another peer for a while. 
- Implement the RequestVote() RPC handler so that servers will vote for one another.
- Implement heartbeats, 
  - define an AppendEntries RPC struct, 
  - teh leader send them out periodically. 
  - Write an AppendEntries RPC handler method.
  - The tester requires that the leader send heartbeat RPCs no more than ten times per second.
- The paper's Section 5.2 mentions election timeouts in the range of 150 to 300 milliseconds.
  - Such a range only makes sense if the leader sends heartbeats considerably more often than once per 150 milliseconds (e.g., once per 10 milliseconds).
- You'll need to write code that takes actions periodically or after delays in time. 
  - The easiest way to do this is to create a goroutine with a loop that calls time.Sleep();
  - see the ticker() goroutine that Make() creates for this purpose. 
  - Don't use Go's time.Timer or time.Ticker, which are difficult to use correctly.
  - The tester calls your Raft's rf.Kill() when it is permanently shutting down an instance. 
    - You can check whether Kill() has been called using rf.killed(). 
    - You may want to do this in all loops, to avoid having dead Raft instances print confusing messages.

```shell
go test -run 3A.
```

#### Part 3B: Log Replication

Implement the leader and follower code to append new log entries.

Hint:
- Raft log is **1-indexed**, but we suggest that you view it as 0-indexed, 
  - starting out with an entry (at index=0) that has term 0. 
  - That allows the very first AppendEntries RPC to contain 0 as PrevLogIndex, 
  - be a valid index into the log.
- You will need to implement the **election restriction** (section 5.4.1 in the paper).
- Your code may have loops that repeatedly check for certain events. 
- Don't have these loops execute continuously without pausing, 
  - since that will slow your implementation enough that it fails tests. 
  - Use Go's condition variables, or insert a time.Sleep(10 * time.Millisecond) in each loop iteration.

Goal: Pass TestBasicAgree3B().
- Start by implementing `Start()`, 
- write the code to **send and receive new log entries** via AppendEntries RPCs, following Figure 2. 
- Send each newly **committed entry on applyCh** on each peer.


```shell
 go test -run 3B
```
