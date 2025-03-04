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

Maintaining a version number for each key will be useful for 
implementing locks using Put and ensuring at-most-once semantics for Put's 
when the network is unreliable and the client retransmits.

When you've finished this lab and passed all the tests, 
you'll have a linearizable key/value service from the point of view of clients calling Clerk.Get and Clerk.Put. 

That is, if client operations aren't concurrent, 
each client Clerk.Get and Clerk.Put will observe the modifications to the state 
implied by the preceding sequence of operations. For concurrent operations, 
the return values and final state will be the same as if the operations had executed one at a time in some order. 

Operations are concurrent if they overlap in time: for example, 
if client X calls Clerk.Put(), and client Y calls Clerk.Put(), and then client X's call returns. 

An operation must observe the effects of all operations that have completed before the operation starts. 
See the FAQ on linearizability for more background.

Linearizability is convenient for applications 
because it's the behavior you'd see from a single server that processes requests one at a time. 

For example, if one client gets a successful response from the server for an update request, 
subsequently launched reads from other clients are guaranteed to see the effects of that update. 
Providing linearizability is relatively easy for a single server.

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


## Lab3 Raft