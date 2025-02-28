package kvsrv

import (
	"log"
	"sync"

	"6.5840/kvsrv1/rpc"
	"6.5840/labrpc"
	"6.5840/tester1"
)

// const Debug = true
const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	// Your definitions here.
	records map[string]*Record // in-memory map that records for each key a (value, version) tuple.
}

type Record struct {
	Value   string
	Version rpc.Tversion // the number of times the key has been written
}

func MakeKVServer() *KVServer {
	kv := &KVServer{}
	// Your code here.

	kv.records = make(map[string]*Record)
	return kv
}

// Get returns the value and version for args.Key,
// if args.Key exists. Otherwise, Get returns ErrNoKey.
func (kv *KVServer) Get(args *rpc.GetArgs, reply *rpc.GetReply) {
	// Your code here.
	record, ok := kv.records[args.Key]
	if !ok { // Otherwise, Get returns ErrNoKey.
		reply.Err = rpc.ErrNoKey
		return
	}

	// returns the value and version for args.Key,
	reply.Err = rpc.OK
	reply.Value = record.Value
	reply.Version = record.Version
}

// Put Update the value for a key if args.Version matches the version of
// the key on the server.
// If versions don't match, return ErrVersion.
// If the key doesn't exist, Put installs the value if the args.Version is 0,
// and returns ErrNoKey otherwise.
func (kv *KVServer) Put(args *rpc.PutArgs, reply *rpc.PutReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// If the key doesn't exist, Put installs the value if the args.Version is 0.
	if _, ok := kv.records[args.Key]; !ok {
		if args.Version == 0 {
			kv.records[args.Key] = &Record{
				Value:   args.Value,
				Version: 1,
			}
			reply.Err = rpc.OK
			DPrintf("++++create new record %v:%v-%v\n", args.Key, kv.records[args.Key].Value, kv.records[args.Key].Version)

		} else { // and returns ErrNoKey otherwise.
			reply.Err = rpc.ErrNoKey
			DPrintf("----the key doesn't exist!")
		}
		return
	}

	// the key exist
	// if args.Version matches the version of the key on the server.
	if args.Version == kv.records[args.Key].Version {
		// Update the value
		kv.records[args.Key].Value = args.Value
		kv.records[args.Key].Version++
		reply.Err = rpc.OK
		DPrintf("++++update record %v:%v-%v\n", args.Key, args.Value, args.Version)
	} else { // If versions don't match
		reply.Err = rpc.ErrVersion
		DPrintf("----versions don't match!")
	}
}

// Kill You can ignore Kill() for this lab
func (kv *KVServer) Kill() {
}

// StartKVServer You can ignore all arguments; they are for replicated KVservers
func StartKVServer(ends []*labrpc.ClientEnd, gid tester.Tgid, srv int, persister *tester.Persister) []tester.IService {
	kv := MakeKVServer()
	return []tester.IService{kv}
}
