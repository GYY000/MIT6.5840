package kvsrv

import (
	"log"
	"sync"
)

const Debug = false

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug {
		log.Printf(format, a...)
	}
	return
}

type KVServer struct {
	mu sync.Mutex

	content   map[string]string
	dupRecord map[int32]string
	// Your definitions here.
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if value, flag := kv.dupRecord[args.Stamp]; flag {
		reply.Value = value
		return
	}
	if _, flag := kv.dupRecord[args.LastRPC]; flag {
		delete(kv.dupRecord, args.LastRPC)
	}
	if value, ok := kv.content[args.Key]; ok {
		reply.Value = value
	} else {
		reply.Value = ""
	}
	kv.dupRecord[args.Stamp] = reply.Value
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	_, flag := kv.dupRecord[args.Stamp]
	if flag {
		reply.Value = ""
		return
	}
	if _, flag := kv.dupRecord[args.LastRPC]; flag {
		delete(kv.dupRecord, args.LastRPC)
	}
	kv.content[args.Key] = args.Value
	reply.Value = args.Value
	kv.dupRecord[args.Stamp] = ""
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	defer kv.mu.Unlock()
	value, flag := kv.dupRecord[args.Stamp]
	if flag {
		reply.Value = value
		return
	}
	if _, flag := kv.dupRecord[args.LastRPC]; flag {
		delete(kv.dupRecord, args.LastRPC)
	}
	if value, ok := kv.content[args.Key]; ok {
		reply.Value = value
		kv.content[args.Key] = kv.content[args.Key] + args.Value
	} else {
		reply.Value = ""
		kv.content[args.Key] = args.Value
	}
	kv.dupRecord[args.Stamp] = reply.Value
}

func StartKVServer() *KVServer {
	kv := new(KVServer)

	// You may need initialization code here.
	kv.mu = sync.Mutex{}
	kv.content = make(map[string]string)
	kv.dupRecord = make(map[int32]string)
	return kv
}
