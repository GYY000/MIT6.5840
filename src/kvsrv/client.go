package kvsrv

import (
	"6.5840/labrpc"
	"sync"
)
import "crypto/rand"
import "math/big"

type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func nrandInt32() int32 {
	maxInt32 := int64(1) << 31
	bigx, _ := rand.Int(rand.Reader, big.NewInt(maxInt32))
	x := bigx.Int64()
	return int32(x)
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	return ck
}

// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
var mu = sync.Mutex{}
var rpcPool = [10]int32{}
var front = 0
var end = 0

func (ck *Clerk) Get(key string) string {
	stamp := getRand()
	args := GetArgs{key, fetchLastRPC(), stamp}
	reply := GetReply{}
	ok := ck.server.Call("KVServer.Get", &args, &reply)
	for !ok {
		ok = ck.server.Call("KVServer.Get", &args, &reply)
	}
	// You will have to modify this function.
	putIntoRPC(stamp)
	return reply.Value
}

func getRand() int32 {
	ans := nrandInt32()
	for ans == -1 {
		ans = nrandInt32()
	}
	return ans
}

func fetchLastRPC() int32 {
	mu.Lock()
	defer mu.Unlock()
	if (end+11)%10 != (front+10)%10 {
		ans := rpcPool[front]
		front = (front + 1) % 10
		return ans
	} else {
		return -1
	}
}

func putIntoRPC(x int32) {
	mu.Lock()
	defer mu.Unlock()
	rpcPool[end] = x
	end = (end + 1) % 10
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.server.Call("KVServer."+op, &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) string {
	// You will have to modify this function.
	stamp := getRand()
	args := PutAppendArgs{key, value, fetchLastRPC(), stamp}
	reply := PutAppendReply{}
	ok := ck.server.Call("KVServer."+op, &args, &reply)
	for !ok {
		ok = ck.server.Call("KVServer."+op, &args, &reply)
	}
	putIntoRPC(stamp)
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
