package kvsrv

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.5840/labrpc"
)


type Clerk struct {
	server *labrpc.ClientEnd
	// You will have to modify this struct.
	id int
	op_count int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(server *labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.server = server
	// You'll have to add code here.
	ck.id = int(nrand())
	ck.op_count = 1
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
func (ck *Clerk) Get(key string) string {

	// You will have to modify this function.
	args := GetArgs{}
	reply := GetReply{}
	args.Key = key
	args.Id = ck.id
	args.OpCount = ck.op_count
	// print("Client请求Get:","Key:",args.Key,"\n")
	ok := ck.server.Call("KVServer.Get", &args, &reply)
	for !ok {
		// 短暂睡眠后重试，避免立即重试造成网络拥塞
		time.Sleep(10 * time.Millisecond)
		ok =  ck.server.Call("KVServer.Get", &args, &reply)
	}
	ck.op_count += 1
	// print("Client:Get","Key:",args.Key," 返回值:",reply.Value,"\n")
	return reply.Value
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
	args := PutAppendArgs{}
	reply := PutAppendReply{}
	args.Key = key
	args.Value = value
	args.Id = ck.id
	args.OpCount = ck.op_count
	// print("Client请求:",op,"Key:",args.Key," Value",args.Value,"\n")
	ok := ck.server.Call("KVServer."+op, &args, &reply)
	for !ok {
		// 短暂睡眠后重试，避免立即重试造成网络拥塞
		time.Sleep(10 * time.Millisecond)
		ok = ck.server.Call("KVServer."+op, &args, &reply)
	}
	ck.op_count += 1
	// print("Client接受:",op,"Key:",args.Key ," 返回值:",reply.Value,"\n")
	return reply.Value
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}

// Append value to key's value and return that value
func (ck *Clerk) Append(key string, value string) string {
	return ck.PutAppend(key, value, "Append")
}
