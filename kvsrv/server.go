package kvsrv

import (
	"log"
	// "os"
	// "path/filepath"
	"sync"
	// "strconv"
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
	// Your definitions here.
	key_values   map[string]string
	logs map[int]LogValue
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	log,exist := kv.logs[args.Id]
	if(!exist){
		log := LogValue{0,""}
		kv.logs[args.Id] = log
	}
	if log.count != args.OpCount {
		reply.Value = kv.key_values[args.Key]
		kv.logs[args.Id] =  LogValue{args.OpCount,reply.Value}
	}else {
		// print("Server:重复Get\n")
		reply.Value= kv.logs[args.Id].Value
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	log,exist := kv.logs[args.Id]
	if(!exist){
		log := LogValue{0,""}
		kv.logs[args.Id] = log
	}
	if log.count != args.OpCount  {
		kv.key_values[args.Key] = args.Value
		// WriteLog(id_s,args.OpCount)
		// kv.logs[args.Id] =  LogValue{args.OpCount,"",time.Now()}
		// print("Server:Put:",args.Value,"当前:","key:",args.Key,"value:",kv.key_values[args.Key],"\n")
	}else {
		// print("Server:重复Put\n")
	}
	kv.mu.Unlock()
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	log,exist := kv.logs[args.Id]
	if(!exist){
		log := LogValue{0,""}
		kv.logs[args.Id] = log
	}
	if log.count != args.OpCount{
		reply.Value = kv.key_values[args.Key]
		kv.key_values[args.Key] += args.Value
		// WriteLog(id_s,args.OpCount)
		kv.logs[args.Id] =  LogValue{args.OpCount,reply.Value}
		// print("Server:Append:",args.Value,"当前:","key:",args.Key,"value:",kv.key_values[args.Key],"\n")
	}else {
		// print("Server:重复Append\n")
		reply.Value= kv.logs[args.Id].Value
	}
	kv.mu.Unlock()
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	// You may need initialization code here.
	kv.key_values = make(map[string]string)
	kv.logs = make(map[int]LogValue)
	return kv
}