package kvsrv

import (
	"log"
	"os"
	"path/filepath"
	"sync"
	"strconv"
	"strings"
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
	key_values map[string]string
	logDir     string
}

func (kv *KVServer) readLog(id int) LogValue {
	filePath := filepath.Join(kv.logDir, strconv.Itoa(id))
	data, err := os.ReadFile(filePath)
	if err != nil {
		// 如果文件不存在，返回默认的 LogValue
		return LogValue{0, ""}
	}
	parts := strings.SplitN(string(data), "|", 2)
	count, _ := strconv.Atoi(parts[0])
	value := parts[1]
	return LogValue{count, value}
}

func (kv *KVServer) writeLog(id int, log LogValue) {
	filePath := filepath.Join(kv.logDir, strconv.Itoa(id))
	data := strconv.Itoa(log.count) + "|" + log.Value
	os.WriteFile(filePath, []byte(data), 0644)
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log := kv.readLog(args.Id)
	if log.count != args.OpCount {
		reply.Value = kv.key_values[args.Key]
		kv.writeLog(args.Id, LogValue{args.OpCount, reply.Value})
	} else {
		reply.Value = log.Value
	}
}

func (kv *KVServer) Put(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log := kv.readLog(args.Id)
	if log.count != args.OpCount {
		kv.key_values[args.Key] = args.Value
		kv.writeLog(args.Id, LogValue{args.OpCount, ""})
	}
}

func (kv *KVServer) Append(args *PutAppendArgs, reply *PutAppendReply) {
	kv.mu.Lock()
	defer kv.mu.Unlock()
	log := kv.readLog(args.Id)
	if log.count != args.OpCount {
		reply.Value = kv.key_values[args.Key]
		kv.key_values[args.Key] += args.Value
		kv.writeLog(args.Id, LogValue{args.OpCount, reply.Value})
	} else {
		reply.Value = log.Value
	}
}

func StartKVServer() *KVServer {
	kv := new(KVServer)
	kv.key_values = make(map[string]string)
	kv.logDir = "logs"
	os.MkdirAll(kv.logDir, os.ModePerm)
	return kv
}