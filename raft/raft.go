package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	//	"bytes"
	"fmt"
	"math/rand"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
)

// 定义常量
const (
    HeartbeatInterval = 300 // 心跳间隔，单位为毫秒
    ElectionTimeout   = 500 // 选举超时时间，单位为毫秒
)


// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 3D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 3D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

//日志项
type LogEntry struct {
    Command interface{}
    Term    int
    Index   int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (3A, 3B, 3C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state string //节点状态，follower,candidate,leader,初始时为follower
	heartbeat_timer int64 //心跳计时器，归0启动选举

	currentTerm int //当前周期
	votedFor int //当前周期收到的candidate id,可以置为null
	logs []LogEntry
	applyCh chan ApplyMsg //提交通道
	
	commitIndex int //最后一个被提交的日志的index
	lastApplied int 

	//leader专属状态
	nextindex []int  //下一个要发送给follower的index
	matchindex []int //follower已经收到的index
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (3A).
	rf.mu .Lock()
	term = rf.currentTerm
	if(rf.state == "leader"){
		isleader = true
	} else {
		isleader = false
	}
	rf.mu.Unlock()
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
// before you've implemented snapshots, you should pass nil as the
// second argument to persister.Save().
// after you've implemented snapshots, pass the current snapshot
// (or nil if there's not yet a snapshot).
func (rf *Raft) persist() {
	// Your code here (3C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// raftstate := w.Bytes()
	// rf.persister.Save(raftstate, nil)
}


// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (3C).
	// Example:
	// r := bytes.NewBuffer(data)
	// d := labgob.NewDecoder(r)
	// var xxx
	// var yyy
	// if d.Decode(&xxx) != nil ||
	//    d.Decode(&yyy) != nil {
	//   error...
	// } else {
	//   rf.xxx = xxx
	//   rf.yyy = yyy
	// }
}


// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (3D).

}

type AppendEntriesArgs struct{
	Term int
	LeaderId int 	
	PrevLogIndex int 
	PrevLogTerm int 
	Entries []LogEntry
	LeaderCommit int //leader commit index
}

type AppendEntriesReply struct{
	Term int 
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	// print("id:"+strconv.Itoa(rf.me) + " " + "got vote AppendEntries,from " +strconv.Itoa(args.LeaderId) + " term:" + strconv.Itoa(args.Term) + "\n")
	// 心跳计时器，如果entries为空，重置心跳定时器

	//如果有日志，则处理日志
	//处理原则：
	//① 如果term < currentTerm,success = false 
	//② 如果args中的prevLogIndex指向的日志项与本节点日志项信息不匹配（args中的prevLogTerm!=本节点中的prevLogIndex指向的日志项的term),success = false
	//③ 如果args中的entries与本节点的entries有冲突（即prev的日志项一样，但是prev之后的日志项本节点已经有记录），则删除冲突的日志项并以leader发来的为准
	//④将args中的entries添加到本节点的日志中
	//⑤如果leaderCommit > commitIndex,将commitIndex  = min(leaderCommit,最后一个新entry的index)，commit一定是按顺序的
	rf.mu.Lock()
	if(args.Term >= rf.currentTerm){
		rf.votedFor = -1
		rf.currentTerm = args.Term
		rf.state = "follower"
		rf.votedFor = -1
		rf.heartbeat_timer = HeartbeatInterval

		if(len(args.Entries)>0){
			reply.Success = true
			if(args.PrevLogIndex >0 && rf.logs[args.PrevLogIndex].Term!=args.PrevLogTerm){
				reply.Success = false
			}else{
				fmt.Printf("Appending logs: current logs: %v, args logs: %v, PrevLogIndex: %d, PrevLogTerm: %d\n", rf.logs, args.Entries, args.PrevLogIndex, args.PrevLogTerm)
				rf.logs = rf.logs[:args.PrevLogIndex]
				rf.logs = append(rf.logs, args.Entries...)
				fmt.Printf("After append: current logs: %v\n", rf.logs)
				if(args.LeaderCommit > rf.commitIndex){
					if(args.LeaderCommit < rf.logs[len(rf.logs)-1].Index ){
						rf.commitIndex = args.LeaderCommit
					}else{
						rf.commitIndex =  rf.logs[len(rf.logs)-1].Index
					}
				}
			}
		}
		
	}else{
		reply.Success = false
	}
	rf.mu.Unlock()
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int 
	CandidateId int 
	LastLogIndex int 
	LastLogTerm int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	Term int 
	VoteGranted bool
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	//处理请求，投票给候选者，只能投一票,,遵循先到先得原则
	// print("id:"+strconv.Itoa(rf.me) + " " + "got vote request,from " +strconv.Itoa(args.CandidateId) + " term:" + strconv.Itoa(args.Term) + "\n")
	rf.mu.Lock()
	if(args.Term>rf.currentTerm){//最新的投票请求
		reply.Term = rf.currentTerm
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true //已投票给你了
		rf.heartbeat_timer = HeartbeatInterval
		rf.state = "follower"
		// print("id:"+strconv.Itoa(rf.me) + " " + "vote for " +strconv.Itoa(args.CandidateId) +  "\n")
	} else if(args.Term == rf.currentTerm){//本轮已经投过票了
		reply.Term = rf.currentTerm
		reply.VoteGranted = false //没票了
		// print("id:"+strconv.Itoa(rf.me) + " " + "has voted for " +strconv.Itoa(rf.votedFor) +  "\n")
	} else {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false //让candidate节点先更新自己的状态
		// print("id:"+strconv.Itoa(rf.me) + " " + "newer than  " +strconv.Itoa(args.CandidateId) +  "\n")
	}
	rf.mu.Unlock()
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {//应该有超时的逻辑
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}


func (rf *Raft) replicateToFollower(){

	// for (rf.killed() == false && rf.state == 'leader'){
	// 	ms := 50 + (rand.Int63() % 300)
	// 	time.Sleep(time.Duration(ms) * time.Millisecond)
	// }
	// prevLogIndex := 0
	// prevLogTerm := 0
	// if(len(rf.logs)>0){
	// 	prevLogIndex = rf.logs[len(rf.logs)-1].Index
	// 	prevLogTerm = rf.logs[len(rf.logs)-1].Term
	// }
	// //广播日志到follower节点
	// args := AppendEntriesArgs{}
	// args.LeaderId = rf.me
	// args.Term = rf.currentTerm
	// args.PrevLogTerm = prevLogTerm
	// args.PrevLogIndex = prevLogIndex
	// args.Entries = append(args.Entries, newLog)
	// args.LeaderCommit = prevLogIndex +1 ;
	// for server := range rf.peers{
	// 	if(server != rf.me){
	// 		go func (server int)  {
	// 			reply := AppendEntriesReply{}
	// 			rf.sendAppendEntries(server,&args,&reply)
	// 		}(server)
	// 	}
	// }
	// index = args.Entries[0].Index
	// term = args.Entries[0].Term
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := false
	// Your code here (3B).
	rf.mu.Lock()
	if(rf.state == "leader"){
		isLeader = true
		//将command写入到自己的log中
		index = len(rf.logs)+1
		term = rf.currentTerm
		newLog := LogEntry{Command: command,Term: term,Index: index}
		rf.logs = append(rf.logs, newLog)//加入本地日志
	}
	rf.mu.Unlock()
	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
	print("id:"+strconv.Itoa(rf.me) + " killed\n")
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

func (rf *Raft) sendHeartBeat(currentTerm int){
	for server :=  range rf.peers{
		if(server != rf.me){
			go func (server int)  {
				// print("id:"+strconv.Itoa(rf.me) + " " + "sending appendentries to "+strconv.Itoa(server)+ " term:" +strconv.Itoa(rf.currentTerm) + "\n")
				args := AppendEntriesArgs{}
				reply := AppendEntriesReply{}
				args.LeaderId = rf.me
				args.Term = currentTerm
				rf.sendAppendEntries(server,&args,&reply)
			}(server)
		}
	}
}

func (rf *Raft) startElection(Term int){
	NewTerm := Term
	votes := 0 //选票数量
	var votesMu sync.Mutex // 用于保护votes阶段变量的互斥锁
	var wg sync.WaitGroup //用来等待所有投票线程结束以统计结果
	for server := range rf.peers{
		if server != rf.me{
			wg.Add(1) //任务+1
			go func(server int) {
				defer wg.Done() //goroutine结束后计数减一
				args := RequestVoteArgs{}
				reply := RequestVoteReply{}
				args.CandidateId = rf.me
				args.Term = Term
				ok := rf.sendRequestVote(server, &args, &reply)
				if(ok){
					// print("id:" + strconv.Itoa(rf.me) + " got reply Term " + strconv.Itoa(reply.Term) + " " + strconv.FormatBool(reply.VoteGranted) + "\n")
					votesMu.Lock()
					if(NewTerm < reply.Term){ //记录其他节点的最新term
						NewTerm = reply.Term
					}else{
						if(reply.VoteGranted){//选票+1
							// print("id:"+strconv.Itoa(rf.me) + " got vote from " + strconv.Itoa(server) + "Term " + strconv.Itoa(reply.Term) +"\n")
							votes += 1;
						}
					}
					votesMu.Unlock()
				}
			}(server)//应该是go routine的语法糖写法吧
		}
	}
	wg.Wait()//等待所有投票询问完成

	//检查此时的状态是否仍是candidate
	rf.mu.Lock()
	state := rf.state
	rf.mu.Unlock()
	if(state != "candidate"){
		// print("id:"+strconv.Itoa(rf.me) +"stop candidtae cause it is no longer candidate" +"\n")
		return
	}

	//如果有更新的Term，转为follow状态等待新的heartbeat
	if(NewTerm > Term){
		rf.mu.Lock()
		if(rf.state != "follower"){
			rf.state = "follower"
			rf.currentTerm = NewTerm
			rf.heartbeat_timer = HeartbeatInterval
		}
		rf.mu.Unlock()
		// print("id:"+strconv.Itoa(rf.me) +"stop candidtae cause got new Term" +"\n")
		return
	}

	//计算票数
	// print("id:"+strconv.Itoa(rf.me) + " got votes num " +strconv.Itoa(votes + 1) + " need " + strconv.Itoa(len(rf.peers)/2) +"\n")
	if(votes + 1 > len(rf.peers)/2){//获得半数以上的选票
		// print("id:"+strconv.Itoa(rf.me) + " become leader " + "\n")
		rf.mu.Lock()
		UpdateSuccess := false
		if(rf.state == "candidate" && rf.currentTerm == Term){
			rf.state = "leader"
			rf.heartbeat_timer = HeartbeatInterval
			rf.sendHeartBeat(rf.currentTerm)//立刻发送一次hearbeat进行告知，防止同时出现多个leader
			UpdateSuccess = true
		}
		rf.mu.Unlock()
		if(UpdateSuccess){
			//初始化leader相关的状态
			rf.nextindex = make([]int, len(rf.peers))
			for i := range rf.nextindex {
				rf.nextindex[i] = 1
			}
			rf.matchindex = make([]int, len(rf.peers))
			for i := range rf.matchindex {
				rf.matchindex[i] = 0
			}
			//启动日志同步
			// go rf.replicateToFollower()
		}
	}
}

func (rf *Raft) ticker() {
	for !rf.killed() {
		// print("ticker\n")
		ms := 50 + (rand.Int63() % 300)
		// Your code here (3A)
		// Check if a leader election should be started.
		// print("id:"+strconv.Itoa(rf.me) + " " + rf.state + "\n")
		rf.mu.Lock()
		rf.heartbeat_timer -= ms
		switch rf.state {
		case "follower":
			// print("id:"+strconv.Itoa(rf.me) + " " + "follower Term "+ strconv.Itoa(rf.currentTerm) + "\n")
			if(rf.heartbeat_timer < 0 ){
				//启动选举
				rf.state = "candidate"
			}
		case "leader":
			// print("id:"+strconv.Itoa(rf.me) + " " + "being leader Term "+ strconv.Itoa(rf.currentTerm) +"\n")
			go rf.sendHeartBeat(rf.currentTerm)
		case "candidate":
			if(rf.heartbeat_timer< 0 ){
				rf.votedFor = rf.me
				rf.currentTerm += 1
				rf.heartbeat_timer = ElectionTimeout//设置选举超时时间，超过则重新发起选举
				// print("id:"+strconv.Itoa(rf.me) + " " + "Term:" + strconv.Itoa(rf.currentTerm)+ " candidate start\n")
				go rf.startElection(rf.currentTerm)
			}
		}
		rf.mu.Unlock()

		// pause for a random amount of time between 50 and 350
		// milliseconds.
		time.Sleep(time.Duration(ms) * time.Millisecond)
	}
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here (3A, 3B, 3C).
	rf.state = "follower"
	rf.heartbeat_timer = 500
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.applyCh = applyCh
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()

	return rf
}
