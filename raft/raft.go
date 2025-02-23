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
	// "fmt"
	// "log"
	"math/rand"
	"sync"
	"sync/atomic"
	"time"

	//	"6.5840/labgob"
	"6.5840/labrpc"
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

	//3A
	role string
	currentTerm int
	votedFor int

	//3B
	logs []LogEntry
	commitIndex int
	lastApplied int
	applyCh chan ApplyMsg

	nextIndex []int
	matchIndex []int
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	rf.mu.Lock()
	defer rf.mu.Unlock()
	term = rf.currentTerm
	isleader = rf.role == "leader"
	// Your code here (3A).
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


// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (3A, 3B).
	Term int
	CandidateId int
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (3A).
	VoteGranted bool
	Term int
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (3A, 3B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
		return
	}else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.role = "follower"
		reply.Term = rf.currentTerm
		return
	}else if rf.votedFor == -1 {
		reply.Term = rf.currentTerm
		reply.VoteGranted = true
		rf.votedFor = args.CandidateId
		rf.role = "follower"
		return
	}
}

type AppendEntriesArgs struct {
	Term int
	LeaderId int
	PrevLogIndex int
	PrevLogTerm int
	Entries []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	//处理心跳
	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
	}else {
		// fmt.Printf("server %d receive heartbeat from %d\n", rf.me, args.LeaderId)
		rf.currentTerm = args.Term
		rf.role = "follower"
		reply.Term = rf.currentTerm
	}
	if(args.Entries == nil){
		return
	}

	//处理日志
	if(args.PrevLogIndex > len(rf.logs) || rf.logs[args.PrevLogIndex].Term != args.PrevLogTerm){
		reply.Success = false
		//删除不同步的日志
		if args.PrevLogIndex <= len(rf.logs) {
			rf.logs = rf.logs[:args.PrevLogIndex]
		}
	}else{
		//追加日志
		rf.logs = append(rf.logs, args.Entries...)
	}

	//标记可以提交的日志
	if args.LeaderCommit > rf.commitIndex {
		if(args.LeaderCommit < len(rf.logs)){
			rf.commitIndex = args.LeaderCommit
		}else{
			rf.commitIndex = len(rf.logs)
		}
	}
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
func (rf *Raft) sendRequestVote() {
	
	rf.mu.Lock()
	rf.currentTerm++
	// 准备投票参数
	args := &RequestVoteArgs{
		Term: rf.currentTerm,
		CandidateId: rf.me,
	}
	rf.votedFor = rf.me
	rf.mu.Unlock()

	//send RequestVote RPCs to all other servers
	votes := 1  // 给自己投票
	voteCh := make(chan RequestVoteReply, len(rf.peers)-1)

	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				reply := &RequestVoteReply{
					VoteGranted: false,   // RPC失败视为投票失败
					Term:    args.Term,  // 使用当前term而不是reply.Term
				}
				if rf.peers[server].Call("Raft.RequestVote", args, reply){
					if reply.VoteGranted {
						voteCh <- *reply
					} else {
						voteCh <- *reply
					}
				} else {
					voteCh <- *reply
				}
			}(i)
		}
	}

	// 统计投票结果
	maxReplyTerm := 0
	for i := 0; i < len(rf.peers)-1; i++ {
		reply := <-voteCh
		if reply.VoteGranted {
			votes++
		} else {
			if reply.Term > args.Term && reply.Term > maxReplyTerm {
				maxReplyTerm = reply.Term
			}
		}
	}

	rf.mu.Lock()
	if(rf.role == "follower"){//候选期间是否收到心跳信号？
		rf.mu.Unlock()
		return
	} else if maxReplyTerm > args.Term{//是否有更高任期的存在？
		rf.currentTerm = maxReplyTerm
		rf.role = "follower"
		rf.mu.Unlock()
		return
	} else if votes > len(rf.peers)/2 {
		rf.role = "leader"
		rf.matchIndex = make([]int, len(rf.peers))
		rf.nextIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.logs)
			rf.matchIndex[i] = 0
		}
		rf.sendHeartBeat()
		go rf.replicateToFollower()
	}
	rf.mu.Unlock()
}


func(rf *Raft) replicateToFollower(){
	for rf.role == "leader" {
		//进行日志同步
		for i := 0; i < len(rf.peers); i++ {
			if i != rf.me {
				go func(server int) {
					args := &AppendEntriesArgs{
						Term: rf.currentTerm,
						LeaderId: rf.me,
						PrevLogIndex: rf.nextIndex[server]-1,
						PrevLogTerm: rf.logs[rf.nextIndex[server]-1].Term,
						Entries: rf.logs[rf.nextIndex[server]:],
						LeaderCommit: rf.commitIndex,
					}
					reply := &AppendEntriesReply{
						Term: 0,
						Success: false,
					}
					rf.peers[server].Call("Raft.AppendEntries", args, reply)
					if reply.Success {
						rf.matchIndex[server] = len(rf.logs)
						rf.nextIndex[server] = len(rf.logs)
					} else {
						rf.nextIndex[server]--
					}
				}(i)
			}
		}

		//判断是否提交日志并写入applyCh
		for i := rf.commitIndex; i <= len(rf.logs); i++{
			count := 1
			for j := 0; j < len(rf.peers); j++ {
				if rf.matchIndex[j] >= i {
					count++
				}
			}
			if count > len(rf.peers)/2 {
				rf.commitIndex = i
				rf.applyCh <- ApplyMsg{CommandValid: true, Command: rf.logs[i].Command, CommandIndex: i}
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
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
	isLeader := true

	// Your code here (3B).
	if rf.role != "leader" {
		isLeader = false
	}else{
		//append log
		rf.mu.Lock()
		index = len(rf.logs)
		term = rf.currentTerm
		newLog := LogEntry{Command: command,Term: term,Index: index}
		rf.logs = append(rf.logs, newLog)
		rf.mu.Unlock()
	}
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
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}


func (rf *Raft) sendHeartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i != rf.me {
			go func(server int) {
				args := &AppendEntriesArgs{
					Term: rf.currentTerm,
					LeaderId: rf.me,
					PrevLogIndex: len(rf.logs)-1,
					PrevLogTerm: 0,
					Entries: nil,
					LeaderCommit: 0,
				}
				reply := &AppendEntriesReply{
					Term: 0,
					Success: false,
				}
				rf.peers[server].Call("Raft.AppendEntries", args, reply)
				if reply.Term > rf.currentTerm {
					rf.currentTerm = reply.Term
					rf.role = "follower"
				}
			}(i)
		}
	}
}

func (rf *Raft) ticker() {//只负责查看是否超时
	for rf.killed() == false {

		// Your code here (3A)
		// Check if a leader election should be started.
		
		//debug print

		// fmt.Printf("server %d, role: %s, term: %d\n", rf.me, rf.role, rf.currentTerm)
		ms := 50 + (rand.Int63() % 150)

		if rf.role == "follower" {
			rf.role = "candidate"
		} else if rf.role == "candidate" {
			rf.sendRequestVote() 
			ms = 50 + (rand.Int63() % 150)
		} else {
			rf.sendHeartBeat()
			ms = 50
		}

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
	rf.votedFor = -1
	rf.logs = make([]LogEntry, 0)
	rf.applyCh = applyCh
	rf.commitIndex = 0
	rf.lastApplied = 0

	// Your initialization code here (3A, 3B, 3C).
	rf.currentTerm = 0
	rf.role = "follower"
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()


	return rf
}
