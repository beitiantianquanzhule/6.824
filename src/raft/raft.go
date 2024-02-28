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
	"fmt"
	"math/rand"
	"strconv"

	//	"bytes"
	"sync"
	"sync/atomic"
	"time"

	//	"6.824/labgob"
	"6.824/labrpc"
)

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in part 2D you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh, but set CommandValid to false for these
// other uses.
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int

	// For 2D:
	SnapshotValid bool
	Snapshot      []byte
	SnapshotTerm  int
	SnapshotIndex int
}

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	currentTerm int   // 记录到自己的最新的term
	votedFor    int   // 自己投票的index，如果是成为候选者则投票给自己
	log         []Log // log entries

	statusMu sync.Mutex
	status   int // 服务器的状态，0是跟随者，1是候选者，2是领导者

	heartBeatMu  sync.Mutex
	hasHeartBeat bool // 是否有收到心跳检测

	votedMu     sync.Mutex // 投票加锁
	hasVoted    bool       // 至多只能投一次票，每次选举时刷新
	votedTerm   int        // 投票的term
	commitIndex int        // 已提交的index
	lastApplied int        // 已执行的index

	nextIndex  int // 下一条要接收的log entries的序号，初始化为领导人最后索引值 + 1
	matchIndex int //
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.

}

type Log struct {
	instruct string
	term     int32
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.statusMu.Lock()
	defer rf.statusMu.Unlock()
	isleader = false
	if rf.status == 2 {
		isleader = true
	}
	term = rf.currentTerm
	return term, isleader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	// Example:
	// w := new(bytes.Buffer)
	// e := labgob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
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

// A service wants to switch to snapshot.  Only do so if Raft hasn't
// have more recent info since it communicate the snapshot on applyCh.
func (rf *Raft) CondInstallSnapshot(lastIncludedTerm int, lastIncludedIndex int, snapshot []byte) bool {

	// Your code here (2D).

	return true
}

// the service says it has created a snapshot that has
// all info up to and including index. this means the
// service no longer needs the log through (and including)
// that index. Raft should now trim its log as much as possible.
func (rf *Raft) Snapshot(index int, snapshot []byte) {
	// Your code here (2D).

}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // 候选人申请的任期号
	CandidateId  int // 候选人序号
	LastLogIndex int // 候选人最后日志序号
	LastLogTerm  int // 候选人日志最后任期号
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int  // 当前任期号
	VoteGranted bool // 是否赢得选票
}

type AppendEntriesArgs struct {
	Leader int // 领导者的编号
	Term   int // 领导者的任期
}

type AppendEntriesReply struct {
	Term int // 现在的任期
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	reply.Term = rf.currentTerm
	fmt.Print(time.Now())
	fmt.Println(strconv.Itoa(rf.me) + "收到投票请求" + "给" + strconv.Itoa(args.CandidateId))
	rf.heartBeatMu.Lock()
	rf.hasHeartBeat = true
	rf.heartBeatMu.Unlock()
	rf.votedMu.Lock()
	defer rf.votedMu.Unlock()
	rf.statusMu.Lock()
	if rf.status == 2 && rf.currentTerm < args.Term {
		rf.status = 0
	}
	rf.statusMu.Unlock()
	if rf.hasVoted && rf.currentTerm >= args.Term {
		fmt.Println(strconv.Itoa(rf.me) + "已经投票了" + "无法给" + strconv.Itoa(args.CandidateId) + "投票了" + "已经投给了" + strconv.Itoa(rf.votedFor))
		reply.VoteGranted = false
		return
	}

	if args.Term <= rf.currentTerm || args.Term < rf.votedTerm {
		fmt.Println(strconv.Itoa(rf.me) + "的term 比" + strconv.Itoa(args.CandidateId) + "大")
		reply.VoteGranted = false
		return
	}
	rf.hasVoted = true

	rf.votedFor = args.CandidateId
	rf.nextIndex = args.LastLogIndex + 1
	rf.currentTerm = args.Term
	reply.VoteGranted = true
}

func (rf *Raft) ReceiveInstructRPC(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	fmt.Println("我是" + strconv.Itoa(rf.me) + "我的term是" + strconv.Itoa(rf.currentTerm) + "收到的id是" + strconv.Itoa(args.Leader) + "term是" + strconv.Itoa(args.Term))
	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		return
	}
	rf.heartBeatMu.Lock()
	rf.hasHeartBeat = true
	rf.heartBeatMu.Unlock()
	rf.votedMu.Lock()
	rf.hasVoted = false
	rf.votedMu.Unlock()
	//rf.currentTerm = args.Term
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
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply, resultChannel chan ChannelResult) {
	fmt.Println(time.Now())
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	fmt.Println(time.Now())
	fmt.Print("我是" + strconv.Itoa(rf.me) + "在投票选举中")
	if !ok {
		fmt.Println(strconv.Itoa(server) + "断线了")

	}
	if reply.VoteGranted {
		fmt.Println(strconv.Itoa(server) + "的投票同意")
	} else {
		fmt.Println(strconv.Itoa(server) + "的投票不同意")
	}
	result := ChannelResult{
		Voted:     reply.VoteGranted,
		Term:      reply.Term,
		Index:     server,
		Connected: ok,
	}
	resultChannel <- result
}

type ChannelResult struct {
	Voted     bool
	Term      int
	Index     int
	Connected bool
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

	// Your code here (2B).

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
	rf.currentTerm = 0
	rf.votedTerm = 0
	rf.hasHeartBeat = false
	rf.status = 0
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// The ticker go routine starts a new election if this peer hasn't received
// heartsbeats recently.
func (rf *Raft) ticker() {
	for rf.killed() == false {
		r := rand.New(rand.NewSource(time.Now().UnixNano()))
		// Your code here to check if a leader election should
		// be started and to randomize sleeping time using
		// time.Sleep().
		sleepTime := time.Duration(r.Float32()*400+350) * time.Millisecond
		time.Sleep(sleepTime)
		if rf.killed() {
			break
		}
		rf.heartBeatMu.Lock()
		if !rf.hasHeartBeat {

			rf.currentTerm = rf.currentTerm + 1
			fmt.Println(time.Now())
			fmt.Println(strconv.Itoa(rf.me) + "选择" + "发起选举" + "term是" + strconv.Itoa(rf.currentTerm))
			rf.statusMu.Lock()
			rf.status = 1
			rf.statusMu.Unlock()

			go rf.StartRequestVote()
		}
		rf.hasHeartBeat = false
		rf.heartBeatMu.Unlock()

	}
}

func (rf *Raft) SendHeartBeat() {
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		if rf.killed() {
			break
		}
		args := &AppendEntriesArgs{
			Term: rf.currentTerm,
		}
		reply := &AppendEntriesReply{}
		ok := rf.peers[i].Call("Raft.ReceiveInstructRPC", args, reply)
		if !ok {
			//fmt.Println("我是" + strconv.Itoa(rf.me) + "id是" + strconv.Itoa(i) + "的服从者已经下线")
		}
		//fmt.Println("发起心跳检测的领导者的id是" + strconv.Itoa(rf.me) + "term是" + strconv.Itoa(rf.currentTerm) + "收到的id是" + strconv.Itoa(i) + "term是" + strconv.Itoa(reply.Term))
		if reply.Term > rf.currentTerm {
			fmt.Println("领导下线")
			rf.statusMu.Lock()
			rf.status = 0
			rf.statusMu.Unlock()
			rf.currentTerm = reply.Term
			return
		}
	}
}

// 根据状态发起心跳检测
func (rf *Raft) HeartBeatsCheck() {
	for rf.killed() == false {
		_, isLeader := rf.GetState()
		if isLeader {
			fmt.Println(strconv.Itoa(rf.me) + "发起了心跳检测")
			rf.heartBeatMu.Lock()
			rf.hasHeartBeat = true
			rf.heartBeatMu.Unlock()
			go rf.SendHeartBeat()
		}
		time.Sleep(150 * time.Millisecond)
	}

}

func (rf *Raft) StartRequestVote() {
	count := 1
	rf.votedMu.Lock()
	rf.votedFor = rf.me
	rf.hasVoted = true
	rf.votedMu.Unlock()
	result := make(chan ChannelResult, len(rf.peers))
	fmt.Println(strconv.Itoa(rf.me) + "开始发起选举")
	for i := 0; i < len(rf.peers); i++ {
		if i == rf.me {
			continue
		}
		fmt.Println("向" + strconv.Itoa(i) + "发起投票请求")
		args := &RequestVoteArgs{
			Term:         rf.currentTerm,
			CandidateId:  rf.me,
			LastLogIndex: rf.lastApplied,
			LastLogTerm:  rf.currentTerm - 1,
		}
		reply := &RequestVoteReply{}
		go rf.sendRequestVote(i, args, reply, result)
	}
	time.Sleep(300 * time.Millisecond)
	connectedNum := 1
	for i := 0; i < len(rf.peers)-1; i++ {
		ch := <-result

		if ch.Connected {
			fmt.Println(strconv.Itoa(ch.Index) + "连接了")
			connectedNum++
		} else {
			fmt.Println(strconv.Itoa(ch.Index) + "没连接")
		}
		if ch.Voted {
			count++
		}
	}
	fmt.Println(strconv.Itoa(count) + ":" + strconv.Itoa(connectedNum))
	if count > (connectedNum / 2) {
		rf.statusMu.Lock()
		rf.status = 2
		rf.statusMu.Unlock()
		rf.votedMu.Lock()
		rf.hasVoted = false
		rf.votedMu.Unlock()
		fmt.Println(strconv.Itoa(rf.me) + "成为领导者, term 是" + strconv.Itoa(rf.currentTerm))
		rf.heartBeatMu.Lock()
		rf.hasHeartBeat = true
		rf.heartBeatMu.Unlock()
		rf.SendHeartBeat()
	} else {
		rf.statusMu.Lock()
		rf.status = 0
		rf.statusMu.Unlock()
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

	// Your initialization code here (2A, 2B, 2C).

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	// start ticker goroutine to start elections
	go rf.ticker()
	go rf.HeartBeatsCheck()
	return rf
}
