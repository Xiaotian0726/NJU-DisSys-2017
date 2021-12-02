package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	Leader = "Leader"
	Candidate = "Candidate"
	Follower = "Follower"
	votedNull = -1
	heartBeat = time.Duration(500) * time.Millisecond
)

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
//
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   	  	// ignore for lab2; only used in lab3
	Snapshot    []byte 	  	// ignore for lab2; only used in lab3
}

// Raft
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state string

	currentTerm int
	votedFor    int
	log         []LogEntry

	commitIndex int
	lastApplied int

	nextIndex  []int    // for each server, index of the next log entry to send to that server (initialized to leader last log index + 1)
	matchIndex []int    // for each server, index of the highest log entry known to be replicated on server (initialized to 0, increases monotonically)

	appendCh chan *AppendEntryArgs    // 判断在 election timeout 时间内有没有收到心跳信号
	voteCh   chan *RequestVoteArgs

	applyCh chan ApplyMsg

	printLog bool

	electionTimeout *time.Timer
	heartBeat       int
}

func (rf *Raft) changeState(state string) {
	switch state {
	case Leader:
		rf.state = Leader
		DPrintf(rf.printLog, "warn", "me: %2d term: %3d | %s change state to Leader!\n", rf.me, rf.currentTerm, rf.state)
	case Candidate:
		rf.currentTerm = rf.currentTerm+1
		rf.state = Candidate
		DPrintf(rf.printLog, "warn", "me: %2d term: %3d | %s change state to candidate!\n", rf.me, rf.currentTerm, rf.state)
	case Follower:
		rf.state = Follower
		DPrintf(rf.printLog, "warn", "me: %2d term: %3d | %s change state to follower!\n", rf.me, rf.currentTerm, rf.state)
	}
}

func (rf *Raft) randTime() int {
	basicTime := 400
	randNum := 150
	timeout := basicTime + rand.Intn(randNum)
	return timeout
}

func (rf *Raft) resetTimeout() {
	if rf.electionTimeout == nil {
		rf.electionTimeout = time.NewTimer(time.Duration(rf.randTime()) * time.Millisecond)
	} else {
		rf.electionTimeout.Reset(time.Duration(rf.randTime()) * time.Millisecond)
	}
}

func (rf *Raft) leader() {
	rf.mu.Lock()
	lastLogIndex := len(rf.log)-1
	currTerm := rf.currentTerm
	rf.mu.Unlock()

	for followerId, _ := range rf.peers {
		if followerId == rf.me {
			continue
		}
		rf.nextIndex[followerId] = lastLogIndex+1
		rf.matchIndex[followerId] = 0
	}

	for {
		commitFlag := true
		commitNum := 1
		commitL := sync.Mutex{}

		for followerId, _ := range rf.peers {
			if followerId == rf.me {
				continue
			}

			rf.mu.Lock()
			newEntries := make([]LogEntry, 0)
			appendArgs := &AppendEntryArgs{
				currTerm,
				rf.me,
				rf.nextIndex[followerId]-1,
				rf.log[rf.nextIndex[followerId]-1].Term,
				newEntries,
				rf.commitIndex}
			rf.mu.Unlock()

			// goroutine for a follower
			go func(server int) {
				reply := &AppendEntryReply{}
				DPrintf(rf.printLog, "info", "me: %2d term: %3d | Leader send message to %2d\n", rf.me, currTerm, server)
				if ok := rf.peers[server].Call("Raft.AppendEntry", *appendArgs, reply); ok {
					rf.mu.Lock()
					if rf.currentTerm != currTerm {
						rf.mu.Unlock()
						return
					}
					rf.mu.Unlock()
					
					if reply.Success {
						commitL.Lock()
						commitNum += 1
						if commitFlag && commitNum > len(rf.peers)/2 {
							commitFlag = true
						}
						commitL.Unlock()
					} else {
						rf.mu.Lock()
						if reply.Term > currTerm {
							rf.currentTerm = reply.Term
							rf.mu.Unlock()
							rf.changeState(Follower)
						} else {
							rf.nextIndex[server] = rf.nextIndex[server]-1
							rf.mu.Unlock()
						}
					}
				}
			} (followerId)
		}

		select {
		case <- rf.appendCh:
			rf.changeState(Follower)
			return
		case <- rf.voteCh:
			rf.changeState(Follower)
			return
		case <- time.After(time.Duration(rf.heartBeat) * time.Millisecond):
			//do nothing
		}
	}
}

func (rf *Raft) candidate() {
	rf.mu.Lock()
	rf.votedFor = rf.me
	lastLogIndex := len(rf.log)-1
	requestArgs := &RequestVoteArgs{rf.currentTerm, rf.me, lastLogIndex, rf.log[lastLogIndex].Term}
	rf.mu.Unlock()

	voteCnt := 1
	voteFlag := true
	voteOk := make(chan bool)
	voteL := sync.Mutex{}

	rf.resetTimeout()

	for followerId, _ := range rf.peers {
		if followerId == rf.me {
			continue
		}

		// goroutine for a follower
		go func(server int) {
			reply := &RequestVoteReply{}
			if ok := rf.sendRequestVote(server, *requestArgs, reply); ok {
				if reply.VoteGranted {
					voteL.Lock()
					voteCnt += 1
					if voteFlag && voteCnt > len(rf.peers)/2 {
						voteFlag = false
						voteL.Unlock()
						voteOk <- true
					} else {
						voteL.Unlock()
					}
				}
			}
		} (followerId)
	}

	select {
	case args := <- rf.appendCh:
		DPrintf(rf.printLog, "info", "me: %2d term: %3d | receive heartbeat from Leader %2d\n", rf.me, rf.currentTerm, args.LeaderId)
		rf.changeState(Follower)
		return
	case args := <- rf.voteCh:
		DPrintf(rf.printLog, "warn", "me: %2d term: %3d | as a %s vote to Candidate %2d\n", rf.me, rf.currentTerm, rf.state, args.CandidateId)
		rf.changeState(Follower)
		return
	case <- voteOk:
		rf.changeState(Leader)
		return
	case <- rf.electionTimeout.C:
		DPrintf(rf.printLog, "warn", "me: %2d term: %3d | Candidate timeout!\n", rf.me, rf.currentTerm)
		rf.changeState(Follower)
		return
	}
}

func (rf *Raft) follower() {
	for {
		rf.resetTimeout()
		select {
		case args := <- rf.appendCh:    // 收到心跳信息
			DPrintf(rf.printLog, "info", "me: %2d term: %3d | receive heartbeat from Leader %2d\n", rf.me, rf.currentTerm, args.LeaderId)
		case args := <-rf.voteCh:
			DPrintf(rf.printLog, "warn", "me: %2d term: %3d | as a %s vote to Candidate %2d\n", rf.me, rf.currentTerm, rf.state, args.CandidateId)
		case <- rf.electionTimeout.C:  	// 超时
			DPrintf(rf.printLog, "warn", "me: %2d term: %3d | Follower timeout!\n", rf.me, rf.currentTerm)
			rf.changeState(Candidate)
			return
		}
	}
}



// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isLeader bool

	// Your code here.
	rf.mu.Lock()
	term = rf.currentTerm
	isLeader = rf.state == Leader
	rf.mu.Unlock()

	return term, isLeader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here.
	// Example:
	// w := new(bytes.Buffer)
	// e := gob.NewEncoder(w)
	// e.Encode(rf.xxx)
	// e.Encode(rf.yyy)
	// data := w.Bytes()
	// rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	// Example:
	// r := bytes.NewBuffer(data)
	// d := gob.NewDecoder(r)
	// d.Decode(&rf.xxx)
	// d.Decode(&rf.yyy)
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
//
type RequestVoteArgs struct {
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
//
type RequestVoteReply struct {
	Term        int
	VoteGranted bool
}

// RequestVote
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.VoteGranted = false
	} else if rf.currentTerm < args.Term {
		lastLogIndex := len(rf.log)-1
		if args.LastLogIndex >= lastLogIndex && args.LastLogTerm >= rf.log[lastLogIndex].Term {
			rf.votedFor = args.CandidateId
			rf.currentTerm = args.Term
			reply.Term = rf.currentTerm
			reply.VoteGranted = true

			go func() {rf.voteCh <- &args}()
		}
	}
}

//
// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// returns true if labrpc says the RPC was delivered.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
//
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

type LogEntry struct {
	Command interface{}
	Term    int
}

type AppendEntryArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int
	PrevLogTerm  int
	Entries      []LogEntry
	LeaderCommit int
}

type AppendEntryReply struct {
	Term    int
	Success bool
}

// AppendEntry
// 站在 follower 角度完成下面这个 RPC 调用
func (rf *Raft) AppendEntry(args AppendEntryArgs, reply *AppendEntryReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		reply.Success = false
		return
	}

	lastLogIndex := len(rf.log)-1
	logLess := lastLogIndex < args.PrevLogIndex
	logMismatch := !logLess && args.PrevLogIndex > 0 && rf.log[args.PrevLogIndex].Term != args.PrevLogTerm
	if logLess || logMismatch {
		reply.Term = rf.currentTerm
		reply.Success = false
		DPrintf(rf.printLog, "info", "me: %2d term: %3d | receive Leader: [%2d] message but not match!\n", rf.me, rf.currentTerm, args.LeaderId)
	} else {
		// 接收者的日志大于等于 PrevLogIndex
		rf.currentTerm = args.Term
		reply.Success = true
		// 修改日志长度
		// 找到接收者和 leader（如果有）第一个不相同的日志
		length := Min(lastLogIndex-args.PrevLogIndex, len(args.Entries))
		i := 0
		for ; i < length; i++ {
			if rf.log[args.PrevLogIndex+i+1].Term != args.Entries[i].Term {
				rf.log = rf.log[:args.PrevLogIndex+i+1]
				break
			}
		}
		if i != len(args.Entries) {
			rf.log = append(rf.log, args.Entries[i:]...)
		}

		//修改commitIndex
		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = Min(args.LeaderCommit, len(rf.log)-1)
		}
		DPrintf(rf.printLog, "info", "me: %2d term: %3d | receive new entries from Leader: %2d, size: %3d\n", rf.me, rf.currentTerm, args.LeaderId, len(args.Entries))
	}
	// 即便日志不匹配，但是也算是接收到了来自 leader 的日志。
	go func() { rf.appendCh <- &args }()
}

// Start
// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false, otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	return index, term, isLeader
}

func (rf *Raft) run() {
	for {
		switch rf.state {
		case Leader:
			rf.leader()
		case Candidate:
			rf.candidate()
		case Follower:
			rf.follower()
		}
	}
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

// Make
// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
//
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	// Your initialization code here.
	rf.currentTerm = -1
	rf.votedFor = -1
	rf.state = Follower

	// log下标从 1 开始，0 是占位符
	rf.log = make([]LogEntry, 1)

	// term为 -1，第一个 leader 的 term 编号是 0，test 只接受 int 型的 command
	rf.log[0] = LogEntry{-1, -1}
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = make([]int, len(peers))
	rf.matchIndex = make([]int, len(peers))

	rf.appendCh = make(chan *AppendEntryArgs)
	rf.voteCh = make(chan *RequestVoteArgs)

	rf.printLog = true

	rand.Seed(time.Now().UnixNano())
	rf.heartBeat = 100

	DPrintf(rf.printLog, "info", "Create a new server: [%2d]! term: [%3d]\n", rf.me,rf.currentTerm)

	go rf.run()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	return rf
}
