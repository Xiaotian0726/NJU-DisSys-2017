package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, Term, isLeader)
//   start agreement on a new log entry
// rf.GetState() (Term, isLeader)
//   ask a Raft for its current Term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"bytes"
	"encoding/gob"
	"labrpc"
	"math/rand"
	"sync"
	"time"
)

const (
	Follower  = 0
	Candidate = 1
	Leader    = 2

	MinElectionTimeout = 150
	MaxElectionTimeout = 300

	HeartBeatsTimer = time.Duration(50) * time.Millisecond
)

func stateMap(state int) string {
	switch state {
	case Follower:
		return "Follower"
	case Candidate:
		return "Candidate"
	case Leader:
		return "Leader"
	}
	return "Unknown"
}

func randTimeout() time.Duration {
	return time.Duration(MinElectionTimeout+rand.Intn(MaxElectionTimeout-MinElectionTimeout)) * time.Millisecond
}

// ApplyMsg
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make().
type ApplyMsg struct {
	Index       int
	Command     interface{}
	UseSnapshot bool   // ignore for lab2; only used in lab3
	Snapshot    []byte // ignore for lab2; only used in lab3
}

type Entry struct {
	Index   int
	Term    int
	Command interface{}
}

// Raft
// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex
	peers     []*labrpc.ClientEnd
	persister *Persister
	me        int // index into peers[]

	// Your data here.
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state int

	currentTerm int
	voteFor     int
	log         []Entry

	commitIndex int
	lastApplied int

	nextIndex  []int
	matchIndex []int

	HeartBeatChan          chan int
	Candidate2FollowerChan chan int
	Leader2FollowerChan    chan int

	commitChan chan int

	electionTimer *time.Timer
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool

	// Your code here.
	term = rf.currentTerm
	isLeader = rf.state == Leader

	return term, isLeader
}

// persist
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here.
	w := new(bytes.Buffer)
	e := gob.NewEncoder(w)

	_ = e.Encode(rf.currentTerm)
	_ = e.Encode(rf.voteFor)
	_ = e.Encode(rf.log)

	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// readPersist
// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	// Your code here.
	r := bytes.NewBuffer(data)
	d := gob.NewDecoder(r)

	_ = d.Decode(&rf.currentTerm)
	_ = d.Decode(&rf.voteFor)
	_ = d.Decode(&rf.log)
}

// RequestVoteArgs
// example RequestVote RPC arguments structure.
type RequestVoteArgs struct {
	// Your data here.
	Term         int
	CandidateId  int
	LastLogIndex int
	LastLogTerm  int
}

// RequestVoteReply
// example RequestVote RPC reply structure
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

type AppendEntriesArgs struct {
	Term         int
	LeaderId     int
	PrevLogIndex int // index of log entry immediately preceding new ones
	PrevLogeTerm int // term of prevLogIndex entry
	Entries      []Entry
	LeaderCommit int // leader’s commitIndex
}

type AppendEntriesReply struct {
	Term      int
	Success   bool
	NextIndex int
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.
	rf.mu.Lock()
	defer rf.mu.Unlock()
	toFollower := false
	reply.VoteGranted = false

	if rf.currentTerm > args.Term {
		reply.Term = rf.currentTerm
		// "bp" means "break point"
		_, _ = DPrintf("Warn", "[RequestVote bp1] Server %d VoteGranted for Server %d - %t in Term %d\n", rf.me, args.CandidateId, reply.VoteGranted, reply.Term)
		return
	}

	if rf.currentTerm < args.Term {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
		toFollower = true
	}
	reply.Term = rf.currentTerm
	if rf.state == Leader && !toFollower {
		reply.VoteGranted = false
		_, _ = DPrintf("Warn", "[RequestVote bp2] Server %d VoteGranted for Server %d - %t in Term %d\n", rf.me, args.CandidateId, reply.VoteGranted, reply.Term)
		return
	}
	if rf.state == Candidate && toFollower {
		rf.Candidate2FollowerChan <- 1
	}
	if rf.state == Leader && toFollower {
		rf.Leader2FollowerChan <- 1
	}

	if (args.LastLogTerm < rf.log[len(rf.log)-1].Term) || ((args.LastLogTerm == rf.log[len(rf.log)-1].Term) && (args.LastLogIndex < rf.log[len(rf.log)-1].Index)) {
		reply.VoteGranted = false
		rf.persist()
	} else if rf.voteFor == -1 || rf.voteFor == args.CandidateId {
		rf.voteFor = args.CandidateId
		reply.VoteGranted = true
		rf.persist()
		rf.electionTimer.Reset(randTimeout())
	}

	_, _ = DPrintf("Warn", "[RequestVote bp3] Server %d VoteGranted for Server %d - %t in Term %d\n", rf.me, args.CandidateId, reply.VoteGranted, reply.Term)
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Success = false

	if args.Term < rf.currentTerm {
		reply.Term = rf.currentTerm
		reply.NextIndex = rf.log[len(rf.log)-1].Index + 1
		return
	}

	if rf.state == Candidate && args.LeaderId != rf.me && args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.state = Follower
		rf.Candidate2FollowerChan <- 1
		rf.persist()
	}
	if rf.state == Leader && args.LeaderId != rf.me && args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.currentTerm = -1
		rf.state = Follower
		rf.Leader2FollowerChan <- 1
		rf.persist()
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.voteFor = -1
		rf.persist()
	}
	rf.electionTimer.Reset(randTimeout())
	reply.Term = rf.currentTerm

	if args.PrevLogIndex > len(rf.log)-1 {
		reply.NextIndex = len(rf.log)
		return
	}

	term := rf.log[args.PrevLogIndex].Term
	if args.PrevLogeTerm != term {
		for i := args.PrevLogIndex - 1; i >= 0; i-- {
			if rf.log[i].Term != term {
				reply.NextIndex = i + 1
				break
			}
		}
		return
	}

	rf.log = rf.log[:args.PrevLogIndex+1]
	rf.log = append(rf.log, args.Entries...)
	rf.persist()
	reply.Success = true
	reply.NextIndex = rf.log[len(rf.log)-1].Index

	if args.LeaderCommit > rf.commitIndex {
		last := rf.log[len(rf.log)-1].Index
		if args.LeaderCommit > last {
			rf.commitIndex = last
		} else {
			rf.commitIndex = args.LeaderCommit
		}
		rf.commitChan <- 1
	}
	return
}

// sendRequestVote
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
func (rf *Raft) sendRequestVote(server int, args RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
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
// Term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	if rf.state != Leader {
		return index, term, false
	}

	index = rf.log[len(rf.log)-1].Index + 1
	entry := Entry{index, term, command}
	rf.log = append(rf.log, entry)
	_, _ = DPrintf("Info", "[Start bp1] Server %d (state: %s) append entry {index: %d, term: %d, command} to log\n", rf.me, stateMap(rf.state), index, term)
	rf.persist()

	rf.HeartBeatChan <- 1

	return index, term, true
}

// Kill
// the tester calls Kill() when a Raft instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (rf *Raft) Kill() {
	// Your code here, if desired.
}

func (rf *Raft) follower() {
	_, _ = DPrintf("Info", "[follower bp1] Server %d, Term %d\n", rf.me, rf.currentTerm)
	for {
		if rf.state == Candidate {
			break
		}
		select {
		case <-rf.electionTimer.C:
			rf.currentTerm += 1
			rf.voteFor = -1
			rf.state = Candidate
			rf.electionTimer.Reset(randTimeout())
		}
	}
}

func (rf *Raft) candidate() {
	_, _ = DPrintf("Info", "[candidate bp1] Server %d, Term %d\n", rf.me, rf.currentTerm)
	rf.voteFor = rf.me
	count := 1
	n := len(rf.peers)
	success := make(chan int)

	go func() {
		reply := make(chan *RequestVoteReply)
		for i := 0; i < n; i++ {
			if i != rf.me {
				go func(i int) {
					var tReply *RequestVoteReply
					ok := rf.peers[i].Call("Raft.RequestVote", RequestVoteArgs{rf.currentTerm, rf.me, rf.log[len(rf.log)-1].Index, rf.log[len(rf.log)-1].Term}, &tReply)
					if ok {
						reply <- tReply
					}
					return
				}(i)
			}
		}
		for {
			rep := <-reply
			if rep.Term > rf.currentTerm {
				rf.state = Follower
				rf.voteFor = -1
				rf.currentTerm = rep.Term
				rf.persist()
				rf.Candidate2FollowerChan <- 1
				return
			}
			if rep.VoteGranted {
				count += 1
				if count > n/2 {
					success <- 1
					return
				}
			}
		}
	}()

	select {
	case <-rf.Candidate2FollowerChan:
		rf.state = Follower
		_, _ = DPrintf("Info", "[candidate bp3] Server %d become Follower in Term %d\n", rf.me, rf.currentTerm)
		return
	case <-success:
		rf.mu.Lock()
		rf.state = Leader
		rf.voteFor = -1
		_, _ = DPrintf("Info", "[candidate bp2] Server %d become Leader in Term %d, state %d\n", rf.me, rf.currentTerm, rf.state)

		rf.persist()
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := range rf.peers {
			rf.nextIndex[i] = rf.log[len(rf.log)-1].Index + 1
			rf.matchIndex[i] = 0
		}
		rf.mu.Unlock()
		return
	case <-rf.electionTimer.C:
		rf.electionTimer.Reset(randTimeout())
		rf.currentTerm += 1
		rf.voteFor = -1
		rf.persist()
		return
	}
}

func (rf *Raft) leader() {
	_, _ = DPrintf("Info", "[leader bp1] Server %d, Term %d\n", rf.me, rf.currentTerm)
	nPeers := len(rf.peers)
	rf.mu.Lock()
	rf.nextIndex = make([]int, nPeers)
	rf.matchIndex = make([]int, nPeers)

	// When a leader first comes to power, it initializes all nextIndex values to the index just after the last one in its log
	for i := 0; i < nPeers; i++ {
		rf.nextIndex[i] = rf.lastApplied + 1
		rf.matchIndex[i] = 0
	}
	rf.persist()
	rf.mu.Unlock()

	heartBeatTicker := time.NewTicker(HeartBeatsTimer)
	defer heartBeatTicker.Stop()

	go func() {
		for range heartBeatTicker.C {
			rf.HeartBeatChan <- 1
		}
	}()

	for {
		select {
		case <-rf.Leader2FollowerChan:
			rf.state = Follower
			rf.voteFor = -1
			rf.persist()
			return
		case <-rf.HeartBeatChan:
			n := len(rf.peers)
			base := rf.log[0].Index
			commit := rf.commitIndex
			last := rf.log[len(rf.log)-1].Index
			for i := rf.commitIndex + 1; i <= last; i++ {
				num := 1
				for j := range rf.peers {
					if j != rf.me && rf.matchIndex[j] >= i && rf.log[i-base].Term == rf.currentTerm {
						num += 1
					}
				}
				// A log entry is committed once the leader that created the entry has replicated it on a majority of the servers
				if num*2 > len(rf.peers) {
					commit = i
				}
			}
			if commit != rf.commitIndex {
				rf.commitIndex = commit
				rf.commitChan <- 1
			}
			for i := 0; i < n; i++ {
				if i != rf.me {
					var reply *AppendEntriesReply
					var args AppendEntriesArgs
					args.Term = rf.currentTerm
					args.LeaderId = rf.me
					args.PrevLogIndex = rf.nextIndex[i] - 1
					if args.PrevLogIndex > len(rf.log)-1 {
						args.PrevLogIndex = len(rf.log) - 1
					}
					args.PrevLogeTerm = rf.log[args.PrevLogIndex].Term
					args.LeaderCommit = rf.commitIndex
					if rf.nextIndex[i] > base {
						args.Entries = make([]Entry, len(rf.log[args.PrevLogIndex+1:]))
						copy(args.Entries, rf.log[args.PrevLogIndex+1:])
					}
					go func(i int, args AppendEntriesArgs) {
						if rf.peers[i].Call("Raft.AppendEntries", args, &reply) {
							if reply.Success {
								if len(args.Entries) > 0 {
									rf.mu.Lock()
									rf.nextIndex[i] = args.Entries[len(args.Entries)-1].Index + 1
									rf.matchIndex[i] = rf.nextIndex[i] - 1
									rf.mu.Unlock()
								}
							} else {
								rf.nextIndex[i] = reply.NextIndex
							}
						}
					}(i, args)
				}
			}
		}
	}
}

func (rf *Raft) init() {
	rf.state = Follower

	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}

	rf.Leader2FollowerChan = make(chan int)
	rf.Candidate2FollowerChan = make(chan int)
	rf.HeartBeatChan = make(chan int)
	rf.commitChan = make(chan int)
	rf.electionTimer = time.NewTimer(randTimeout())

	// initialize from state persisted before a crash
	rf.electionTimer.Reset(randTimeout())
	rf.log = append(rf.log, Entry{Term: 0})
}

func (rf *Raft) applyCommitted(applyCh chan ApplyMsg) {
	for range rf.commitChan {
		for i := rf.lastApplied + 1; i <= rf.commitIndex && i < len(rf.log); i++ {
			msg := ApplyMsg{Index: i, Command: rf.log[i].Command}
			applyCh <- msg
			rf.lastApplied = i
		}
		continue
	}
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
func Make(peers []*labrpc.ClientEnd, me int, persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me

	rf.init()
	rf.readPersist(persister.ReadRaftState())

	go func() {
		rf.applyCommitted(applyCh)
	}()

	go func() {
		for {
			switch rf.state {
			case Follower:
				rf.follower()
			case Candidate:
				rf.candidate()
			case Leader:
				rf.leader()
			}
		}
	}()

	return rf
}
