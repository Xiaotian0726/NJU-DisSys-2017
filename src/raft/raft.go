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
	"math/rand"
	"sync"
	"time"
)
import "labrpc"

// import "bytes"
// import "encoding/gob"

const (
	FOLLOWER  = 0
	CANDIDATE = 1
	LEADER    = 2

	MinElectionTimeout = 150 // ms
	MaxElectionTimeout = 300 // ms

	HeartBeatTimeout = 50 // ms
)

// resetElectionTimer
func (rf *Raft) resetElectionTimer() {
	randElectionTimeout := rand.Intn(MaxElectionTimeout-MinElectionTimeout) + MinElectionTimeout

	if rf.electionTimer == nil {
		rf.electionTimer = time.NewTimer(time.Duration(randElectionTimeout) * time.Millisecond)
	} else {
		rf.electionTimer.Reset(time.Duration(randElectionTimeout) * time.Millisecond)
	}
}

// stateMap
func stateMap(state int) string {
	switch state {
	case FOLLOWER:
		return "Follower"
	case CANDIDATE:
		return "Candidate"
	case LEADER:
		return "Leader"
	}
	return "Unknown"
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
	currentTerm int // candidateId that received vote in current term (or null if none)
	votedFor    int

	commitIndex int
	lastApplied int
	nextIndex   []int
	matchIndex  []int

	state int

	HeartBeatChan          chan int
	Candidate2FollowerChan chan int
	Leader2FollowerChan    chan int

	electionTimer *time.Timer
}

// GetState
// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	term := rf.currentTerm
	isLeader := rf.state == LEADER
	return term, isLeader
}

func (rf *Raft) sendToFollowerSignal() {
	switch rf.state {
	case FOLLOWER:
		return
	case CANDIDATE:
		rf.Candidate2FollowerChan <- 1
	case LEADER:
		rf.Leader2FollowerChan <- 1
	}
}

// persist
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
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

// readPersist
// restore previously persisted state.
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
type RequestVoteArgs struct {
	// Your data here.
	Term        int
	CandidateId int
}

// RequestVoteReply
// example RequestVote RPC reply structure.
type RequestVoteReply struct {
	// Your data here.
	Term        int
	VoteGranted bool
}

// RequestVote
// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here.

	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, _ = DPrintf("Info", "[RequestVote] Server{id=%d, term=%d, %s} get RequestVote RPC from candidate{id=%d, term=%d}", rf.me, rf.currentTerm, stateMap(rf.state), args.CandidateId, args.Term)

	reply.Term = rf.currentTerm

	// 需要注意，此时接收 RequestVote RPC 的 Server 可能为三种状态中的任意一种
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
	} else if args.Term == rf.currentTerm {
		if rf.votedFor == -1 {
			rf.votedFor = args.CandidateId
			reply.VoteGranted = true
		} else {
			reply.VoteGranted = false
		}
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = args.CandidateId
		reply.VoteGranted = true
		rf.sendToFollowerSignal()
	}
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

type AppendEntriesArgs struct {
	Term     int
	LeaderId int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	_, _ = DPrintf("Info", "[AppendEntries] Server{id=%d, term=%d, %s} get AppendEntries RPC from Leader{id=%d, term=%d}", rf.me, rf.currentTerm, stateMap(rf.state), args.LeaderId, args.Term)

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
	} else if args.Term == rf.currentTerm {
		if rf.state == FOLLOWER {
			reply.Success = true
		} else if rf.state == CANDIDATE {
			reply.Success = true
			rf.sendToFollowerSignal()
			rf.votedFor = -1
		} else if rf.state == LEADER {
			reply.Success = false
		}
	} else if args.Term > rf.currentTerm {
		reply.Success = true
		rf.currentTerm = args.Term
		rf.sendToFollowerSignal()
		rf.votedFor = -1
	}
}

func (rf *Raft) sendAppendEntries(server int, args AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
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
// term. the third return value is true if this server believes it is
// the leader.
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	index := -1
	term := rf.currentTerm
	isLeader := rf.state == LEADER

	if isLeader {
		rf.HeartBeatChan <- 1
	}

	return index, term, isLeader
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
	for {
		select {
		case <-rf.electionTimer.C:
			rf.currentTerm++
			rf.state = CANDIDATE
			_, _ = DPrintf("Warn", "[follower()] Server %d, change state to CANDIDATE, term = %d", rf.me, rf.currentTerm)
			rf.votedFor = -1
			rf.resetElectionTimer()
			return
		}
	}
}

func (rf *Raft) candidate() {
	rf.votedFor = rf.me
	voteCnt := 1

	electionSuccessChan := make(chan int)
	replyChan := make(chan *RequestVoteReply)

	// send RequestVote RPC
	go func() {
		for i := 0; i < len(rf.peers); i++ {
			if i == rf.me {
				continue
			}
			go func(i int) {
				reply := &RequestVoteReply{}
				args := RequestVoteArgs{rf.currentTerm, rf.me}
				ok := rf.sendRequestVote(i, args, reply)
				if ok {
					replyChan <- reply
				}
			}(i)
		}
	}()

	// Receive RequestVoteReply
	go func() {
		for {
			reply := <-replyChan
			go func(reply *RequestVoteReply) {
				if reply.Term > rf.currentTerm {
					rf.Candidate2FollowerChan <- 1
					rf.currentTerm = reply.Term
					return
				}
				if reply.VoteGranted {
					voteCnt += 1
					if voteCnt > len(rf.peers)/2 {
						electionSuccessChan <- 1
					}
				}
			}(reply)
		}
	}()

	select {
	case <-electionSuccessChan:
		rf.state = LEADER
		_, _ = DPrintf("Warn", "[candidate()] Server %d, change state to LEADER, term = %d", rf.me, rf.currentTerm)
	case <-rf.electionTimer.C:
		rf.resetElectionTimer()
		rf.currentTerm += 1
		rf.votedFor = -1
	case <-rf.Candidate2FollowerChan:
		rf.state = FOLLOWER
		_, _ = DPrintf("Warn", "[candidate()] Server %d, change state to FOLLOWER, term = %d", rf.me, rf.currentTerm)
	}
}

func (rf *Raft) leader() {
	heartBeatTicker := time.NewTicker(HeartBeatTimeout)
	defer heartBeatTicker.Stop()

	go func() {
		for _ = range heartBeatTicker.C {
			rf.HeartBeatChan <- 1
		}
	}()

	for {
		select {
		case <-rf.HeartBeatChan:
			replyChan := make(chan AppendEntriesReply)

			// Send AppendEntries RPC
			go func() {
				args := AppendEntriesArgs{rf.currentTerm, rf.me}
				for i := 0; i < len(rf.peers); i++ {
					if i == rf.me {
						continue
					}
					go func(i int, args AppendEntriesArgs) {
						reply := &AppendEntriesReply{}
						ok := rf.sendAppendEntries(i, args, reply)
						if ok {
							replyChan <- *reply
						}
					}(i, args)
				}
			}()

			// Receive AppendEntriesReply
			go func() {
				for {
					reply := <-replyChan
					go func(reply AppendEntriesReply) {
						if reply.Term > rf.currentTerm {
							rf.currentTerm = reply.Term
							rf.votedFor = -1
							rf.sendToFollowerSignal()
							return
						}
					}(reply)
				}
			}()
		case <-rf.Leader2FollowerChan:
			rf.state = FOLLOWER
			_, _ = DPrintf("Warn", "[leader()] Server %d, change state to FOLLOWER, term = %d", rf.me, rf.currentTerm)
			return
		}
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

	// Your initialization code here.
	rf.state = FOLLOWER
	_, _ = DPrintf("Info", "[Make()] Server %d, init state = %s, term = %d", rf.me, stateMap(rf.state), rf.currentTerm)

	rf.HeartBeatChan = make(chan int)
	rf.Candidate2FollowerChan = make(chan int)
	rf.Leader2FollowerChan = make(chan int)

	rf.resetElectionTimer()

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())

	go func() {
		for {
			switch rf.state {
			case FOLLOWER:
				rf.follower()
			case CANDIDATE:
				rf.candidate()
			case LEADER:
				rf.leader()
			}
		}
	}()
	return rf
}
