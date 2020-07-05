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
	"bytes"
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
)

// import "bytes"
// import "../labgob"

//
// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
//
type ApplyMsg struct {
	CommandValid bool
	Command      interface{}
	CommandIndex int
}

type LogEntry struct {
	Term    int
	Command interface{}
}

type State string

const (
	Leader    State = "leader"
	Candidate       = "candidate"
	Follower        = "follower"
)

//
// Time related constants
//
const (
	BroardcastTime            = time.Millisecond * 100
	ResendRequestTime         = time.Millisecond * 300
	ElectionTimeOutLowerBound = time.Millisecond * 400
	ElectionTimeOutUpperBound = time.Millisecond * 600
)

//
// Raft ...
// A Go object implementing a single Raft peer.
//
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	state           State
	currentTerm     int
	votedFor        int
	log             []LogEntry
	commitIndex     int
	lastApplied     int
	nextIndex       []int
	matchIndex      []int
	electionTimeOut *time.Timer
	heartBeatTimer  *time.Timer
	testTimer       *time.Timer

	applyCh chan ApplyMsg
	// HeartBeatCond       *sync.Cond
	CheckMatchIndexCond *sync.Cond
	// stopLeaderCh        chan struct{}
	// argCh   chan AppendEntriesArgs
	// replyCh chan AppendEntriesReply
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {

	var term int
	var isleader bool
	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	isleader = rf.state == Leader
	term = rf.currentTerm
	return term, isleader
}

//
// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
//
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

//
// restore previously persisted state.
//
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var term int
	var voteFor int
	var logs []LogEntry
	if d.Decode(&term) != nil ||
		d.Decode(&voteFor) != nil ||
		d.Decode(&logs) != nil {
		log.Fatal("Cannot read persist")
	} else {
		rf.currentTerm = term
		rf.votedFor = voteFor
		rf.log = logs
	}
}

func (rf *Raft) getLastLogTermIndex() (int, int) {
	term := rf.log[len(rf.log)-1].Term
	index := len(rf.log) - 1
	return term, index
}

//
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
//
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()
	// DPrintf("[%d] received command %+v", rf.me, command)
	if rf.state != Leader {
		isLeader = false
		return index, term, isLeader
	}
	// DPrintf("[%d] received command %+v", rf.me, command)
	term = rf.currentTerm
	index = len(rf.log)
	newLog := LogEntry{
		Term:    rf.currentTerm,
		Command: command,
	}
	rf.log = append(rf.log, newLog)
	rf.matchIndex[rf.me] = rf.nextIndex[rf.me]
	rf.nextIndex[rf.me]++
	rf.persist()
	// for server := range rf.peers {
	// 	if server == rf.me {
	// 		continue
	// 	}
	// 	// DPrintf("[%d] send log to server: %d", rf.me, server)
	// 	go rf.startSendAppendEntries(server, rf.currentTerm)
	// }
	// rf.heartBeatTimer.Reset(BroardcastTime)
	// DPrintf("[%d] log: %+v", rf.me, rf.log)
	// DPrintf("[%d] reset heartBeatTimer", rf.me)
	return index, term, isLeader
}

//
// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
//
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

//
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

	// Your initialization code here (2A, 2B, 2C).
	rf.applyCh = applyCh
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = make([]LogEntry, 1)
	// rf.stopLeaderCh = make(chan struct{})
	// rf.argCh = make(chan AppendEntriesArgs)
	// rf.replyCh = make(chan AppendEntriesReply)
	rf.commitIndex = 0
	rf.lastApplied = 0
	rf.nextIndex = []int{}
	rf.matchIndex = []int{}
	rf.testTimer = time.NewTimer(BroardcastTime * 10)
	rf.state = Follower
	rf.electionTimeOut = time.NewTimer(getElectionTimeOut())
	rf.heartBeatTimer = time.NewTimer(BroardcastTime)
	// rf.HeartBeatCond = sync.NewCond(&rf.mu)
	rf.CheckMatchIndexCond = sync.NewCond(&rf.mu)
	DPrintf("[%d] raft server created", rf.me)
	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.startRoutine()
	return rf
}

func (rf *Raft) startRoutine() {
	// start election after timeout
	go func() {
		for !rf.killed() {
			<-rf.electionTimeOut.C
			rf.mu.Lock()
			// DPrintf("[%d] reset the election timeout: startElection", rf.me)
			rf.electionTimeOut.Reset(getElectionTimeOut())
			go rf.startElection()
			rf.mu.Unlock()
		}
	}()

	// check matchIndex and decide to send commit
	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			// rf.CheckMatchIndexCond.Wait()
			for index := rf.commitIndex + 1; index < len(rf.log); index++ {
				result := 0
				for _, i := range rf.matchIndex {
					if i >= index {
						result++
					}
				}
				//DPrintf("[%d] index %d result: %d log len: %d", rf.me, index, result, len(rf.log))
				if result > len(rf.peers)/2 && rf.log[index].Term == rf.currentTerm {
					if index < len(rf.log) {
						rf.commitIndex = index
					} else {
						rf.commitIndex = len(rf.log) - 1
					}

					//DPrintf("[%d] try to commit: %d", rf.me, rf.commitIndex)
				}
			}
			rf.mu.Unlock()
		}
	}()

	// check commitIndex > lastApplied
	go func() {
		for !rf.killed() {
			if rf.commitIndex > rf.lastApplied {
				rf.lastApplied++
				// DPrintf("[%d] log: %+v", rf.me, rf.log)
				// DPrintf("[%d] as %s applied: %d %+v", rf.me, rf.state, rf.lastApplied, rf.log[rf.lastApplied])
				// apply to state machine
				rf.applyCh <- ApplyMsg{
					CommandValid: true,
					CommandIndex: rf.lastApplied,
					Command:      rf.log[rf.lastApplied].Command,
				}
			}
			time.Sleep(time.Millisecond * 10)
		}
	}()

	// go func() {
	// 	for !rf.killed() {
	// 		<-rf.testTimer.C
	// 		rf.mu.Lock()
	// 		rf.testTimer.Reset(BroardcastTime * 10)
	// 		DPrintf("[%d] is alive", rf.me)
	// 		rf.mu.Unlock()
	// 	}
	// }()
}
