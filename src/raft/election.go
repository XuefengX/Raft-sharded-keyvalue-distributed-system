package raft

import (
	"math/rand"
	"sync"
	"time"
)

//
// example RequestVote RPC arguments structure.
// field names must start with capital letters!
//
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int
	CandidateID  int
	LastLogIndex int
	LastLogTerm  int
}

//
// example RequestVote RPC reply structure.
// field names must start with capital letters!
//
type RequestVoteReply struct {
	// Your data here (2A).
	Term        int
	VoteGranted bool
}

//
// example RequestVote RPC handler.
//
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	//defer DPrintf("[%d] Send back vote result for %d: %+v", rf.me, args.CandidateID, reply)
	defer rf.mu.Unlock()
	// DPrintf("[%d] Received RequestVote RPC call: %+v", rf.me, args)
	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()
	//DPrintf("[%d] Current : %d %d", rf.me, lastLogTerm, lastLogIndex)
	reply.Term = rf.currentTerm
	reply.VoteGranted = false
	if args.Term < rf.currentTerm {
		return
	}
	if args.Term > rf.currentTerm {
		rf.changeState(Follower)
		rf.currentTerm = args.Term
		rf.votedFor = -1
	}

	grantVote := args.LastLogTerm > lastLogTerm || (args.LastLogTerm == lastLogTerm && args.LastLogIndex >= lastLogIndex)
	grantVote = grantVote && (rf.votedFor == -1 || rf.votedFor == args.CandidateID)

	if grantVote {
		rf.votedFor = args.CandidateID
		reply.VoteGranted = true
		reply.Term = rf.currentTerm
		// DPrintf("[%d] reset the election timeout: RequestVote", rf.me)
		rf.electionTimeOut.Reset(getElectionTimeOut())
	}
	rf.persist()
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
//
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

func getElectionTimeOut() time.Duration {
	electionTimeOutDuration := ElectionTimeOutUpperBound - ElectionTimeOutLowerBound
	return ElectionTimeOutLowerBound + time.Duration(rand.Int63())%electionTimeOutDuration
}

func (rf *Raft) changeState(state State) {
	switch state {
	case Leader:
		if rf.state == Leader {
			return
		}
		_, lastLogIndex := rf.getLastLogTermIndex()
		rf.state = Leader
		rf.nextIndex = make([]int, len(rf.peers))
		rf.matchIndex = make([]int, len(rf.peers))
		for i := 0; i < len(rf.peers); i++ {
			rf.nextIndex[i] = len(rf.log)
		}
		//rf.stopLeaderCh = make(chan struct{})
		// rf.HeartBeatCond.Broadcast()
		rf.matchIndex[rf.me] = lastLogIndex
		// rf.electionTimeOut = time.NewTimer(getElectionTimeOut())
		rf.heartBeatTimer.Reset(BroardcastTime)
		go rf.startHeartbeat()
		//DPrintf("[%d] becomes a %s", rf.me, state)
	case Follower:
		if rf.state == Follower {
			return
		}
		// close(rf.stopLeaderCh)
		// rf.electionTimeOut.Reset(getElectionTimeOut())
		rf.state = Follower
		//DPrintf("[%d] becomes a %s", rf.me, state)
	case Candidate:
		rf.currentTerm = rf.currentTerm + 1
		//rf.stopLeaderCh = make(chan struct{})
		rf.votedFor = rf.me
		rf.state = Candidate
		//DPrintf("[%d] reset the election timeout: Change to Candidate", rf.me)
		rf.electionTimeOut.Reset(getElectionTimeOut())
		//DPrintf("[%d] becomes a %s", rf.me, state)
	}
}

func (rf *Raft) startElection() {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if rf.state == Leader {
		return
	}

	rf.changeState(Candidate)
	rf.persist()
	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()
	args := RequestVoteArgs{
		Term:         rf.currentTerm,
		CandidateID:  rf.me,
		LastLogIndex: lastLogIndex,
		LastLogTerm:  lastLogTerm,
	}
	//DPrintf("[%d] is trying to election at term %d", rf.me, rf.currentTerm)
	cond := sync.NewCond(&rf.mu)
	vote := 1
	finished := 1
	for server := range rf.peers {
		if server == rf.me {
			continue
		}
		go func(server int, args RequestVoteArgs) {
			reply := RequestVoteReply{}
			timer := time.NewTimer(ResendRequestTime)
			for !rf.killed() && !rf.sendRequestVote(server, &args, &reply) {
				// DPrintf("[%d] send RequestVote to %d", args.CandidateID, server)
				// select {
				// case <-rf.stopLeaderCh:
				// 	return
				// default:
				<-timer.C
				timer.Reset(ResendRequestTime)
				//  }
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()
			if reply.Term > rf.currentTerm {
				//DPrintf("[%d] change state from election reply from %d", rf.me, server)
				rf.changeState(Follower)
				rf.currentTerm = reply.Term
				return
			}
			if reply.VoteGranted {
				vote++
			}
			finished++
			rf.persist()
			cond.Broadcast()
		}(server, args)
	}

	for vote <= len(rf.peers)/2 && finished != len(rf.peers) {
		cond.Wait()
		// time.Sleep(10 * time.Millisecond)
	}
	//DPrintf("[%d] get vote : %d, less then length of peers : %v", rf.me, vote, vote < len(rf.peers)/2)
	if vote > len(rf.peers)/2 && rf.state == Candidate {
		rf.changeState(Leader)
	}
}
