package raft

type AppendEntriesArgs struct {
	Term         int
	LeaderID     int
	PrevLogIndex int
	PrevLogTerm  int
	Log          []LogEntry
	LeaderCommit int
}

type AppendEntriesReply struct {
	Term    int
	Success bool
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	DPrintf("[%d] received %+v", rf.me, args)
	reply.Term = rf.currentTerm
	reply.Success = false
	if args.Term >= rf.currentTerm {
		rf.currentTerm = args.Term
		rf.changeState(Follower)
	} else {
		return
	}
	//DPrintf("[%d] reset the election timeout: AppendEntries from %d %+v", rf.me, args.LeaderID, args)
	rf.electionTimeOut.Reset(getElectionTimeOut())
	if args.PrevLogIndex >= len(rf.log) || rf.log[args.PrevLogIndex].Term != args.PrevLogTerm {
		//DPrintf("[%d] return false because of prev log not match", rf.me)
		return
	}
	insertIndex := args.PrevLogIndex + 1
	if insertIndex < len(rf.log) && rf.log[insertIndex].Term != args.Term {
		rf.log = rf.log[:insertIndex]
		// DPrintf("[%d] %+v", rf.me, rf.log)
	}
	if insertIndex == len(rf.log) {
		for _, log := range args.Log {
			rf.log = append(rf.log, log)
		}
		//DPrintf("[%d] log: %+v", rf.me, rf.log)
	}
	_, lastLogIndex := rf.getLastLogTermIndex()
	if args.LeaderCommit > rf.commitIndex {
		if lastLogIndex > args.LeaderCommit {
			rf.commitIndex = args.LeaderCommit
		} else {
			rf.commitIndex = lastLogIndex
		}
	}
	reply.Term = rf.currentTerm
	reply.Success = true
	rf.persist()
	// DPrintf("[%d] reply AppendEntries %+v Request: %+v", rf.me, args, reply)
	// DPrintf("[%d] log: %+v", rf.me, rf.log)
}

func (rf *Raft) sendAppendEntriesRequest(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	// DPrintf("[%d] Send AppendEntries Request as state: %s", rf.me, rf.state)
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply)
	return ok
}

func (rf *Raft) sendOneAppendEntriesRequest(server int, args AppendEntriesArgs) {
	// DPrintf("[%d] sendOneAE to %d", rf.me, server)
	// DPrintf("[%d] log: %v", rf.me, rf.log)
	reply := AppendEntriesReply{}
	// timer := time.NewTimer(ResendRequestTime)
	if !rf.sendAppendEntriesRequest(server, &args, &reply) {
		//DPrintf("[%d] fail to send AE to %d", rf.me, server)
		return
	}
	// for !rf.killed() && !rf.sendAppendEntriesRequest(server, &args, &reply) {
	// 	select {
	// 	case <-rf.stopLeaderCh:
	// 		return
	// 	default:
	// 		<-timer.C
	// 		timer.Reset(ResendRequestTime)
	// 	}
	// }
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if reply.Term > rf.currentTerm {
		// DPrintf("[%d] change state from election reply from %d", rf.me, server)
		rf.changeState(Follower)
		rf.currentTerm = reply.Term
		return
	}
	if rf.state != Leader || args.Term != rf.currentTerm {
		return
	}
	if reply.Success {
		rf.matchIndex[server] = args.PrevLogIndex + len(args.Log)
		rf.nextIndex[server]++
		rf.CheckMatchIndexCond.Broadcast()
		// DPrintf("[%d] finished sending log to server %d: %+v", rf.me, server, args.Log)
	} else {
		if rf.nextIndex[server] > 1 {
			rf.nextIndex[server]--
		}
		// DPrintf("[%d] start send append entries to %d", rf.me, server)
		newArgs := rf.getNewAppendEntriesArgs(server)
		go rf.sendOneAppendEntriesRequest(server, newArgs)
		// DPrintf("[%d] next index for server %d: %d, len of log: %d", rf.me, server, rf.nextIndex[server], len(rf.log))
	}
	rf.persist()
}

// func (rf *Raft) startSendAppendEntries(server int, term int) {
// 	rf.mu.Lock()
// 	rf.mu.Unlock()
// 	DPrintf("[%d] start send append entries", rf.me)
// 	for server := range rf.peers {
// 		if server == rf.me {
// 			continue
// 		}
// 		DPrintf("[%d] start send append entries to %d", rf.me, server)
// 		args := rf.getNewAppendEntriesArgs(server)
// 		go rf.sendOneAppendEntriesRequest(server, args)
// 	}
// }

func (rf *Raft) getNewAppendEntriesArgs(server int) AppendEntriesArgs {
	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()
	if rf.nextIndex[server] < len(rf.log) {
		// DPrintf("[%d] send logs", rf.me)
		return AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: rf.nextIndex[server] - 1,
			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
			Log: []LogEntry{
				rf.log[rf.nextIndex[server]],
			},
			LeaderCommit: rf.commitIndex,
		}
	} else {
		// DPrintf("[%d] send heartbeat", rf.me)
		return AppendEntriesArgs{
			Term:         rf.currentTerm,
			LeaderID:     rf.me,
			PrevLogIndex: lastLogIndex,
			PrevLogTerm:  lastLogTerm,
			Log:          []LogEntry{},
			LeaderCommit: rf.commitIndex,
		}
	}
}

// start heartbeat for leader
func (rf *Raft) startHeartbeat() {
	for !rf.killed() {
		<-rf.heartBeatTimer.C
		rf.mu.Lock()
		if rf.state != Leader {
			rf.mu.Unlock()
			return
		}
		//DPrintf("[%d] send heartbeat at term %d", rf.me, rf.currentTerm)
		// DPrintf("[%d] heartbeat timeout", rf.me)
		for server := range rf.peers {
			if server == rf.me {
				continue
			}
			// DPrintf("[%d] send heartbeat to server: %d", rf.me, server)
			args := rf.getNewAppendEntriesArgs(server)
			go rf.sendOneAppendEntriesRequest(server, args)
		}
		rf.heartBeatTimer.Reset(BroardcastTime)
		// DPrintf("[%d] arrange heartbeat", rf.me)
		rf.mu.Unlock()
	}
	//DPrintf("HeartBeat stopped")
}

// func (rf *Raft) startSendAppendEntries(server int, term int) {
// 	rf.mu.Lock()
// 	// DPrintf("[%d] start send append entries to %d", rf.me, server)
// 	for rf.currentTerm == term && rf.state == Leader && rf.nextIndex[server] < len(rf.log) && !rf.killed() {
// 		args := AppendEntriesArgs{
// 			Term:         rf.currentTerm,
// 			LeaderID:     rf.me,
// 			PrevLogIndex: rf.nextIndex[server] - 1,
// 			PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
// 			Log: []LogEntry{
// 				rf.log[rf.nextIndex[server]],
// 			},
// 			LeaderCommit: rf.commitIndex,
// 		}
// 		reply := AppendEntriesReply{}
// 		rf.mu.Unlock()
// 		if !rf.sendAppendEntriesRequest(server, &args, &reply) {
// 			time.Sleep(10 * time.Millisecond)
// 			rf.mu.Lock()
// 			continue
// 		}

// 		rf.mu.Lock()
// 		if reply.Term > rf.currentTerm {
// 			// DPrintf("[%d] change state from AppendEntries reply from %d, %+v", rf.me, server, reply)
// 			rf.currentTerm = reply.Term
// 			rf.changeState(Follower)
// 			break
// 		}

// 		if reply.Success {
// 			rf.matchIndex[server] = args.PrevLogIndex + len(args.Log)
// 			rf.nextIndex[server]++
// 			rf.CheckMatchIndexCond.Broadcast()
// 			DPrintf("[%d] finished sending log to server %d: %+v", rf.me, server, args.Log)
// 		} else {
// 			if rf.nextIndex[server] > 1 {
// 				rf.nextIndex[server]--
// 			}
// 			// DPrintf("[%d] get here", rf.me)
// 			rf.mu.Unlock()
// 			time.Sleep(10 * time.Millisecond)
// 			rf.mu.Lock()
// 			// DPrintf("[%d] next index for server %d: %d, len of log: %d", rf.me, server, rf.nextIndex[server], len(rf.log))
// 		}
// 		// DPrintf("[%d] get here", rf.me)
// 	}
// 	rf.mu.Unlock()
// 	// DPrintf("[%d] log: %+v", rf.me, rf.log)
// }

// func (rf *Raft) sendHeartbeat() {
// 	rf.mu.Lock()
// 	defer rf.mu.Unlock()
// 	lastLogTerm, lastLogIndex := rf.getLastLogTermIndex()
// 	args := AppendEntriesArgs{
// 		Term:         rf.currentTerm,
// 		LeaderID:     rf.me,
// 		PrevLogIndex: lastLogIndex,
// 		PrevLogTerm:  lastLogTerm,
// 		Log:          []LogEntry{},
// 		LeaderCommit: rf.commitIndex,
// 	}

// 	for server := range rf.peers {
// 		if server == rf.me {
// 			// DPrintf("[%d] find its me", rf.me)
// 			continue
// 		}
// 		// DPrintf("[%d] try to send Heartbeat to %d", rf.me, server)
// 		go func(server int, args AppendEntriesArgs) {
// 			// DPrintf("[%d] send Heartbeat to %d", rf.me, server)
// 			reply := AppendEntriesReply{}
// 			if !rf.sendAppendEntriesRequest(server, &args, &reply) {
// 				//log.Printf("[%d] as %s failed to send appendentries request to %d", rf.me, rf.state, server)
// 				return
// 			}
// 			rf.mu.Lock()
// 			defer rf.mu.Unlock()
// 			if args.Term != rf.currentTerm || rf.state != Leader {
// 				return
// 			}
// 			if reply.Term > rf.currentTerm {
// 				// DPrintf("[%d] change state from heartbeat reply from %d", rf.me, server)
// 				rf.currentTerm = reply.Term
// 				rf.changeState(Follower)
// 			}
// 			if reply.Success {
// 				rf.matchIndex[server] = args.PrevLogIndex + len(args.Log)
// 				rf.nextIndex[server]++
// 				rf.CheckMatchIndexCond.Broadcast()
// 			} else {
// 				newArgs := AppendEntriesArgs{
// 					Term:         rf.currentTerm,
// 					LeaderID:     rf.me,
// 					PrevLogIndex: rf.nextIndex[server] - 1,
// 					PrevLogTerm:  rf.log[rf.nextIndex[server]-1].Term,
// 					Log: []LogEntry{
// 						rf.log[rf.nextIndex[server]],
// 					},
// 					LeaderCommit: rf.commitIndex,
// 				}
// 				go rf.sendOneAppendEntriesRequest(server, newArgs)
// 				// DPrintf("[%d] update server: %d", rf.me, server)
// 			}
// 		}(server, args)
// 	}
// }
