package shardmaster

import (
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const (
	CommandTimeout = 500 * time.Millisecond
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg

	// Your data here.
	msgCh       map[int64]chan Message
	lastApplied map[int64]int64
	configs     []Config // indexed by config num
}

type Op struct {
	// Your data here.
	ClientID  int64
	MsgID     int64
	RequestID int64
	Args      interface{}
	Operator  string
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
}

//
// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
//
func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	// Your code here, if desired.
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.lastApplied = make(map[int64]int64)
	sm.msgCh = make(map[int64]chan Message)
	go sm.listen()

	return sm
}

func (sm *ShardMaster) listen() {
	for {
		msg := <-sm.applyCh
		if !msg.CommandValid {
			continue
		}
		op := msg.Command.(Op)

		sm.mu.Lock()
		if !sm.duplicated(op.ClientID, op.MsgID) {
			switch op.Operator {
			case "Join":
			case "Leave":
			case "Move":
			case "Query":
			}
		}
		res := Message{
			Err:         OK,
			WrongLeader: false,
		}
		if op.Operator != "Query" {
			sm.lastApplied[op.ClientID] = op.MsgID
		} else {
			res.Config = sm.getConfigByIndex(op.Args.(QueryArgs).Num)
		}
		if ch, ok := sm.msgCh[op.RequestID]; ok {
			ch <- res
		}
		sm.mu.Unlock()
	}
}

func (sm *ShardMaster) duplicated(clientID int64, msgID int64) bool {
	if val, ok := sm.lastApplied[clientID]; ok {
		return val == msgID
	}
	return false
}

func (sm *ShardMaster) getConfigByIndex(idx int) Config {
	if idx < 0 || idx >= len(sm.configs) {
		return sm.configs[len(sm.configs)-1].Copy()
	} else {
		return sm.configs[idx].Copy()
	}
}
