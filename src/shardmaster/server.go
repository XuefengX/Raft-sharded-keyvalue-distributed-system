package shardmaster

import (
	"log"
	"math"
	"sync"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	// Your data here.
	configs []Config // indexed by config num
	chMap   map[int]chan Op
	cid2Seq map[int64]int
	killCh  chan bool
}

type Op struct {
	OpType string      "operation type(eg. join/leave/move/query)"
	Args   interface{} // could be JoinArgs, LeaveArgs, MoveArgs and QueryArgs, in reply it could be config
	Cid    int64
	SeqNum int
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	originOp := Op{"Join", *args, args.Cid, args.SeqNum}
	reply.WrongLeader = sm.templateHandler(originOp)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	originOp := Op{"Leave", *args, args.Cid, args.SeqNum}
	reply.WrongLeader = sm.templateHandler(originOp)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	originOp := Op{"Move", *args, args.Cid, args.SeqNum}
	reply.WrongLeader = sm.templateHandler(originOp)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	reply.WrongLeader = true
	originOp := Op{"Query", *args, Nrand(), -1}
	reply.WrongLeader = sm.templateHandler(originOp)
	if !reply.WrongLeader {
		sm.mu.Lock()
		defer sm.mu.Unlock()
		if args.Num >= 0 && args.Num < len(sm.configs) {
			reply.Config = sm.configs[args.Num]
		} else {
			reply.Config = sm.configs[len(sm.configs)-1]
		}
	}
}

func (sm *ShardMaster) templateHandler(originOp Op) bool {
	wrongLeader := true
	index, _, isLeader := sm.rf.Start(originOp)
	if !isLeader {
		return wrongLeader
	}
	ch := sm.getCh(index, true)
	op := sm.beNotified(ch, index)
	if equalOp(op, originOp) {
		wrongLeader = false
	}
	return wrongLeader
}

func (sm *ShardMaster) beNotified(ch chan Op, index int) Op {
	select {
	case notifyArg := <-ch:
		close(ch)
		sm.mu.Lock()
		delete(sm.chMap, index)
		sm.mu.Unlock()
		return notifyArg
	case <-time.After(time.Duration(600) * time.Millisecond):
		return Op{}
	}
}

func equalOp(a Op, b Op) bool {
	return a.SeqNum == b.SeqNum && a.Cid == b.Cid && a.OpType == b.OpType
}

func (sm *ShardMaster) Kill() {
	sm.rf.Kill()
	sm.killCh <- true
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

func (sm *ShardMaster) getCh(idx int, createIfNotExists bool) chan Op {
	sm.mu.Lock()
	defer sm.mu.Unlock()
	if _, ok := sm.chMap[idx]; !ok {
		if !createIfNotExists {
			return nil
		}
		sm.chMap[idx] = make(chan Op, 1)
	}
	return sm.chMap[idx]
}

func (sm *ShardMaster) updateConfig(op string, arg interface{}) {
	cfg := sm.createNextConfig()
	if op == "Move" {
		moveArg := arg.(MoveArgs)
		if _, exists := cfg.Groups[moveArg.GID]; exists {
			cfg.Shards[moveArg.Shard] = moveArg.GID
		} else {
			return
		}
	} else if op == "Join" {
		joinArg := arg.(JoinArgs)
		for gid, servers := range joinArg.Servers {
			newServers := make([]string, len(servers))
			copy(newServers, servers)
			cfg.Groups[gid] = newServers
			sm.rebalance(&cfg, op, gid)
		}
	} else if op == "Leave" {
		leaveArg := arg.(LeaveArgs)
		for _, gid := range leaveArg.GIDs {
			delete(cfg.Groups, gid)
			sm.rebalance(&cfg, op, gid)
		}
	} else {
		log.Fatal("invalid area", op)
	}
	sm.configs = append(sm.configs, cfg)
}

func (sm *ShardMaster) createNextConfig() Config {
	lastCfg := sm.configs[len(sm.configs)-1]
	nextCfg := Config{Num: lastCfg.Num + 1, Shards: lastCfg.Shards, Groups: make(map[int][]string)}
	for gid, servers := range lastCfg.Groups {
		nextCfg.Groups[gid] = append([]string{}, servers...)
	}
	return nextCfg
}

func (sm *ShardMaster) rebalance(cfg *Config, request string, gid int) {
	shardsCount := sm.groupByGid(cfg) // gid -> shards
	switch request {
	case "Join":
		avg := NShards / len(cfg.Groups)
		for i := 0; i < avg; i++ {
			maxGid := sm.getMaxShardGid(shardsCount)
			cfg.Shards[shardsCount[maxGid][0]] = gid
			shardsCount[maxGid] = shardsCount[maxGid][1:]
		}
	case "Leave":
		shardsArray, exists := shardsCount[gid]
		if !exists {
			return
		}
		delete(shardsCount, gid)
		if len(cfg.Groups) == 0 { // remove all gid
			cfg.Shards = [NShards]int{}
			return
		}
		for _, v := range shardsArray {
			minGid := sm.getMinShardGid(shardsCount)
			cfg.Shards[v] = minGid
			shardsCount[minGid] = append(shardsCount[minGid], v)
		}
	}
}
func (sm *ShardMaster) groupByGid(cfg *Config) map[int][]int {
	shardsCount := map[int][]int{}
	for k, _ := range cfg.Groups {
		shardsCount[k] = []int{}
	}
	for k, v := range cfg.Shards {
		shardsCount[v] = append(shardsCount[v], k)
	}
	return shardsCount
}
func (sm *ShardMaster) getMaxShardGid(shardsCount map[int][]int) int {
	max := -1
	var gid int
	for k, v := range shardsCount {
		if max < len(v) {
			max = len(v)
			gid = k
		}
	}
	return gid
}
func (sm *ShardMaster) getMinShardGid(shardsCount map[int][]int) int {
	min := math.MaxInt32
	var gid int
	for k, v := range shardsCount {
		if min > len(v) {
			min = len(v)
			gid = k
		}
	}
	return gid
}
func send(notifyCh chan Op, op Op) {
	notifyCh <- op
}
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}
	labgob.Register(Op{})
	labgob.Register(JoinArgs{})
	labgob.Register(LeaveArgs{})
	labgob.Register(MoveArgs{})
	labgob.Register(QueryArgs{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)
	// Your code here.
	sm.chMap = make(map[int]chan Op)
	sm.cid2Seq = make(map[int64]int)
	sm.killCh = make(chan bool, 1)
	go func() {
		for {
			select {
			case <-sm.killCh:
				return
			case applyMsg := <-sm.applyCh:
				if !applyMsg.CommandValid {
					continue
				}
				op := applyMsg.Command.(Op)
				sm.mu.Lock()
				maxSeq, found := sm.cid2Seq[op.Cid]
				if op.SeqNum >= 0 && (!found || op.SeqNum > maxSeq) {
					sm.updateConfig(op.OpType, op.Args)
					sm.cid2Seq[op.Cid] = op.SeqNum
				}
				sm.mu.Unlock()
				if notifyCh := sm.getCh(applyMsg.CommandIndex, false); notifyCh != nil {
					send(notifyCh, op)
				}
			}
		}
	}()
	return sm
}
