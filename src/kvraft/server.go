package kvraft

import (
	"log"
	"sync"
	"sync/atomic"
	"time"

	"../labgob"
	"../labrpc"
	"../raft"
)

const Debug = 1

const (
	LogTimeOut = time.Millisecond * 1000
)

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	SerialID  int64
	ClientID  int64
	RequestID int64
	Operator  string
	Key       string
	Value     string
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	lastApplied      map[int64]int64
	data             map[string]string
	persister        *raft.Persister
	lastAppliedIndex int
	lastAppliedTerm  int
	requestCount     int64
	msgChs           map[int64]chan Err
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	//DPrintf("[s%d] Get service %+v", kv.me, args)
	// _, isLeader := kv.rf.GetState()
	// if !isLeader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }
	op := Op{
		SerialID:  args.SerialID,
		RequestID: kv.requestCount,
		Key:       args.Key,
		Operator:  "Get",
		ClientID:  args.ClientID,
	}
	kv.requestCount++
	res := kv.sendToRaft(op)

	reply.Err = res.Err
	reply.Value = res.value
	DPrintf("[s%d] send back Get: %+v", kv.me, reply)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	//DPrintf("[s%d] %s service %+v", kv.me, args.Op, args)
	// _, isLeader := kv.rf.GetState()
	// if !isLeader {
	// 	reply.Err = ErrWrongLeader
	// 	return
	// }
	op := Op{
		ClientID:  args.ClientID,
		SerialID:  args.SerialID,
		RequestID: kv.requestCount,
		Key:       args.Key,
		Value:     args.Value,
		Operator:  args.Op,
	}
	kv.requestCount++
	res := kv.sendToRaft(op)
	reply.Err = res.Err
	DPrintf("[s%d] send back PutAppend: %+v", kv.me, reply)
}

func (kv *KVServer) sendToRaft(op Op) (r RaftReply) {
	var isLeader bool
	r.index, r.term, isLeader = kv.rf.Start(op)
	if !isLeader {
		r.Err = ErrWrongLeader
		return
	}
	//DPrintf("[s%d] as leader send to raft %+v", kv.me, op)
	// DPrintf("[%d] is the leader", kv.me)
	msgCh := make(chan Err)
	kv.msgChs[op.RequestID] = msgCh
	timer := time.NewTimer(LogTimeOut)
	select {
	case err := <-msgCh:
		r.Err = err
	case <-timer.C:
		r.Err = ErrTimeout
	}
	DPrintf("[s%d] received %+v", kv.me, r.Err)
	if r.Err == OK {
		switch op.Operator {
		case "Get":
			if val, ok := kv.data[op.Key]; ok {
				r.value = val
			} else {
				r.Err = ErrNoKey
			}
		case "Put":
			// kv.data[op.Key] = op.Value
			// kv.lastApplied[op.ClientID] = op.SerialID
		case "Append":
			// if val, ok := kv.data[op.Key]; ok {
			// 	kv.data[op.Key] = val + op.Value
			// 	kv.lastApplied[op.ClientID] = op.SerialID
			// } else {
			// 	kv.data[op.Key] = op.Value
			// 	kv.lastApplied[op.ClientID] = op.SerialID
			// }
		}
		//DPrintf("[s%d] send back %+v", kv.me, r)
	}
	close(msgCh)
	delete(kv.msgChs, op.RequestID)
	return
}

//
// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
//
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Raft to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
// the k/v server should snapshot when Raft's saved state exceeds maxraftstate bytes,
// in order to allow Raft to garbage-collect its log. if maxraftstate is -1,
// you don't need to snapshot.
// StartKVServer() must return quickly, so it should start goroutines
// for any long-running work.
//
func StartKVServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int) *KVServer {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	labgob.Register(Op{})

	kv := new(KVServer)
	kv.me = me
	kv.maxraftstate = maxraftstate

	// You may need initialization code here.

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// You may need initialization code here.
	kv.persister = persister
	kv.lastApplied = make(map[int64]int64)
	kv.data = make(map[string]string)
	kv.msgChs = make(map[int64]chan Err)
	kv.lastAppliedIndex = 0
	kv.lastAppliedTerm = 0
	kv.requestCount = 1
	go kv.listen()
	// go kv.alive()
	return kv
}

func (kv *KVServer) listen() {
	for msg := range kv.applyCh {
		//DPrintf("[s%d] raft response: %+v %T", kv.me, msg.Command, msg.Command)
		if !msg.CommandValid {
			DPrintf("[s%d] server bad command: %d", kv.me, msg.CommandIndex)
			continue
		}
		op := msg.Command.(Op)
		kv.mu.Lock()
		if kv.duplicateCmd(op.ClientID, op.SerialID) {
			kv.mu.Unlock()
			continue
		}
		switch op.Operator {
		case "Get":
		case "Put":
			kv.data[op.Key] = op.Value
			kv.lastApplied[op.ClientID] = op.SerialID
		case "Append":
			if val, ok := kv.data[op.Key]; ok {
				kv.data[op.Key] = val + op.Value
				kv.lastApplied[op.ClientID] = op.SerialID
			} else {
				kv.data[op.Key] = op.Value
				kv.lastApplied[op.ClientID] = op.SerialID
			}
		}
		// DPrintf("[s%d] applied %+v", kv.me, op)
		if ch, ok := kv.msgChs[op.RequestID]; ok {
			// DPrintf("[s%d] have channel", kv.me)
			ch <- OK
		}

		kv.mu.Unlock()
	}
}

func (kv *KVServer) duplicateCmd(clientID int64, serialID int64) bool {
	if val, ok := kv.lastApplied[clientID]; ok && val >= serialID {
		return true
	}
	kv.lastApplied[clientID] = serialID
	return false
}

func (kv *KVServer) alive() {
	timer := time.NewTimer(time.Millisecond * 100)
	for !kv.killed() {
		<-timer.C
		kv.mu.Lock()
		DPrintf("[s%d] is alive", kv.me)
		timer.Reset(time.Millisecond * 100)
		kv.mu.Unlock()
	}
}
