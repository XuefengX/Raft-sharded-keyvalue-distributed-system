package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

const (
	LeaderTimeout = time.Millisecond * 10
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientID    int64
	leaderID    int
	serialcount int64
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.clientID = nrand()
	ck.serialcount = 1
	ck.leaderID = 0
	return ck
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.Get", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:      key,
		SerialID: ck.serialcount,
		ClientID: ck.clientID,
	}
	ck.serialcount++
	for {
		reply := GetReply{}
		if !ck.servers[ck.leaderID].Call("KVServer.Get", &args, &reply) {
			time.Sleep(LeaderTimeout)
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			//DPrintf("[c%d] client successfully sent get request: %s : %s", ck.clientID, key, reply.Value)
			return reply.Value
		case ErrNoKey:
			//DPrintf("[c%d] client successfully sent put request: No Key", ck.clientID)
			return ""
		case ErrWrongLeader:
			time.Sleep(LeaderTimeout)
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
		}
	}
}

//
// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
//
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		SerialID: ck.serialcount,
		ClientID: ck.clientID,
	}
	ck.serialcount++
	for {
		reply := PutAppendReply{}
		if !ck.servers[ck.leaderID].Call("KVServer.PutAppend", &args, &reply) {
			time.Sleep(LeaderTimeout)
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
		switch reply.Err {
		case OK:
			//DPrintf("[c%d] client successfully sent putappend request: %s : %s", ck.clientID, key, value)
			return
		case ErrWrongLeader:
			time.Sleep(LeaderTimeout)
			ck.leaderID = (ck.leaderID + 1) % len(ck.servers)
			continue
		}
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, "Put")
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, "Append")
}
