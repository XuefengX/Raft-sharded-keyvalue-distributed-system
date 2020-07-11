package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"../labrpc"
)

const RetryInterval = time.Duration(100 * time.Millisecond)

type Clerk struct {
	servers    []*labrpc.ClientEnd
	id         int64
	seqNum     int
	lastLeader int
}

func Nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []*labrpc.ClientEnd) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	ck.id = Nrand()
	ck.seqNum = 0
	ck.lastLeader = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := QueryArgs{Num: num}
	for {
		var reply QueryReply
		if ck.servers[ck.lastLeader].Call("ShardMaster.Query", &args, &reply) && !reply.WrongLeader {
			return reply.Config
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := JoinArgs{Servers: servers, Cid: ck.id, SeqNum: ck.seqNum}
	ck.seqNum++
	for {
		var reply JoinReply
		if ck.servers[ck.lastLeader].Call("ShardMaster.Join", &args, &reply) && !reply.WrongLeader {
			return
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := LeaveArgs{GIDs: gids, Cid: ck.id, SeqNum: ck.seqNum}
	ck.seqNum++
	for {
		var reply LeaveReply
		if ck.servers[ck.lastLeader].Call("ShardMaster.Leave", &args, &reply) && !reply.WrongLeader {
			return
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := MoveArgs{Shard: shard, GID: gid, Cid: ck.id, SeqNum: ck.seqNum}
	ck.seqNum++
	for {
		var reply MoveReply
		if ck.servers[ck.lastLeader].Call("ShardMaster.Move", &args, &reply) && !reply.WrongLeader {
			return
		}
		ck.lastLeader = (ck.lastLeader + 1) % len(ck.servers)
		time.Sleep(RetryInterval)
	}
}
