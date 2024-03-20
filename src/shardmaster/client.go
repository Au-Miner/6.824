package shardmaster

//
// Shardmaster clerk.
//

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// Your data here.
	clientId int64 // client唯一标识符
	seqNum   int   // seqNum个数 (init: 0, start from 1)
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
	// Your code here.
	ck.clientId = nrand()
	ck.seqNum = 0
	return ck
}

func (ck *Clerk) Query(num int) Config {
	args := &QueryArgs{}
	// Your code here.
	args.Num = num
	args.ClientId = ck.clientId
	ck.seqNum++
	args.SeqNum = ck.seqNum
	Logger.Debugf("Client[%v] request Query(seqNum = %v, config num = %v)\n", ck.clientId, ck.seqNum, num)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply QueryReply
			ok := srv.Call("ShardMaster.Query", args, &reply)
			if ok && reply.WrongLeader == false {
				Logger.Debugf("Client[%v] receive Query response and Config=%v. (seqNum -> %v)\n", ck.clientId, reply.Config, ck.seqNum)
				return reply.Config
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Join(servers map[int][]string) {
	args := &JoinArgs{}
	// Your code here.
	args.Servers = servers
	args.ClientId = ck.clientId
	ck.seqNum++
	args.SeqNum = ck.seqNum
	Logger.Debugf("Client[%v] request Join(seqNum = %v, join servers = %v)\n", ck.clientId, ck.seqNum, servers)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply JoinReply
			ok := srv.Call("ShardMaster.Join", args, &reply)
			if ok && reply.WrongLeader == false {
				Logger.Debugf("Client[%v] receive Join response. (seqNum -> %v)\n", ck.clientId, ck.seqNum)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Leave(gids []int) {
	args := &LeaveArgs{}
	// Your code here.
	args.GIDs = gids
	args.ClientId = ck.clientId
	ck.seqNum++
	args.SeqNum = ck.seqNum
	Logger.Debugf("Client[%v] request Leave(seqNum = %v, leave gids = %v)\n", ck.clientId, ck.seqNum, gids)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply LeaveReply
			ok := srv.Call("ShardMaster.Leave", args, &reply)
			if ok && reply.WrongLeader == false {
				Logger.Debugf("Client[%v] receive Leave response. (seqNum -> %v)\n", ck.clientId, ck.seqNum)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}

func (ck *Clerk) Move(shard int, gid int) {
	args := &MoveArgs{}
	// Your code here.
	args.Shard = shard
	args.GID = gid
	args.ClientId = ck.clientId
	ck.seqNum++
	args.SeqNum = ck.seqNum
	Logger.Debugf("Client[%v] request Move(seqNum = %v, move shard = %v, moveTO gid = %v)\n", ck.clientId, ck.seqNum, shard, gid)

	for {
		// try each known server.
		for _, srv := range ck.servers {
			var reply MoveReply
			ok := srv.Call("ShardMaster.Move", args, &reply)
			if ok && reply.WrongLeader == false {
				Logger.Debugf("Client[%v] receive Move response. (seqNum -> %v)\n", ck.clientId, ck.seqNum)
				return
			}
		}
		time.Sleep(100 * time.Millisecond)
	}
}
