package kvraft

import (
	"crypto/rand"
	"math/big"
	"time"

	"6.824/labrpc"
)

type Clerk struct {
	servers []*labrpc.ClientEnd
	// You will have to modify this struct.
	clientId    int64 // client唯一标识符
	knownLeader int   // 已知的leaderId，从请求到非leader的server中的reply获取
	commandNum  int   // command个数 (init: 0)
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
	ck.clientId = nrand()
	ck.knownLeader = -1
	ck.commandNum = 0
	return ck
}

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
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.commandNum++
	args := GetArgs{
		Key:      key,
		ClientId: ck.clientId,
		CmdId:    ck.commandNum,
	}
	serverId := 0
	if ck.knownLeader != -1 {
		serverId = ck.knownLeader
	}
	for ; ; serverId = (serverId + 1) % len(ck.servers) {
		reply := GetReply{}
		if serverId != ck.knownLeader {
			time.Sleep(time.Millisecond * 5)
		}
		Logger.Debugf("Client[%v] send GET RPC to Server[%v]", ck.clientId, serverId)
		ok := ck.servers[serverId].Call("KVServer.Get", &args, &reply)
		if !ok {
			Logger.Infof("Client[%v] cannot send GET RPC to Server[%v]", ck.clientId, serverId)
			continue
		}
		switch reply.Err {
		case OK:
			ck.knownLeader = serverId
			Logger.Debugf("Client[%d] request(Seq:%d) for Get(key:%v) [successfully]!\n", ck.clientId, args.CmdId, args.Key)
			return reply.Value
		case ErrNoKey:
			ck.knownLeader = serverId
			Logger.Debugf("Client[%d] request(Seq:%d) for Get(key:%v) [NoKey]!\n", ck.clientId, args.CmdId, args.Key)
			return ""
		case ErrWrongLeader:
			Logger.Debugf("Client[%d] request(Seq:%d) for Get(key:%v) [WrongLeader]!\n", ck.clientId, args.CmdId, args.Key)
			continue
		case ErrTimeout:
			Logger.Debugf("Client[%d] request(Seq:%d) for Get(key:%v) [Timeout]!\n", ck.clientId, args.CmdId, args.Key)
			continue
		}
	}
}

// shared by Put and Append.
//
// you can send an RPC with code like this:
// ok := ck.servers[i].Call("KVServer.PutAppend", &args, &reply)
//
// the types of args and reply (including whether they are pointers)
// must match the declared types of the RPC handler function's
// arguments. and reply must be passed as a pointer.
func (ck *Clerk) PutAppend(key string, value string, op string) {
	// You will have to modify this function.
	ck.commandNum++
	args := PutAppendArgs{
		Key:      key,
		Value:    value,
		Op:       op,
		ClientId: ck.clientId,
		CmdId:    ck.commandNum,
	}
	serverId := 0
	if ck.knownLeader != -1 {
		serverId = ck.knownLeader
	}
	for ; ; serverId = (serverId + 1) % len(ck.servers) {
		reply := PutAppendReply{}
		if serverId != ck.knownLeader {
			time.Sleep(time.Millisecond * 5)
		}
		Logger.Debugf("Client[%v] send PUT RPC to Server[%v]", ck.clientId, serverId)
		ok := ck.servers[serverId].Call("KVServer.PutAppend", &args, &reply)
		if !ok {
			Logger.Infof("Client[%v] cannot send PUT RPC to Server[%v]", ck.clientId, serverId)
			continue
		}
		switch reply.Err {
		case OK:
			ck.knownLeader = serverId
			Logger.Debugf("Client[%d] request(Seq:%d) for PutAppend [successfully]!\n", ck.clientId, args.CmdId)
			return
		case ErrNoKey:
			ck.knownLeader = serverId
			Logger.Debugf("Client[%d] request(Seq:%d) for PutAppend [NoKey]!\n", ck.clientId, args.CmdId)
			return
		case ErrWrongLeader:
			Logger.Debugf("Client[%d] request(Seq:%d) for PutAppend [WrongLeader]!\n", ck.clientId, args.CmdId)
			continue
		case ErrTimeout:
			Logger.Debugf("Client[%d] request(Seq:%d) for PutAppend [Timeout]!\n", ck.clientId, args.CmdId)
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
