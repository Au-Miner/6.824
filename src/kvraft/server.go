package kvraft

import (
	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
	"sync"
	"sync/atomic"
	"time"
)

const RespondTimeout = 500

type Op struct {
	ClientId int64 // 标识客户端
	CmdId    int
	OpType   string // 操作类型，put/append/get
	Key      string
	Value    string // 若为Get命令则value可不设
}

// Session session跟踪为client处理的最新序列号，以及相关的响应。如server接收到序列号已经执行过的命令，它会立即响应，而不需要重新执行请求
type Session struct {
	LastCmdNum int    // 该server为该client处理的上一条指令的序列号
	OpType     string // 最新处理的指令的类型
	Response   Reply  // 对应的响应
}

// Reply GetReply和PutAppendReply的统一结构，用于kvserver保存请求的回复用
type Reply struct {
	Err   Err
	Value string // Get命令时有效
}

type KVServer struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg
	dead    int32 // set by Kill()

	maxraftstate int // snapshot if log grows this big

	// Your definitions here.
	kvDB                  map[string]string
	sessions              map[int64]Session  // clientId -> LastCmd信息和结果
	notifyMapCh           map[int]chan Reply // 每一个cmd发送raft返回得到的index都对应一个chan来获取raft applier到的结果
	logLastApplied        int                // 此kvserver apply的上一个日志的index
	passiveSnapshotBefore bool               // 标志着applyMessage上一个从channel中取出的是被动快照并已安装完
}

func (kv *KVServer) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		CmdId:    args.CmdId,
		OpType:   "Get",
		Key:      args.Key,
	}
	index, _, ifLeader := kv.rf.Start(op)
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	notifyCh := kv.createNotifyCh(index)
	select {
	case res := <-notifyCh:
		reply.Err = res.Err
		reply.Value = res.Value
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go kv.closeNotifyCh(index)
}

func (kv *KVServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if kv.sessions[args.ClientId].LastCmdNum > args.CmdId {
		reply.Err = OK // 过期请求，不需要执行任何操作，直接返回OK（结束重复发送putAppend）
		kv.mu.Unlock()
		return
	} else if kv.sessions[args.ClientId].LastCmdNum == args.CmdId { // 就是最近一次执行，将最近执行结果返回
		reply.Err = kv.sessions[args.ClientId].Response.Err
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()
	// 最新请求
	op := Op{
		ClientId: args.ClientId,
		CmdId:    args.CmdId,
		OpType:   args.Op,
		Key:      args.Key,
		Value:    args.Value,
	}
	index, _, ifLeader := kv.rf.Start(op)
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	notifyCh := kv.createNotifyCh(index)
	select {
	case res := <-notifyCh:
		reply.Err = res.Err
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go kv.closeNotifyCh(index)

}

// the tester calls Kill() when a KVServer instance won't
// be needed again. for your convenience, we supply
// code to set rf.dead (without needing a lock),
// and a killed() method to test rf.dead in
// long-running loops. you can also add your own
// code to Kill(). you're not required to do anything
// about this, but it may be convenient (for example)
// to suppress debug output from a Kill()ed instance.
func (kv *KVServer) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *KVServer) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

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
	kv.kvDB = make(map[string]string)
	kv.sessions = make(map[int64]Session)
	kv.notifyMapCh = make(map[int]chan Reply)
	kv.logLastApplied = 0
	kv.passiveSnapshotBefore = false

	// 开启goroutine来循环检测和apply从applyCh中传来的指令或快照
	go kv.applyMessage()

	// 开启goroutine来定期检查log是否太大需要快照
	go kv.checkSnapshotNeed()

	return kv
}

func (kv *KVServer) createNotifyCh(index int) chan Reply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	notifyCh := make(chan Reply)
	kv.notifyMapCh[index] = notifyCh
	return notifyCh
}

func (kv *KVServer) closeNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.notifyMapCh[index]; ok {
		close(kv.notifyMapCh[index])
		delete(kv.notifyMapCh, index)
	}
}

func (kv *KVServer) applyMessage() {
	Logger.Debugf("KVServer[%d] (re)start applyMessage.\n", kv.me)
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			kv.mu.Lock()
			Logger.Debugf("KVServer[%d] get a Command applyMsg(= %v) from applyCh.\n", kv.me, applyMsg)

			if applyMsg.CommandIndex <= kv.logLastApplied {
				kv.mu.Unlock()
				continue
			}
			// 有关为什么这里可以直接赋值：因为raft.lastApplied是逐一递增的，将对应的index作为CommandIndex
			kv.logLastApplied = applyMsg.CommandIndex
			Logger.Debugf("KVServer[%d] update logLastApplied to %d.\n", kv.me, kv.logLastApplied)
			op, ok := applyMsg.Command.(Op)
			if !ok {
				Logger.Infof("%v cannot assert into Op!", applyMsg.Command)
				kv.mu.Unlock()
				continue
			}
			reply := Reply{}
			sessionLast, exist := kv.sessions[op.ClientId]
			if exist && op.OpType != "Get" && op.CmdId < sessionLast.LastCmdNum {
				reply.Err = OK
				Logger.Debugf("KVServer[%d] use the reply(=%v) in sessions for %v.\n", kv.me, reply, op.OpType)
			} else if exist && op.OpType != "Get" && op.CmdId == sessionLast.LastCmdNum {
				reply.Err = kv.sessions[op.ClientId].Response.Err
				Logger.Debugf("KVServer[%d] use the reply(=%v) in sessions for %v.\n", kv.me, reply, op.OpType)
			} else {
				switch op.OpType {
				case "Get":
					value, exist := kv.kvDB[op.Key]
					if exist {
						reply.Err = OK
						reply.Value = value
					} else {
						reply.Err = ErrNoKey
					}
					Logger.Debugf("KVServer[%d] apply Get(key:%v, value:%v) Op!\n", kv.me, op.Key, reply.Value)
				case "Put":
					reply.Err = OK
					kv.kvDB[op.Key] = op.Value
					Logger.Debugf("KVServer[%d] apply Put(key:%v, value:%v) Op!\n", kv.me, op.Key, op.Value)
				case "Append":
					value, exist := kv.kvDB[op.Key]
					if exist {
						reply.Err = OK
						kv.kvDB[op.Key] = value + op.Value
					} else {
						reply.Err = ErrNoKey
						kv.kvDB[op.Key] = op.Value
					}
					Logger.Debugf("KVServer[%d] apply Append(key:%v, value:%v) Op!\n", kv.me, op.Key, op.Value)
				default:
					Logger.Warnf("Unexpected OpType! %v\n", op.OpType)
				}
				if op.OpType != "Get" {
					session := Session{
						LastCmdNum: op.CmdId,
						OpType:     op.OpType,
						Response:   reply,
					}
					kv.sessions[op.ClientId] = session
					Logger.Debugf("KVServer[%d].sessions[%d] = %v\n", kv.me, op.ClientId, session)
				}
			}
			notifyCh, exist := kv.notifyMapCh[applyMsg.CommandIndex]
			term, isLeader := kv.rf.GetState()
			if exist && isLeader && term == applyMsg.CommandTerm {
				notifyCh <- reply
			}
			kv.mu.Unlock()
		} else {

		}
	}
}

func (kv *KVServer) checkSnapshotNeed() {

}
