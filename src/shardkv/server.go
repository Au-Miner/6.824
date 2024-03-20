package shardkv

// import "../shardmaster"
import (
	"6.824/shardmaster"
	"bytes"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const (
	RespondTimeout      = 600
	ConfigCheckInterval = 80 // config检测间隔
)

const (
	Get          = "Get"
	Put          = "Put"
	Append       = "Append"
	UpdateConfig = "UpdateConfig" // 更新config版本
	GetShard     = "GetShard"
	GiveShard    = "GiveShard"
	EmptyOp      = "EmptyOp"
)

// shard在某个group中的状态
type ShardState int

// shard状态类型
const (
	NoExist  ShardState = iota // 该shard已经不归该group管（稳定态）
	Exist                      // 该shard归该group管且已经完成迁移（稳定态）
	WaitGet                    // 该shard归该group管但需要等待迁移完成（迁移过程的中间态）
	WaitGive                   // 该shard已经不归该group管还需要等待将shard迁移走（迁移过程的中间态）
)

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64  // 标识客户端
	CmdId    int    // 指令序号
	OpType   string // 操作类型
	Key      string
	Value    string // 若为Get命令则value可不设
	// only UpdateConfig
	NewConfig shardmaster.Config // 新版本的config
	// both GetShard and GiveShard
	CfgNum   int // 本次shard迁移的config编号
	ShardNum int // 从别的group获取到的shard序号
	// only GetShard
	ShardData   map[string]string // shard数据
	SessionData map[int64]Session // shard前任所有者的Session数据
}

type Session struct {
	LastCmdNum int
	OpType     string
	Response   Reply
}

type Reply struct {
	Err   Err
	Value string // Get命令时有效
}

type ShardKV struct {
	mu           sync.Mutex
	me           int // 该server在这一组servers中的序号
	rf           *raft.Raft
	applyCh      chan raft.ApplyMsg
	make_end     func(string) *labrpc.ClientEnd
	gid          int                 // 该ShardKV所属的group的gid
	masters      []*labrpc.ClientEnd // ShardMaters集群的servers端口
	maxraftstate int                 // snapshot if log grows this big

	// Your definitions here.
	dead                  int32                           // set by Kill()
	mck                   *shardmaster.Clerk              // 关联shardmaster
	kvDB                  map[int]map[string]string       // 该group的服务器保存的键值对，为嵌套的map类型（shardNum -> {key -> value}）
	sessions              map[int64]Session               // 此server的状态机为各个client维护的会话，只记录Put/Append的上一次结果
	notifyMapCh           map[int]chan Reply              // kvserver apply到了等待回复的日志则通过chan通知对应的handler方法回复client，key为日志的index
	logLastApplied        int                             // 此kvserver apply的上一个日志的index
	passiveSnapshotBefore bool                            // 标志着applyMessage上一个从channel中取出的是被动快照并已安装完
	ownedShards           [shardmaster.NShards]ShardState // 该ShardKV负责的shard的状态
	preConfig             shardmaster.Config              // 该ShardKV当前的上一个config（用来找前任shard拥有者）
	curConfig             shardmaster.Config              // 该ShardKV目前所处的config
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.checkKeyInGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
	kv.mu.Unlock()

	op := Op{
		ClientId: args.ClientId,
		CmdId:    args.CmdId,
		OpType:   Get,
		Key:      args.Key,
	}
	index, _, ifLeader := kv.rf.Start(op)
	if !ifLeader {
		reply.Err = ErrWrongLeader
		return
	}
	Logger.Debugf("Client[%d] Req Group[%d]ShardKV[%d] for Get(key:%v, index:%v).\n", args.ClientId, kv.gid, kv.me, args.Key, index)
	notifyCh := kv.createNotifyCh(index)
	select {
	case res := <-notifyCh:
		kv.mu.Lock()
		if !kv.checkKeyInGroup(args.Key) {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		reply.Err = res.Err
		reply.Value = res.Value
		Logger.Debugf("Group[%d]ShardKV[%d] respond client[%v] Get(key:%v, index:%v) req.\n", kv.gid, kv.me, args.ClientId, args.Key, index)
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go kv.closeNotifyCh(index)
}

func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *PutAppendReply) {
	// Your code here.
	kv.mu.Lock()
	if !kv.checkKeyInGroup(args.Key) {
		reply.Err = ErrWrongGroup
		kv.mu.Unlock()
		return
	}
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
	Logger.Debugf("Client[%d] Req Group[%d]ShardKV[%d] for Put/Append(key:%v, value:%v, index:%v).\n",
		args.ClientId, kv.gid, kv.me, args.Key, args.Value, index)
	notifyCh := kv.createNotifyCh(index)
	select {
	case res := <-notifyCh:
		kv.mu.Lock()
		if !kv.checkKeyInGroup(args.Key) {
			reply.Err = ErrWrongGroup
			kv.mu.Unlock()
			return
		}
		kv.mu.Unlock()
		reply.Err = res.Err
		Logger.Debugf("Group[%d]ShardKV[%d] respond client[%v] Put/Append(key:%v, value:%v, index:%v) req.\n",
			kv.gid, kv.me, args.ClientId, args.Key, args.Value, index)
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go kv.closeNotifyCh(index)
}

// the tester calls Kill() when a ShardKV instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (kv *ShardKV) Kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.rf.Kill()
	// Your code here, if desired.
}

func (kv *ShardKV) killed() bool {
	z := atomic.LoadInt32(&kv.dead)
	return z == 1
}

// servers[] contains the ports of the servers in this group.
//
// me is the index of the current server in servers[].
//
// the k/v server should store snapshots through the underlying Raft
// implementation, which should call persister.SaveStateAndSnapshot() to
// atomically save the Raft state along with the snapshot.
//
// the k/v server should snapshot when Raft's saved state exceeds
// maxraftstate bytes, in order to allow Raft to garbage-collect its
// log. if maxraftstate is -1, you don't need to snapshot.
//
// gid is this group's GID, for interacting with the shardmaster.
//
// pass masters[] to shardmaster.MakeClerk() so you can send
// RPCs to the shardmaster.
//
// make_end(servername) turns a server name from a
// Config.Groups[gid][i] into a labrpc.ClientEnd on which you can
// send RPCs. You'll need this to send RPCs to other groups.
//
// look at client.go for examples of how to use masters[]
// and make_end() to send RPCs to the group owning a specific shard.
//
// StartServer() must return quickly, so it should start goroutines
// for any long-running work.
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister, maxraftstate int, gid int, masters []*labrpc.ClientEnd, make_end func(string) *labrpc.ClientEnd) *ShardKV {
	// call labgob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	// masters是shardmaster的servers，而servers是当前shardkv所在的groups的servers
	labgob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.maxraftstate = maxraftstate
	kv.make_end = make_end
	kv.gid = gid
	kv.masters = masters

	// Your initialization code here.
	kv.dead = 0
	kv.mck = shardmaster.MakeClerk(masters)
	kv.kvDB = make(map[int]map[string]string)
	for i := 0; i < shardmaster.NShards; i++ {
		kv.kvDB[i] = make(map[string]string)
	}
	kv.sessions = make(map[int64]Session)
	kv.notifyMapCh = make(map[int]chan Reply)
	kv.logLastApplied = 0
	kv.passiveSnapshotBefore = false
	kv.ownedShards = [shardmaster.NShards]ShardState{}
	kv.preConfig = shardmaster.Config{}
	kv.curConfig = shardmaster.Config{}

	// Use something like this to talk to the shardmaster:
	// kv.mck = shardmaster.MakeClerk(kv.masters)

	kv.applyCh = make(chan raft.ApplyMsg)
	kv.rf = raft.Make(servers, me, persister, kv.applyCh)

	// apply日志或快照
	go kv.applyMessage()

	// 定期检查log是否太大需要快照
	go kv.checkSnapshotNeed()

	// 定期轮询shardmaster以获取新配置
	go kv.getLatestConfig()

	// 定期检查是否有shard需要从其他group获取（针对自己的waitGet shard去对应group获取数据）
	go kv.checkAndGetShard()

	// 定期检查是否有shard迁移走已经完成（针对自己的waitGive shard去对应group询问是否已经Get完成）
	go kv.checkAndFinishGiveShard()

	// 定期检查raft层当前term是否有日志，如没有就提交一条空日志帮助之前term的日志提交，防止活锁
	go kv.addEmptyLog()

	return kv
}

func (kv *ShardKV) createNotifyCh(index int) chan Reply {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	notifyCh := make(chan Reply)
	kv.notifyMapCh[index] = notifyCh
	return notifyCh
}

func (kv *ShardKV) closeNotifyCh(index int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if _, ok := kv.notifyMapCh[index]; ok {
		close(kv.notifyMapCh[index])
		delete(kv.notifyMapCh, index)
	}
}

func (kv *ShardKV) checkKeyInGroup(key string) bool {
	sid := key2shard(key)
	if kv.ownedShards[sid] != Exist {
		return false
	}
	return true
}

func (kv *ShardKV) applyMessage() {
	Logger.Debugf("Group[%d]ShardKV[%d] (re)start apply message.\n", kv.gid, kv.me)
	for !kv.killed() {
		applyMsg := <-kv.applyCh
		if applyMsg.CommandValid {
			kv.mu.Lock()
			if applyMsg.CommandIndex <= kv.logLastApplied {
				kv.mu.Unlock()
				continue
			}
			if kv.passiveSnapshotBefore {
				if applyMsg.CommandIndex-kv.logLastApplied != 1 {
					kv.mu.Unlock()
					continue
				}
				kv.passiveSnapshotBefore = false
			}
			kv.logLastApplied = applyMsg.CommandIndex
			kv.mu.Unlock()
			Logger.Debugf("Group[%d]ShardKV[%d] get a Command(index=%v) applyMsg(= %v) from applyCh.\n", kv.gid, kv.me, applyMsg.CommandIndex, applyMsg)
			op, ok := applyMsg.Command.(Op)
			if !ok {
				kv.mu.Unlock()
				Logger.Infof("convert fail!\n")
				continue
			}
			if op.OpType == Get || op.OpType == Put || op.OpType == Append {
				kv.executeClientCmd(op, applyMsg.CommandIndex, applyMsg.CommandTerm)
			} else if op.OpType == UpdateConfig || op.OpType == GetShard || op.OpType == GiveShard {
				kv.executeConfigCmd(op)
			} else if op.OpType == EmptyOp {
				kv.executeEmptyCmd()
			} else {
				Logger.Infof("Unexpected OpType!\n")
			}
		} else if applyMsg.SnapshotValid {
			Logger.Debugf("KVServer[%d] get a Snapshot applyMsg from applyCh.\n", kv.me)
			kv.mu.Lock()
			if applyMsg.StateMachineState != nil && len(applyMsg.StateMachineState) > 0 {
				r := bytes.NewBuffer(applyMsg.StateMachineState)
				d := labgob.NewDecoder(r)
				var kvDB map[int]map[string]string
				var sessions map[int64]Session
				var preConfig shardmaster.Config
				var curConfig shardmaster.Config
				var ownedShards [shardmaster.NShards]ShardState
				if d.Decode(&kvDB) != nil || d.Decode(&sessions) != nil ||
					d.Decode(&preConfig) != nil || d.Decode(&curConfig) != nil || d.Decode(&ownedShards) != nil {
					Logger.Infof("Group[%d]KVServer[%d] applySnapshotToSM ERROR!\n", kv.gid, kv.me)
				} else {
					kv.kvDB = kvDB
					kv.sessions = sessions
					kv.preConfig = preConfig
					kv.curConfig = curConfig
					kv.ownedShards = ownedShards
				}
				kv.logLastApplied = applyMsg.SnapshotIndex
				kv.passiveSnapshotBefore = true
			}
			kv.mu.Unlock()
			Logger.Debugf("Group[%d]KVServer[%d] finish a Snapshot applyMsg from applyCh.\n", kv.gid, kv.me)
			kv.rf.SetPassiveSnapshottingFlag(false)
		} else {
			Logger.Infof("Group[%d]ShardKV[%d] get an unexpected ApplyMsg!\n", kv.gid, kv.me)
		}
	}
}

func (kv *ShardKV) executeClientCmd(op Op, commandIndex int, commandTerm int) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	if !kv.checkKeyInGroup(op.Key) {
		// 这里可以直接return的原因在于Get/PutAppend中已经讨论了ErrWrongGroup的情况
		return
	}
	Logger.Debugf("Group[%d]ShardKV[%d] update logLastApplied to %d.\n", kv.gid, kv.me, kv.logLastApplied)
	reply := Reply{}
	sessionLast, exist := kv.sessions[op.ClientId]
	if exist && op.OpType != "Get" && op.CmdId < sessionLast.LastCmdNum {
		reply.Err = OK
		Logger.Debugf("Group[%d]ShardKV[%d] use the reply(=%v) in sessions for %v.\n", kv.gid, kv.me, reply, op.OpType)
	} else if exist && op.OpType != "Get" && op.CmdId == sessionLast.LastCmdNum {
		reply.Err = kv.sessions[op.ClientId].Response.Err
		Logger.Debugf("Group[%d]ShardKV[%d] use the reply(=%v) in sessions for %v.\n", kv.gid, kv.me, reply, op.OpType)
	} else {
		sid := key2shard(op.Key)
		if op.OpType == Get {
			val, exist := kv.kvDB[sid][op.Key]
			if exist {
				reply.Err = OK
				reply.Value = val
			} else {
				reply.Err = ErrNoKey
			}
			Logger.Debugf("Group[%d]ShardKV[%d] apply Get(key:%v, value:%v) Op!\n", kv.gid, kv.me, op.Key, reply.Value)
		} else if op.OpType == Put {
			reply.Err = OK
			kv.kvDB[sid][op.Key] = op.Value
			Logger.Debugf("Group[%d]ShardKV[%d] apply Put(key:%v, value:%v) Op!\n", kv.gid, kv.me, op.Key, op.Value)
		} else {
			val, exist := kv.kvDB[sid][op.Key]
			if exist {
				reply.Err = OK
				kv.kvDB[sid][op.Key] = val + op.Value
			} else {
				reply.Err = ErrNoKey
				kv.kvDB[sid][op.Key] = op.Value
			}
			Logger.Debugf("Group[%d]ShardKV[%d] apply Append(key:%v, value:%v) Op!\n", kv.gid, kv.me, op.Key, op.Value)
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
	notifyCh, exist := kv.notifyMapCh[commandIndex]
	term, isLeader := kv.rf.GetState()
	if exist && isLeader && term == commandTerm {
		notifyCh <- reply
	}
}

func (kv *ShardKV) getLatestConfig() {
	for !kv.killed() {

		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(ConfigCheckInterval * time.Millisecond)
			continue
		}
		if !kv.updateConfigIsReady() {
			time.Sleep(ConfigCheckInterval * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		curConfig := kv.curConfig
		kv.mu.Unlock()
		nextConfig := kv.mck.Query(curConfig.Num + 1)
		if nextConfig.Num != curConfig.Num {
			updateConfigOp := Op{
				OpType:    UpdateConfig,
				NewConfig: nextConfig,
			}
			kv.rf.Start(updateConfigOp)
		}
		time.Sleep(ConfigCheckInterval * time.Millisecond)
	}
}

func (kv *ShardKV) updateConfigIsReady() bool {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	for _, state := range kv.ownedShards {
		if state != NoExist && state != Exist {
			return false
		}
	}
	return true
}

func (kv *ShardKV) checkAndGetShard() {
	for !kv.killed() {
		_, isLeader := kv.rf.GetState()
		if !isLeader {
			time.Sleep(ConfigCheckInterval * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		waitGetShards := []int{}
		for sid, state := range kv.ownedShards {
			if state == WaitGet {
				waitGetShards = append(waitGetShards, sid)
			}
		}
		preConfig := kv.preConfig
		curConfig := kv.curConfig
		kv.mu.Unlock()
		wg := sync.WaitGroup{}
		for _, sid := range waitGetShards {
			wg.Add(1)
			preServers := preConfig.Groups[preConfig.Shards[sid]]
			go func(servers []string, cfgNum int, shardNum int) {
				defer wg.Done()
				for _, server := range servers {
					srv := kv.make_end(server)
					args := MigrateArgs{
						CfgNum:   cfgNum,
						ShardNum: shardNum,
					}
					reply := MigrateReply{}
					ok := srv.Call("ShardKV.MigrateShard", &args, &reply)
					if ok && reply.Err == OK {
						getShardOp := Op{
							OpType:      GetShard,
							CfgNum:      cfgNum,
							ShardNum:    shardNum,
							ShardData:   reply.ShardData,
							SessionData: reply.SessionData,
						}
						kv.rf.Start(getShardOp)
						Logger.Debugf("Group[%d]ShardKV[%d] request server[%v] for shard[%v] [Successfully]!\n", kv.gid, kv.me, server, shardNum)
						break
					} else if ok && reply.Err == ErrNotReady {
						Logger.Debugf("Group[%d]ShardKV[%d] request server[%v] for shard[%v] but [ErrNotReady]!\n", kv.gid, kv.me, server, shardNum)
						break
					}
				}
			}(preServers, curConfig.Num, sid)
		}
		wg.Wait()
		time.Sleep(ConfigCheckInterval * time.Millisecond)
	}
}

type MigrateArgs struct {
	CfgNum   int // 请求shard源configNum
	ShardNum int // 要请求的shard序号
}

type MigrateReply struct {
	ShardData   map[string]string
	SessionData map[int64]Session
	Err         Err
}

func (kv *ShardKV) MigrateShard(args *MigrateArgs, reply *MigrateReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.CfgNum > kv.curConfig.Num {
		reply.Err = ErrNotReady
		return
	} else if args.CfgNum < kv.curConfig.Num {
		Logger.Warnf("真的出现了！！！")
		return
	}
	reply.Err = OK
	reply.ShardData = deepCopyMap(kv.kvDB[args.ShardNum])
	reply.SessionData = deepCopySession(kv.sessions)
}

func deepCopyMap(originMp map[string]string) map[string]string {
	resMp := make(map[string]string)
	for key, value := range originMp {
		resMp[key] = value
	}
	return resMp
}

func deepCopySession(originSession map[int64]Session) map[int64]Session {
	resSession := make(map[int64]Session)
	for key, value := range originSession {
		resSession[key] = value
	}
	return resSession
}

func (kv *ShardKV) checkAndFinishGiveShard() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(ConfigCheckInterval * time.Millisecond)
			continue
		}
		kv.mu.Lock()
		waitGiveShards := []int{}
		for sid, state := range kv.ownedShards {
			if state == WaitGive {
				waitGiveShards = append(waitGiveShards, sid)
			}
		}
		curConfig := kv.curConfig
		kv.mu.Unlock()
		wg := sync.WaitGroup{}
		for _, sid := range waitGiveShards {
			wg.Add(1)
			curServers := curConfig.Groups[curConfig.Shards[sid]]
			go func(servers []string, cfgNum int, shardNum int) {
				defer wg.Done()
				for _, server := range servers {
					srv := kv.make_end(server)
					args := AckArgs{
						CfgNum:   cfgNum,
						ShardNum: shardNum,
					}
					reply := AckReply{}
					ok := srv.Call("ShardKV.AckReceiveShard", &args, &reply)
					if ok && reply.Err == ErrNotReady {
						Logger.Debugf("Group[%d]ShardKV[%d] request server[%v] for ack[shard %v] but [ErrNotReady]!\n", kv.gid, kv.me, server, shardNum)
						break
					} else if ok && reply.Err == OK {
						if reply.Receive {
							giveShardOp := Op{
								OpType:   GiveShard,
								CfgNum:   cfgNum,
								ShardNum: shardNum,
							}
							kv.rf.Start(giveShardOp)
							Logger.Debugf("Group[%d]ShardKV[%d] request server[%v] for ack[shard %v] [Successfully]!\n", kv.gid, kv.me, server, shardNum)
						} else {
							Logger.Debugf("Group[%d]ShardKV[%d] request server[%v] for ack[shard %v] but [Has not received shard]!\n", kv.gid, kv.me, server, shardNum)
						}
						break
					}
				}
			}(curServers, curConfig.Num, sid)
		}
		wg.Wait()
		time.Sleep(ConfigCheckInterval * time.Millisecond)
	}
}

type AckArgs struct {
	CfgNum   int // 询问房自己所处CfgNum
	ShardNum int // 已收到的shard序号
}

type AckReply struct {
	Receive bool
	Err     Err
}

func (kv *ShardKV) AckReceiveShard(args *AckArgs, reply *AckReply) {
	if _, isLeader := kv.rf.GetState(); !isLeader {
		return
	}
	kv.mu.Lock()
	defer kv.mu.Unlock()
	if args.CfgNum > kv.curConfig.Num {
		reply.Err = ErrNotReady
		return
	} else if args.CfgNum < kv.curConfig.Num {
		// 如果本身的config比请求方的config更新，说明接收方已经在之前的config成功拿到了shard。并继续更新config
		reply.Receive = true
		reply.Err = OK
		return
	}
	reply.Err = OK
	reply.Receive = kv.ownedShards[args.ShardNum] == Exist
}

func (kv *ShardKV) executeConfigCmd(op Op) {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	Logger.Debugf("Group[%d]ShardKV[%d] update logLastApplied to %d.\n", kv.gid, kv.me, kv.logLastApplied)
	switch op.OpType {
	case UpdateConfig:
		// 通过configNum来过滤重复的相关请求
		if op.NewConfig.Num == kv.curConfig.Num+1 {
			kv.updateShardState(op.NewConfig)
			Logger.Debugf("Group[%d]ShardKV[%d] apply UpdateConfig(newCfgNum:%v) Op!\n", kv.gid, kv.me, op.CfgNum)
		}
	case GetShard:
		// 从别的group获取的shard数据存放到本地
		if op.CfgNum == kv.curConfig.Num && kv.ownedShards[op.ShardNum] == WaitGet {
			// 之所以要在这里再重新神拷贝一次，考虑到一个Group内的所有raftServers都会存储该op，并在对应kvServers中存储在kvDB中
			kv.kvDB[op.ShardNum] = deepCopyMap(op.ShardData)
			kv.ownedShards[op.ShardNum] = Exist
			for clientId, session := range op.SessionData {
				if lastSession, exist := kv.sessions[clientId]; !exist || session.LastCmdNum > lastSession.LastCmdNum {
					kv.sessions[clientId] = session
				}
			}
			Logger.Debugf("Group[%d]ShardKV[%d] apply GetShard(cfgNum:%v, shardNum:%v, shardData:%v) Op!\n",
				kv.gid, kv.me, op.CfgNum, op.ShardNum, op.ShardData)
		}
	case GiveShard:
		if op.CfgNum == kv.curConfig.Num && kv.ownedShards[op.ShardNum] == WaitGive {
			kv.ownedShards[op.ShardNum] = NoExist
			delete(kv.kvDB, op.ShardNum)
			Logger.Debugf("Group[%d]ShardKV[%d] apply GiveShard(cfgNum:%v, shardNum:%v) Op!\n",
				kv.gid, kv.me, op.CfgNum, op.ShardNum)
		}
	default:
		Logger.Debugf("Not Config Change CMD OpType!\n")
	}
}

func (kv *ShardKV) updateShardState(nextConfig shardmaster.Config) {
	// 执行updateShardState时，updateConfigIsReady一定为true
	// 因为如果不为true，无法getLatestConfig，也就无法start(updateConfigOp)，无法来到applyMessage
	for sid, state := range kv.ownedShards {
		newGroup := nextConfig.Shards[sid]
		if newGroup != kv.gid && state == Exist {
			kv.ownedShards[sid] = WaitGive
		} else if newGroup == kv.gid && state == NoExist {
			if nextConfig.Num == 1 {
				kv.ownedShards[sid] = Exist
			} else {
				kv.ownedShards[sid] = WaitGet
			}
		}
	}
	Logger.Debugf("Group[%d]ShardKV[%d] update shard state(%v).\n", kv.gid, kv.me, kv.ownedShards)
	kv.preConfig = kv.curConfig
	kv.curConfig = nextConfig
}

func (kv *ShardKV) addEmptyLog() {
	for !kv.killed() {
		if _, isLeader := kv.rf.GetState(); !isLeader {
			time.Sleep(time.Millisecond * ConfigCheckInterval)
			continue
		}
		if !kv.rf.CheckCurrentTermLog() {
			emptyOp := Op{
				OpType: EmptyOp,
			}
			kv.rf.Start(emptyOp)
		}
		time.Sleep(ConfigCheckInterval * time.Millisecond)
	}
}

func (kv *ShardKV) executeEmptyCmd() {
	Logger.Debugf("Group[%d]ShardKV[%d] update logLastApplied to %d.\n", kv.gid, kv.me, kv.logLastApplied)
	Logger.Debugf("Group[%d]ShardKV[%d] apply Empty Op!\n", kv.gid, kv.me)
}

// checkSnapshotNeed -> Snapshot 发起主动快照
func (kv *ShardKV) checkSnapshotNeed() {
	for !kv.killed() {
		if !kv.rf.GetPassiveFlagAndSetActiveFlag() {
			if kv.maxraftstate != -1 && float32(kv.rf.GetRaftStateSize())/float32(kv.maxraftstate) >= 0.9 {
				kv.mu.Lock()
				Logger.Debugf("Group[%d]KVServer[%d]: The Raft state size is approaching the maxraftstate, Start to snapshot...\n", kv.gid, kv.me)
				snapshotIndex := kv.logLastApplied
				w := new(bytes.Buffer)
				e := labgob.NewEncoder(w)
				e.Encode(kv.kvDB)
				e.Encode(kv.sessions)
				e.Encode(kv.preConfig)
				e.Encode(kv.curConfig)
				e.Encode(kv.ownedShards)
				snapshotData := w.Bytes()
				kv.mu.Unlock()
				if snapshotData != nil {
					kv.rf.ActiveSnapshot(snapshotIndex, snapshotData)
				}
			}
		} else {
			Logger.Debugf("Group[%d]Server[%v] is passive snapshotting and refuses positive snapshot.", kv.gid, kv.me)
		}
		kv.rf.SetActiveSnapshottingFlag(false)
		time.Sleep(50 * time.Millisecond)
	}
}
