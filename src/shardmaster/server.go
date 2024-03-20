package shardmaster

import (
	"math"
	"sync"
	"sync/atomic"
	"time"

	"6.824/labgob"
	"6.824/labrpc"
	"6.824/raft"
)

const RespondTimeout = 500

type ShardMaster struct {
	mu      sync.Mutex
	me      int
	rf      *raft.Raft
	applyCh chan raft.ApplyMsg // 和Raft传递消息
	dead    int32

	// Your data here.
	sessions    map[int64]Session  // clientId -> Session (last response)
	configs     []Config           // indexed by config num (历史配置记录，从1开始)
	notifyMapCh map[int]chan Reply // shardmaster执行完请求操作后通过chan通知对应的handler方法回复client，key为日志的index
}

type Session struct {
	LastSeqNum int    // shardmaster为该client处理的上一次请求的序列号
	OpType     string // 上一次处理的操作请求的类型
	Response   Reply  // 对应的响应
}

type Reply struct {
	Err    Err
	Config Config // 仅Query请求时有效
}

type Op struct {
	// Your data here.
	ClientId int64            // 标识客户端
	SeqNum   int              //请求序列号
	OpType   string           // 操作类型，Join/Leave/Move/Query
	Servers  map[int][]string // new GID -> servers mappings，// 仅在Join操作有效
	GIDs     []int            // 仅在Leave操作有效
	Shard    int              // 仅在Move操作有效
	GID      int              // 仅在Move操作有效
	CfgNum   int              // desired config number，仅在Query操作有效
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) {
	// Your code here.
	sm.mu.Lock()
	if args.SeqNum < sm.sessions[args.ClientId].LastSeqNum {
		reply.Err = OK
		sm.mu.Unlock()
		return
	} else if args.SeqNum == sm.sessions[args.ClientId].LastSeqNum {
		reply.Err = sm.sessions[args.ClientId].Response.Err
		reply.WrongLeader = false // 想更新session必须是leader
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	op := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		OpType:   Join,
		Servers:  args.Servers,
	}
	index, _, ifLeader := sm.rf.Start(op)
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	notifyCh := sm.createNotifyCh(index)
	select {
	case res := <-notifyCh:
		reply.Err = res.Err
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go sm.closeNotifyCh(index)
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) {
	// Your code here.
	sm.mu.Lock()
	if args.SeqNum < sm.sessions[args.ClientId].LastSeqNum {
		reply.Err = OK
		sm.mu.Unlock()
		return
	} else if args.SeqNum == sm.sessions[args.ClientId].LastSeqNum {
		reply.Err = sm.sessions[args.ClientId].Response.Err
		reply.WrongLeader = false // 想更新session必须是leader
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	op := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		OpType:   Leave,
		GIDs:     args.GIDs,
	}
	index, _, ifLeader := sm.rf.Start(op)
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	notifyCh := sm.createNotifyCh(index)
	select {
	case res := <-notifyCh:
		reply.Err = res.Err
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go sm.closeNotifyCh(index)
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) {
	// Your code here.
	sm.mu.Lock()
	if args.SeqNum < sm.sessions[args.ClientId].LastSeqNum {
		reply.Err = OK
		sm.mu.Unlock()
		return
	} else if args.SeqNum == sm.sessions[args.ClientId].LastSeqNum {
		reply.Err = sm.sessions[args.ClientId].Response.Err
		reply.WrongLeader = false // 想更新session必须是leader
		sm.mu.Unlock()
		return
	}
	sm.mu.Unlock()
	op := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		OpType:   Move,
		Shard:    args.Shard,
		GID:      args.GID,
	}
	index, _, ifLeader := sm.rf.Start(op)
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	notifyCh := sm.createNotifyCh(index)
	select {
	case res := <-notifyCh:
		reply.Err = res.Err
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go sm.closeNotifyCh(index)
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) {
	// Your code here.
	op := Op{
		ClientId: args.ClientId,
		SeqNum:   args.SeqNum,
		OpType:   Query,
		CfgNum:   args.Num,
	}
	index, _, ifLeader := sm.rf.Start(op)
	if !ifLeader {
		reply.WrongLeader = true
		return
	}
	reply.WrongLeader = false
	notifyCh := sm.createNotifyCh(index)
	select {
	case res := <-notifyCh:
		reply.Err = res.Err
		reply.Config = res.Config
	case <-time.After(RespondTimeout * time.Millisecond):
		reply.Err = ErrTimeout
	}
	go sm.closeNotifyCh(index)
}

// the tester calls Kill() when a ShardMaster instance won't
// be needed again. you are not required to do anything
// in Kill(), but it might be convenient to (for example)
// turn off debug output from this instance.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.rf.Kill()
	// Your code here, if desired.
}

func (sm *ShardMaster) killed() bool {
	z := atomic.LoadInt32(&sm.dead)
	return z == 1
}

// needed by shardkv tester
func (sm *ShardMaster) Raft() *raft.Raft {
	return sm.rf
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []*labrpc.ClientEnd, me int, persister *raft.Persister) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me
	sm.dead = 0 // 0:not dead 1:dead

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int][]string{}

	labgob.Register(Op{})
	sm.applyCh = make(chan raft.ApplyMsg)
	sm.rf = raft.Make(servers, me, persister, sm.applyCh)

	// Your code here.
	sm.sessions = map[int64]Session{}
	sm.notifyMapCh = map[int]chan Reply{}

	go sm.applyConfigChange()

	return sm
}

func (sm *ShardMaster) applyConfigChange() {
	for !sm.killed() {
		applyMsg := <-sm.applyCh
		if applyMsg.CommandValid {
			sm.mu.Lock()
			op, ok := applyMsg.Command.(Op)
			if !ok {
				sm.mu.Unlock()
				Logger.Infof("convert fail!\n")
				continue
			}
			reply := Reply{}
			sessionRec, exist := sm.sessions[op.ClientId]
			if exist && op.SeqNum < sessionRec.LastSeqNum {
				reply.Err = OK
			} else if exist && op.SeqNum == sessionRec.LastSeqNum {
				reply = sm.sessions[op.ClientId].Response
			} else {
				switch op.OpType {
				case Leave:
					reply.Err = sm.executeLeave(op)
					Logger.Debugf("ShardMaster[%v] Leave(SeqNum=%v) apply success! Groups = %v, Shards = %v\n",
						sm.me, op.SeqNum, sm.getLastConfig().Groups, sm.getLastConfig().Shards)
				case Move:
					reply.Err = sm.executeMove(op)
					Logger.Debugf("ShardMaster[%v] Move(SeqNum=%v) apply success! Groups = %v, Shards = %v\n",
						sm.me, op.SeqNum, sm.getLastConfig().Groups, sm.getLastConfig().Shards)
				case Join:
					reply.Err = sm.executeJoin(op)
					Logger.Debugf("ShardMaster[%v] Join(SeqNum=%v) apply success! Groups = %v, Shards = %v\n",
						sm.me, op.SeqNum, sm.getLastConfig().Groups, sm.getLastConfig().Shards)
				case Query:
					reply.Err, reply.Config = sm.executeQuery(op)
					Logger.Debugf("ShardMaster[%v] Query(SeqNum=%v) apply success! Groups = %v, Shards = %v\n",
						sm.me, op.SeqNum, sm.getLastConfig().Groups, sm.getLastConfig().Shards)
				default:
					Logger.Infof("Unexpected OpType!\n")
				}
				if op.OpType != Query {
					session := Session{
						LastSeqNum: op.SeqNum,
						OpType:     op.OpType,
						Response:   reply,
					}
					sm.sessions[op.ClientId] = session
					Logger.Debugf("ShardMaster[%d].sessions[%d] = %v\n", sm.me, op.ClientId, session)
				}
			}

			if _, ok := sm.notifyMapCh[applyMsg.CommandIndex]; ok {
				if currentTerm, isLeader := sm.rf.GetState(); isLeader && currentTerm == applyMsg.CommandTerm {
					sm.notifyMapCh[applyMsg.CommandIndex] <- reply
				}
			}
			sm.mu.Unlock()
		} else {
			Logger.Infof("ShardMaster[%d] get an unexpected ApplyMsg!\n", sm.me)
		}
	}
}

// 加入多个group，每个group中有相应servers组成
func (sm *ShardMaster) executeJoin(op Op) Err {
	lastConfig := sm.getLastConfig()
	newGroups := deepCopyGroups(lastConfig.Groups)
	for gid, servers := range op.Servers {
		newGroups[gid] = servers
	}
	newConfig := Config{
		Groups: newGroups,
		Num:    lastConfig.Num + 1,
		Shards: shardLoadBalance(newGroups, lastConfig.Shards),
	}
	sm.configs = append(sm.configs, newConfig)
	return OK
}

func (sm *ShardMaster) executeLeave(op Op) Err {
	lastConfig := sm.getLastConfig()
	newGroups := deepCopyGroups(lastConfig.Groups)
	for _, gid := range op.GIDs {
		delete(newGroups, gid)
	}
	var newShards [10]int
	if len(newGroups) != 0 {
		newShards = shardLoadBalance(newGroups, lastConfig.Shards)
	}
	newConfig := Config{
		Groups: newGroups,
		Num:    lastConfig.Num + 1,
		Shards: newShards,
	}
	sm.configs = append(sm.configs, newConfig)
	return OK
}

func (sm *ShardMaster) executeMove(op Op) Err {
	lastConfig := sm.getLastConfig()
	newGroups := deepCopyGroups(lastConfig.Groups)
	newShards := lastConfig.Shards
	newShards[op.Shard] = op.GID
	newConfig := Config{
		Groups: newGroups,
		Num:    lastConfig.Num + 1,
		Shards: newShards,
	}
	sm.configs = append(sm.configs, newConfig)
	return OK
}

func (sm *ShardMaster) executeQuery(op Op) (Err, Config) {
	lastConfig := sm.getLastConfig()
	if op.CfgNum == -1 || op.CfgNum > lastConfig.Num {
		return OK, lastConfig
	}
	return OK, sm.configs[op.CfgNum]
}

func (sm *ShardMaster) createNotifyCh(index int) chan Reply {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	notifyCh := make(chan Reply)
	sm.notifyMapCh[index] = notifyCh
	return notifyCh
}

func (sm *ShardMaster) closeNotifyCh(index int) {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	if _, ok := sm.notifyMapCh[index]; ok {
		close(sm.notifyMapCh[index])
		delete(sm.notifyMapCh, index)
	}
}

func deepCopyGroups(lastGroups map[int][]string) map[int][]string {
	newGroups := map[int][]string{}
	for gid, servers1 := range lastGroups {
		servers2 := make([]string, len(servers1))
		copy(servers2, servers1)
		newGroups[gid] = servers2
	}
	return newGroups
}

// 获取最新的config，调用该方法需要持有sm.mu
func (sm *ShardMaster) getLastConfig() Config {
	return sm.configs[len(sm.configs)-1]
}

// 移动不同shard的所处group来实现负载均衡
func shardLoadBalance(groups map[int][]string, lastShards [NShards]int) [NShards]int {
	Logger.Debugf("groups: %v", groups)
	Logger.Debugf("lastShards: %v", lastShards)
	resShards := lastShards
	lastGroupCnt := map[int]int{}
	newGroupCnt := map[int]int{}
	for _, gid := range lastShards {
		lastGroupCnt[gid]++
	}
	Logger.Debugf("lastGroupCnt: %v", lastGroupCnt)
	// ex1: lastGroupCnt{1:3, 2:3, 3:4}
	// ex2: lastGroupCnt{1:3, 2:3, 3:3}
	for gid := range groups {
		if _, ok := lastGroupCnt[gid]; ok {
			newGroupCnt[gid] = lastGroupCnt[gid]
		} else {
			newGroupCnt[gid] = 0
		}
	}
	Logger.Debugf("newGroupCnt: %v", newGroupCnt)
	// ex1: newGroupCnt{1:3, 2:3, 3:4, 4:0}
	// ex2: newGroupCnt{1:3, 2:3}
	cntMinn, cntMaxn := int(math.Floor(float64(1.0*NShards/len(groups)))), int(math.Ceil(float64(1.0*NShards/len(groups))))
	Logger.Debugf("cntMinn: %v", cntMinn)
	Logger.Debugf("cntMaxn: %v", cntMaxn)
	// ex1: cntMinn=2, cntMaxn=3
	// ex2: cntMinn=4, cntMaxn=5
	level := 0
	for gid, cnt := range newGroupCnt {
		if cnt > cntMaxn {
			level += cnt - cntMaxn
			newGroupCnt[gid] = cntMaxn
		} else if cnt < cntMinn {
			level -= cntMinn - cnt
			newGroupCnt[gid] = cntMinn
		}
	}
	Logger.Debugf("newGroupCnt: %v", newGroupCnt)
	Logger.Debugf("level: %v", level)
	// ex1: newGroupCnt{1:3, 2:3, 3:3, 4:2} level=-1
	// ex2: newGroupCnt{1:4, 2:4} level=-2
	for gid := range lastGroupCnt {
		if _, ok := newGroupCnt[gid]; !ok {
			level += lastGroupCnt[gid]
		}
	}
	Logger.Debugf("level: %v", level)
	// ex1: level=-1
	// ex2: level=1
	for gid := range newGroupCnt {
		if level > 0 && newGroupCnt[gid] == cntMinn {
			level--
			newGroupCnt[gid]++
		} else if level < 0 && newGroupCnt[gid] == cntMaxn {
			level++
			newGroupCnt[gid]--
		}
	}
	Logger.Debugf("newGroupCnt: %v", newGroupCnt)
	Logger.Debugf("level: %v", level)
	// ex1: newGroupCnt{1:2, 2:3, 3:3, 4:2} level=0
	// ex2: newGroupCnt{1:5, 2:4} level=0
	// ex1: lastGroupCnt{1:3, 2:3, 3:4} vs newGroupCnt{1:2, 2:3, 3:3, 4:2}
	// ex2: lastGroupCnt{1:3, 2:3, 3:3} vs newGroupCnt{1:5, 2:4}
	changedInto := []int{}
	for gid, cnt := range newGroupCnt {
		if lastCnt, ok := lastGroupCnt[gid]; ok {
			for i := 1; i <= cnt-lastCnt; i++ {
				changedInto = append(changedInto, gid)
			}
		} else {
			for i := 1; i <= cnt; i++ {
				changedInto = append(changedInto, gid)
			}
		}
	}
	Logger.Debugf("changedInto: %v", changedInto)
	// ex1: changedInto[4, 4]
	// ex2: changedInto[1, 1, 2]
	for sid, gid := range resShards {
		if _, ok := newGroupCnt[gid]; !ok || lastGroupCnt[gid] > newGroupCnt[gid] {
			lastGroupCnt[gid]--
			resShards[sid] = changedInto[len(changedInto)-1]
			changedInto = changedInto[:len(changedInto)-1]
		}
	}
	Logger.Debugf("changedInto: %v", changedInto)
	Logger.Debugf("lastGroupCnt: %v", lastGroupCnt)
	// ex1: changedInto[] lastGroupCnt{1:2, 2:3, 3:3}
	// ex2: changedInto[] lastGroupCnt{1:3, 2:3, 3:0}
	return resShards
}
