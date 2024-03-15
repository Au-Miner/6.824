package raft

//
// this is an outline of the API that raft must expose to
// the service (or tester). see comments below for
// each of these functions for more details.
//
// rf = Make(...)
//   create a new Raft server.
// rf.Start(command interface{}) (index, term, isleader)
//   start agreement on a new log entry
// rf.GetState() (term, isLeader)
//   ask a Raft for its current term, and whether it thinks it is leader
// ApplyMsg
//   each time a new entry is committed to the log, each Raft peer
//   should send an ApplyMsg to the service (or tester)
//   in the same server.
//

import (
	"6.824/labgob"
	"6.824/labrpc"
	"bytes"
	"sort"
	"sync"
	"sync/atomic"
	"time"
)

// import "bytes"
// import "../labgob"

// as each Raft peer becomes aware that successive log entries are
// committed, the peer should send an ApplyMsg to the service (or
// tester) on the same server, via the applyCh passed to Make(). set
// CommandValid to true to indicate that the ApplyMsg contains a newly
// committed log entry.
//
// in Lab 3 you'll want to send other kinds of messages (e.g.,
// snapshots) on the applyCh; at that point you can add fields to
// ApplyMsg, but set CommandValid to false for these other uses.
type ApplyMsg struct {
	CommandValid bool // 当ApplyMsg用于apply指令时为true，其余时候为false
	Command      interface{}
	CommandIndex int
	CommandTerm  int

	SnapshotValid     bool   // 当ApplyMsg用于传snapshot时为true，其余时候为false
	SnapshotIndex     int    // 本快照包含的最后一个日志的index
	StateMachineState []byte // 状态机状态，就是快照数据
}

type ServerState int

const (
	Follower ServerState = iota
	Candidate
	Leader
)

// A Go object implementing a single Raft peer.
type Raft struct {
	mu        sync.Mutex          // Lock to protect shared access to this peer's state
	peers     []*labrpc.ClientEnd // RPC end points of all peers
	persister *Persister          // Object to hold this peer's persisted state
	me        int                 // this peer's index into peers[]
	dead      int32               // set by Kill()

	// Your data here (2A, 2B, 2C).
	// Look at the paper's Figure 2 for a description of what
	// state a Raft server must maintain.
	// 需要持久化的数据 (all servers)
	currentTerm int        // 当前termId (init: 0)
	votedFor    int        // 投票人的me  (init: -1)
	log         []LogEntry // 日志信息 (start from index 1)
	// 不用持久化的数据 (all servers)
	commitIndex int // 最大已经commit的index (init: 0)
	lastApplied int // 最大已经persist的index (init: 0)
	// 不用持久化的数据 (leader only)
	nextIndex  []int // 准备"发送"给server_i的下一个log_entry index (init: last log index + 1)
	matchIndex []int // 已经"复制"给server_i的最大log_entry index (init: 0)
	// 个人设置
	state         ServerState
	ready         bool          // 是否ready参加竞选
	timeout       *time.Timer   // 超时计时器（超时后state从follower->candidate || state=candidate && ready从true<->false的转换
	heartBeatTime time.Duration // 心跳时间
	applyCh       chan ApplyMsg // 用来处理log_entry
	leaderId      int           // 已知的leaderId (init: -1)
	// snapshot相关
	lastIncludedIndex   int  // 上次快照替换的最后一个条目的index，需要持久化
	lastIncludedTerm    int  // 上次快照替换的最后一个条目的term，需要持久化
	passiveSnapshotting bool // 该raft server正在进行被动快照的标志（若为true则这期间不进行主动快照）
	activeSnapshotting  bool // 该raft server正在进行主动快照的标志（若为true则这期间不进行被动快照）
}

type LogEntry struct {
	Command interface{}
	Term    int
	Index   int // 即处在log数组中的下标是几
}

type AppendEntriesArgs struct {
	Term         int // Leader的term
	LeaderId     int
	PreLogIndex  int // 新增log之前的log_index
	PreLogTerm   int
	Entries      []LogEntry // 新增的logs
	LeaderCommit int        // Leader已经commit的log_index
}

type AppendEntriesReply struct {
	Term          int
	Success       bool // 如果Follower拥有PreLogIndex和PreLogTerm相同的entry则返回true
	ConflictIndex int  // 日志回滚优化算法所需返回的冲突index字段
	ConflictTerm  int  // 日志回滚优化算法所需返回的冲突term字段
}

// return currentTerm and whether this server
// believes it is the leader.
func (rf *Raft) GetState() (int, bool) {
	var term int
	var isLeader bool

	// Your code here (2A).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	term = rf.currentTerm
	isLeader = rf.state == Leader
	return term, isLeader
}

// save Raft's persistent state to stable storage,
// where it can later be retrieved after a crash and restart.
// see paper's Figure 2 for a description of what should be persistent.
func (rf *Raft) persist() {
	// Your code here (2C).
	w := new(bytes.Buffer)
	e := labgob.NewEncoder(w)
	e.Encode(rf.currentTerm)
	e.Encode(rf.votedFor)
	e.Encode(rf.log)
	e.Encode(rf.lastIncludedIndex)
	e.Encode(rf.lastIncludedTerm)
	data := w.Bytes()
	rf.persister.SaveRaftState(data)
}

// restore previously persisted state.
func (rf *Raft) readPersist(data []byte) {
	if data == nil || len(data) < 1 { // bootstrap without any state?
		return
	}
	// Your code here (2C).
	r := bytes.NewBuffer(data)
	d := labgob.NewDecoder(r)
	var currentTerm int
	var votedFor int
	var log []LogEntry
	var lastIncludedIndex int
	var lastIncludedTerm int
	if d.Decode(&currentTerm) != nil ||
		d.Decode(&votedFor) != nil ||
		d.Decode(&log) != nil ||
		d.Decode(&lastIncludedIndex) != nil ||
		d.Decode(&lastIncludedTerm) != nil {
		Logger.Infof("Raft server %d readPersist ERROR!\n", rf.me)
	} else {
		rf.currentTerm = currentTerm
		rf.votedFor = votedFor
		rf.log = log
		rf.lastIncludedIndex = lastIncludedIndex
		rf.lastIncludedTerm = lastIncludedTerm
	}
}

// example RequestVote RPC arguments structure.
// field names must start with capital letters!
type RequestVoteArgs struct {
	// Your data here (2A, 2B).
	Term         int // term id
	CandidateId  int // candidate id
	LastLogIndex int // candidate最后一个日志记录的index
	LastLogTerm  int // candidate最后一个日志记录的term
}

// example RequestVote RPC reply structure.
// field names must start with capital letters!
type RequestVoteReply struct {
	// Your data here (2A).
	VoteGranted bool // 是否同意投票
	Term        int  // 接受人的term
}

// example RequestVote RPC handler.
func (rf *Raft) RequestVote(args *RequestVoteArgs, reply *RequestVoteReply) {
	// Your code here (2A, 2B).
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.VoteGranted = false
		return
	} else if args.Term > rf.currentTerm {
		reply.VoteGranted = false
		rf.leaderId = -1
		rf.currentTerm = args.Term
		rf.Convert2Follower()
	}

	// 判断candidate的log是否至少与这个待投票的server的log一样新，见论文5.4节，两个规则
	// 如果日志的最后条目具有不同的term，那么具有较后term的日志是更新
	// 如果日志的最后条目具有相同的term，那么哪个日志更长，哪个日志就更新
	allowVote := false
	voterLastLog := rf.log[len(rf.log)-1]
	if args.LastLogTerm > voterLastLog.Term || args.LastLogTerm == voterLastLog.Term && args.LastLogIndex >= voterLastLog.Index {
		allowVote = true
	}
	if (rf.votedFor == -1 || rf.votedFor == args.CandidateId) && allowVote {
		rf.votedFor = args.CandidateId
		rf.leaderId = -1
		reply.VoteGranted = true
		rf.persist()
		rf.timeout.Stop()
		rf.timeout.Reset(time.Duration(MyRand(300, 500)) * time.Millisecond)
	} else {
		reply.VoteGranted = false
	}
}

// example code to send a RequestVote RPC to a server.
// server is the index of the target server in rf.peers[].
// expects RPC arguments in args.
// fills in *reply with RPC reply, so caller should
// pass &reply.
// the types of the args and reply passed to Call() must be
// the same as the types of the arguments declared in the
// handler function (including whether they are pointers).
//
// The labrpc package simulates a lossy network, in which servers
// may be unreachable, and in which requests and replies may be lost.
// Call() sends a request and waits for a reply. If a reply arrives
// within a timeout interval, Call() returns true; otherwise
// Call() returns false. Thus Call() may not return for a while.
// A false return can be caused by a dead server, a live server that
// can't be reached, a lost request, or a lost reply.
//
// Call() is guaranteed to return (perhaps after a delay) *except* if the
// handler function on the server side does not return.  Thus there
// is no need to implement your own timeouts around Call().
//
// look at the comments in ../labrpc/labrpc.go for more details.
//
// if you're having trouble getting RPC to work, check that you've
// capitalized all field names in structs passed over RPC, and
// that the caller passes the address of the reply struct with &, not
// the struct itself.
func (rf *Raft) sendRequestVote(server int, args *RequestVoteArgs, reply *RequestVoteReply) bool {
	ok := rf.peers[server].Call("Raft.RequestVote", args, reply)
	return ok
}

// the service using Raft (e.g. a k/v server) wants to start
// agreement on the next command to be appended to Raft's log. if this
// server isn't the leader, returns false. otherwise start the
// agreement and return immediately. there is no guarantee that this
// command will ever be committed to the Raft log, since the leader
// may fail or lose an election. even if the Raft instance has been killed,
// this function should return gracefully.
//
// the first return value is the index that the command will appear at
// if it's ever committed. the second return value is the current
// term. the third return value is true if this server believes it is
// the leader.
// 返回值：1、指令提交后在日志中的index；2、当前的term；3、当前server是否认为自己是Leader；
func (rf *Raft) Start(command interface{}) (int, int, bool) {
	index := -1
	term := -1
	isLeader := true

	// Your code here (2B).
	term, isLeader = rf.GetState()

	rf.mu.Lock()
	defer rf.mu.Unlock()

	index = rf.log[len(rf.log)-1].Index + 1
	if isLeader == false {
		return index, term, isLeader
	}
	newLogEntry := LogEntry{Term: term, Index: index, Command: command}
	rf.log = append(rf.log, newLogEntry)
	rf.persist()
	Logger.Debugf("[Start]Client sends a new commad(%v) to Leader %d!", command, rf.me)
	Logger.Debugf("the log of leader %v has changed to %v", rf.me, rf.log)

	return index, term, isLeader
}

// the tester doesn't halt goroutines created by Raft after each test,
// but it does call the Kill() method. your code can use killed() to
// check whether Kill() has been called. the use of atomic avoids the
// need for a lock.
//
// the issue is that long-running goroutines use memory and may chew
// up CPU time, perhaps causing later tests to fail and generating
// confusing debug output. any goroutine with a long-running loop
// should call killed() to check whether it should stop.
func (rf *Raft) Kill() {
	atomic.StoreInt32(&rf.dead, 1)
	// Your code here, if desired.
}

func (rf *Raft) killed() bool {
	z := atomic.LoadInt32(&rf.dead)
	return z == 1
}

// the service or tester wants to create a Raft server. the ports
// of all the Raft servers (including this one) are in peers[]. this
// server's port is peers[me]. all the servers' peers[] arrays
// have the same order. persister is a place for this server to
// save its persistent state, and also initially holds the most
// recent saved state, if any. applyCh is a channel on which the
// tester or service expects Raft to send ApplyMsg messages.
// Make() must return quickly, so it should start goroutines
// for any long-running work.
func Make(peers []*labrpc.ClientEnd, me int,
	persister *Persister, applyCh chan ApplyMsg) *Raft {
	rf := &Raft{}
	rf.peers = peers
	rf.persister = persister
	rf.me = me
	rf.dead = 0 // 0->alive 1->dead

	// Your initialization code here (2A, 2B, 2C).
	// need read persistence
	rf.currentTerm = 0
	rf.votedFor = -1
	rf.log = append(rf.log, LogEntry{Term: -1, Command: ""})
	// volatile state
	rf.commitIndex, rf.lastApplied = 0, 0
	// personal setting
	rf.ready = true
	rf.timeout = time.NewTimer(time.Duration(MyRand(300, 500)) * time.Millisecond)
	rf.heartBeatTime = 100 * time.Millisecond
	rf.state = Follower
	rf.applyCh = applyCh
	rf.lastIncludedIndex = 0
	rf.lastIncludedTerm = -1
	rf.passiveSnapshotting = false
	rf.activeSnapshotting = false

	// initialize from state persisted before a crash
	rf.readPersist(persister.ReadRaftState())
	rf.recoverFromSnap(persister.ReadSnapshot())
	rf.persist()

	Logger.Debugf("Server %v (Re)Start and lastIncludedIndex=%v, rf.lastIncludedTerm=%v\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
	go rf.HandleTimeout()
	go rf.applier()

	return rf
}

func (rf *Raft) HandleTimeout() {
	Logger.Debugf("%v start to handle timeout", rf.me)
	for {
		select {
		case <-rf.timeout.C:
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			nowState := rf.state
			rf.mu.Unlock()

			switch nowState {
			case Follower:
				rf.timeout.Stop()
				rf.timeout.Reset(time.Duration(MyRand(300, 500)) * time.Millisecond)
				go rf.Go2Election()
			case Candidate:
				rf.timeout.Stop()
				rf.timeout.Reset(time.Duration(MyRand(300, 500)) * time.Millisecond)
				rf.mu.Lock()
				if rf.ready {
					rf.mu.Unlock()
					go rf.Go2Election()
				} else {
					rf.ready = true
					rf.mu.Unlock()
				}
			case Leader:
				return
			}
		}
	}
}

// Go2Election 当follower一段时间没联系到leader时宣布自己成为candidate参加竞选，流程如下：
// 1. 自增current term
// 2. 给自己投一票
// 3. 重置选举计时器
// 4. 向所有其他servers发送请求投票RPC
func (rf *Raft) Go2Election() {
	rf.Convert2Candidate()

	rf.mu.Lock()
	rf.ready = false
	nowTerm := rf.currentTerm
	rf.mu.Unlock()

	votes := 1
	finish := 1
	var voteMu sync.Mutex
	cond := sync.NewCond(&voteMu)

	for peerId := range rf.peers {
		if rf.killed() {
			return
		}
		rf.mu.Lock()
		if rf.state != Candidate {
			rf.mu.Unlock()
			return
		}
		rf.mu.Unlock()
		if peerId == rf.me {
			continue
		}

		go func(peerId int) {
			Logger.Debugf("%v send request vote to %v", rf.me, peerId)
			rf.mu.Lock()
			args := &RequestVoteArgs{
				Term:         nowTerm,
				CandidateId:  rf.me,
				LastLogIndex: rf.log[len(rf.log)-1].Index,
				LastLogTerm:  rf.log[len(rf.log)-1].Term,
			}
			rf.mu.Unlock()
			reply := &RequestVoteReply{}
			ok := rf.sendRequestVote(peerId, args, reply)
			if !ok {
				Logger.Debugf("Candidate %d call server %d for RequestVote failed!\n", rf.me, peerId)
			}

			rf.mu.Lock()
			if rf.state != Candidate || rf.currentTerm != nowTerm {
				rf.mu.Unlock()
				return
			}
			if reply.Term > rf.currentTerm {
				rf.currentTerm = reply.Term
				rf.Convert2Follower()
				rf.mu.Unlock()
				cond.Broadcast()
				return
			}
			rf.mu.Unlock()
			voteMu.Lock()
			if reply.VoteGranted {
				votes++
			}
			finish++
			voteMu.Unlock()
			cond.Broadcast()
		}(peerId)
	}
	voteMu.Lock()
	for {
		cond.Wait()
		rf.mu.Lock()
		stillCandidate := rf.state == Candidate
		rf.mu.Unlock()
		if !stillCandidate {
			voteMu.Unlock()
			return
		}
		majorNum := len(rf.peers)/2 + 1
		if votes >= majorNum {
			voteMu.Unlock()
			rf.Convert2Leader()
			return
		} else if finish == len(rf.peers) {
			voteMu.Unlock()
			Logger.Debugf("%v failed in the election and continued to wait...", rf.me)
			return
		}
	}
}

func (rf *Raft) Convert2Follower() {
	rf.state = Follower
	rf.votedFor = -1
	rf.persist()
}

func (rf *Raft) Convert2Candidate() {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.state = Candidate
	rf.votedFor = rf.me
	rf.currentTerm++
	rf.leaderId = -1
	rf.persist()
	Logger.Debugf("%v convert to Candidate", rf.me)
}

func (rf *Raft) Convert2Leader() {
	rf.mu.Lock()
	rf.state = Leader
	rf.leaderId = rf.me
	Logger.Debugf("%v convert to Leader", rf.me)

	maxIndex := rf.log[len(rf.log)-1].Index
	rf.nextIndex = make([]int, len(rf.peers))
	rf.matchIndex = make([]int, len(rf.peers))
	for idx := range rf.nextIndex {
		rf.nextIndex[idx] = maxIndex + 1
		rf.matchIndex[idx] = 0
	}
	rf.mu.Unlock()

	go func() {
		for !rf.killed() {
			rf.mu.Lock()
			stillLeader := rf.state == Leader
			rf.mu.Unlock()
			if stillLeader {
				go rf.LeaderAppendEntries()
				time.Sleep(rf.heartBeatTime)
			} else {
				rf.timeout.Stop()
				rf.timeout.Reset(time.Duration(MyRand(300, 500)) * time.Millisecond)
				go rf.HandleTimeout()
				return
			}
		}
	}()
}

func (rf *Raft) LeaderAppendEntries() {
	rf.mu.Lock()
	nowTerm := rf.currentTerm
	rf.matchIndex[rf.me] = rf.log[len(rf.log)-1].Index
	rf.nextIndex[rf.me] = rf.matchIndex[rf.me] + 1
	rf.mu.Unlock()

	for idx := range rf.peers {
		if idx == rf.me {
			continue
		}
		go func(idx int) {
			if rf.killed() {
				return
			}
			rf.mu.Lock()
			if rf.state != Leader {
				rf.mu.Unlock()
				return
			}
			if rf.nextIndex[idx] <= rf.lastIncludedIndex {
				go rf.LeaderSendSnapshot(idx, rf.persister.ReadSnapshot())
				rf.mu.Unlock()
				return
			}
			entries := []LogEntry{}
			startIndex := rf.nextIndex[idx] - rf.lastIncludedIndex
			//Logger.Infof("rf.nextIndex[idx]: %v, rf.lastIncludedIndex: %v", rf.nextIndex[idx], rf.lastIncludedIndex)
			//Logger.Infof("startIndex: %v, len(rf.log): %v", startIndex, len(rf.log))
			if startIndex < len(rf.log) {
				entries = make([]LogEntry, len(rf.log)-startIndex)
				copy(entries, rf.log[startIndex:])
			}
			args := &AppendEntriesArgs{
				Term:         nowTerm,
				LeaderId:     rf.leaderId,
				PreLogIndex:  rf.log[startIndex-1].Index,
				PreLogTerm:   rf.log[startIndex-1].Term,
				Entries:      entries,
				LeaderCommit: rf.commitIndex,
			}
			reply := &AppendEntriesReply{}
			//Logger.Infof("args: %v", args)
			//Logger.Infof("log: %v, from %v to %v", rf.log, rf.me, idx)
			rf.mu.Unlock()
			Logger.Debugf("Leader %v sends AppendEntries RPC(term:%d, Entries len:%d) to server %d...\n", rf.me, nowTerm, len(args.Entries), idx)
			ok := rf.sendAppendEntries(idx, args, reply)
			if !ok {
				Logger.Debugf("%v fail send AppendEntries to %v", rf.me, idx)
				return
			}
			rf.mu.Lock()
			defer rf.mu.Unlock()

			//Logger.Infof("reply: %v", reply)

			if rf.state != Leader || rf.currentTerm != nowTerm {
				return
			}
			if rf.currentTerm < reply.Term {
				rf.currentTerm = reply.Term
				rf.Convert2Follower()
				return
			}
			if !reply.Success {
				/**
				下面的else部分为日志回滚优化算法，文字为助教提供
				  If a follower does not have prevLogIndex in its log, it should return with conflictIndex = len(log) and conflictTerm = None.
				  If a follower does have prevLogIndex in its log, but the term does not match, it should return conflictTerm = log[prevLogIndex].Term, and then search its log for the first index whose entry has term equal to conflictTerm.
				  Upon receiving a conflict response, the leader should first search its log for conflictTerm. If it finds an entry in its log with that term, it should set nextIndex to be the one beyond the index of the last entry in that term in its log.
				  If it does not find an entry with that term, it should set nextIndex = conflictIndex.
				*/
				possibleNextIdx := 0
				if reply.ConflictTerm == -1 {
					possibleNextIdx = reply.ConflictIndex
				} else {
					findConflictTerm := false
					k := len(rf.log) - 1
					for ; k > 0; k-- {
						if rf.log[k].Term == reply.ConflictTerm {
							findConflictTerm = true
							break
						} else if rf.log[k].Term < reply.ConflictTerm {
							break
						}
					}
					if findConflictTerm {
						possibleNextIdx = rf.log[k+1].Index
					} else {
						possibleNextIdx = reply.ConflictIndex
					}
				}
				// 问题：为什么更新possibleNextIdx时候需要判断>rf.matchIndex[idx]
				// 解决：会发生，当leader重启，nextIndex[]会变为last_log_index + 1
				if possibleNextIdx < rf.nextIndex[idx] && possibleNextIdx > rf.matchIndex[idx] {
					rf.nextIndex[idx] = possibleNextIdx
				} else {
					Logger.Debugf("%v find index rollback old message", rf.me)
					return
				}
			} else {
				rf.matchIndex[idx] = max(rf.matchIndex[idx], args.PreLogIndex+len(args.Entries))
				rf.nextIndex[idx] = rf.matchIndex[idx] + 1
				/**
				算法思想参见论文rules for server: leader: 4
				If there exists an N such that commitIndex < N <= a majority of matchIndex[i],
				and log[N].term == currentTerm: set commitIndex = N (§5.3, §5.4).
				*/
				sortMatchIndex := make([]int, len(rf.peers))
				copy(sortMatchIndex, rf.matchIndex)
				sort.Ints(sortMatchIndex)
				N := sortMatchIndex[(len(sortMatchIndex)-1)>>1]
				Logger.Debugf("N: %v, rf.lastIncludedIndex: %v, rf.commitIndex: %v, len(rf.log): %v", N, rf.lastIncludedIndex, rf.commitIndex, len(rf.log))
				for ; N > rf.commitIndex; N-- {
					if rf.log[N-rf.lastIncludedIndex].Term == nowTerm {
						rf.commitIndex = N
						Logger.Debugf("Leader%d's commitIndex is updated to %d.\n", rf.me, N)
						break
					}
				}
			}
		}(idx)
	}
}

func (rf *Raft) sendAppendEntries(server int, args *AppendEntriesArgs, reply *AppendEntriesReply) bool {
	ok := rf.peers[server].Call("Raft.AppendEntries", args, reply) // 调用对应server的Raft.AppendEntries方法进行请求日志追加处理
	return ok
}

func (rf *Raft) AppendEntries(args *AppendEntriesArgs, reply *AppendEntriesReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	reply.Term = rf.currentTerm
	if args.Term < rf.currentTerm {
		reply.Success = false
		return
	} else if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.timeout.Stop()
	rf.timeout.Reset(time.Duration(MyRand(300, 500)) * time.Millisecond)

	if rf.state == Leader {
		go rf.HandleTimeout()
	}
	rf.state = Follower
	rf.leaderId = args.LeaderId

	if args.PreLogIndex < rf.lastIncludedIndex {
		if len(args.Entries) == 0 || args.Entries[len(args.Entries)-1].Index <= rf.lastIncludedIndex {
			reply.Success = true
			return
		} else {
			args.Entries = args.Entries[rf.lastIncludedIndex-args.PreLogIndex:]
			args.PreLogTerm = rf.lastIncludedTerm
			args.PreLogIndex = rf.lastIncludedIndex
		}
	}

	if rf.state == Leader {
		go rf.HandleTimeout()
	}

	// 继续进行上面的日志回滚优化算法
	logStartIndex := args.PreLogIndex - rf.lastIncludedIndex
	if rf.log[len(rf.log)-1].Index < args.PreLogIndex {
		reply.ConflictIndex = rf.log[len(rf.log)-1].Index + 1
		reply.ConflictTerm = -1
		reply.Success = false
	} else if rf.log[logStartIndex].Term != args.PreLogTerm {
		reply.ConflictTerm = rf.log[logStartIndex].Term
		k := args.PreLogIndex - rf.lastIncludedIndex - 1
		for k >= 0 && rf.log[k].Term == reply.ConflictTerm {
			k--
		}
		reply.ConflictIndex = k + 1 + rf.lastIncludedIndex
		reply.Success = false
	} else {
		reply.Success = true
		// rf.log[startIndex + 1] <-对应-> args.Entries[0]
		findSucc := false
		for i, entry := range args.Entries {
			if logStartIndex+1+i == len(rf.log) || rf.log[logStartIndex+1+i] != entry {
				logStartIndex = logStartIndex + 1 + i
				findSucc = true
				break
			}
		}
		if findSucc {
			rf.log = append(rf.log[:logStartIndex], args.Entries[logStartIndex+rf.lastIncludedIndex-args.PreLogIndex-1:]...)
			Logger.Debugf("the log of follower %v has changed to %v", rf.me, rf.log)
		}
		rf.persist()

		if args.LeaderCommit > rf.commitIndex {
			rf.commitIndex = min(args.LeaderCommit, rf.log[len(rf.log)-1].Index)
		}
	}
}

// applier是由leader提交所有已经超过一半的log
func (rf *Raft) applier() {
	for !rf.killed() {
		rf.mu.Lock()

		if rf.lastApplied < rf.commitIndex {
			rf.lastApplied++
			if rf.lastApplied <= rf.lastIncludedIndex {
				rf.lastApplied = rf.lastIncludedIndex
				rf.mu.Unlock()
			} else {
				applyMsg := ApplyMsg{
					CommandValid: true,
					Command:      rf.log[rf.lastApplied-rf.lastIncludedIndex].Command,
					CommandTerm:  rf.log[rf.lastApplied-rf.lastIncludedIndex].Term,
					CommandIndex: rf.log[rf.lastApplied-rf.lastIncludedIndex].Index,
				}
				rf.mu.Unlock()
				rf.applyCh <- applyMsg
			}
		} else {
			rf.mu.Unlock()
			time.Sleep(10 * time.Millisecond)
		}
	}
}

func (rf *Raft) GetPassiveFlagAndSetActiveFlag() bool {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	if !rf.passiveSnapshotting {
		rf.activeSnapshotting = true
	}
	return rf.passiveSnapshotting
}

func (rf *Raft) GetRaftStateSize() int {
	return rf.persister.RaftStateSize()
}

// Leader进行主动快照存储，随后给每个Follower发起被动快照存储
func (rf *Raft) ActiveSnapshot(snapshotIndex int, snapshotData []byte) {
	if snapshotIndex <= rf.lastIncludedIndex {
		return
	}
	rf.mu.Lock()
	Logger.Infof("log的大小从%v变为了%v", len(rf.log), len(rf.log[snapshotIndex-rf.lastIncludedIndex:]))
	rf.log = rf.log[snapshotIndex-rf.lastIncludedIndex:]
	rf.lastIncludedIndex = rf.log[0].Index
	rf.lastIncludedTerm = rf.log[0].Term
	rf.persist()
	state := rf.persister.ReadRaftState()
	rf.persister.SaveStateAndSnapshot(state, snapshotData)
	ifLeader := rf.state == Leader
	rf.mu.Unlock()
	if ifLeader {
		for peerId := range rf.peers {
			if peerId == rf.me {
				continue
			}
			go rf.LeaderSendSnapshot(peerId, snapshotData)
		}
	}
}

func (rf *Raft) SetActiveSnapshottingFlag(flag bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.activeSnapshotting = flag
}

func (rf *Raft) SetPassiveSnapshottingFlag(flag bool) {
	rf.mu.Lock()
	defer rf.mu.Unlock()

	rf.passiveSnapshotting = flag
}

type InstallSnapshotArgs struct {
	Term              int    // Leader的Term
	LeaderId          int    // LeaderId
	LastIncludedIndex int    // 快照最后一个log的index
	LastIncludedTerm  int    // 快照最后一个log的term
	SnapshotData      []byte //Snapshot数据
}

type InstallSnapshotReply struct {
	Term   int  // Follower的Term
	Accept bool // Follower是否同意Install
}

func (rf *Raft) LeaderSendSnapshot(peerId int, snapshot []byte) {
	rf.mu.Lock()
	currentTerm := rf.currentTerm
	rf.mu.Unlock()
	if rf.killed() {
		return
	}
	rf.mu.Lock()
	args := InstallSnapshotArgs{
		Term:              currentTerm,
		LeaderId:          rf.me,
		LastIncludedIndex: rf.lastIncludedIndex,
		LastIncludedTerm:  rf.lastIncludedTerm,
		SnapshotData:      snapshot,
	}
	reply := InstallSnapshotReply{}
	Logger.Debugf("Leader %d sends InstallSnapshot RPC(term:%d, LastIncludedIndex:%d, LastIncludedTerm:%d) to server %d...\n",
		rf.me, currentTerm, args.LastIncludedIndex, args.LastIncludedTerm, peerId)
	rf.mu.Unlock()
	ok := rf.sendSnapshot(peerId, &args, &reply)
	rf.mu.Lock()
	defer rf.mu.Unlock()
	if !ok {
		Logger.Infof("Leader %d calls server %d for InstallSnapshot failed!\n", rf.me, peerId)
		return
	}
	if rf.state != Leader || rf.currentTerm != currentTerm {
		return
	}
	if reply.Term > rf.currentTerm {
		rf.currentTerm = reply.Term
		rf.Convert2Follower()
		return
	}
	if reply.Accept {
		rf.matchIndex[peerId] = max(rf.matchIndex[peerId], args.LastIncludedIndex)
		rf.nextIndex[peerId] = rf.matchIndex[peerId] + 1
	}
}

func (rf *Raft) sendSnapshot(server int, args *InstallSnapshotArgs, reply *InstallSnapshotReply) bool {
	ok := rf.peers[server].Call("Raft.PassiveSnapshot", args, reply)
	return ok
}

// follower接受args准备读取被动快照数据
// PassiveSnapshot -> InstallSnapshotFromLeader，让follower所在KVServer更新数据
func (rf *Raft) PassiveSnapshot(args *InstallSnapshotArgs, reply *InstallSnapshotReply) {
	rf.mu.Lock()
	defer rf.mu.Unlock()
	reply.Term = rf.currentTerm
	if rf.activeSnapshotting || args.Term < rf.currentTerm || args.LastIncludedIndex <= rf.lastIncludedIndex {
		reply.Accept = false
		return
	}
	if args.Term > rf.currentTerm {
		rf.currentTerm = args.Term
		rf.votedFor = -1
		rf.persist()
	}
	rf.timeout.Stop()
	rf.timeout.Reset(time.Duration(MyRand(300, 500)) * time.Millisecond)

	if rf.state == Leader {
		go rf.HandleTimeout()
	}
	rf.state = Follower
	rf.leaderId = args.LeaderId

	reply.Accept = true
	rf.lastApplied = args.LastIncludedIndex
	if args.LastIncludedIndex > rf.commitIndex {
		// 如果snapshot最后index超过了follower已经commit的index
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: "", Index: args.LastIncludedIndex}}
		rf.commitIndex = args.LastIncludedIndex
	} else if rf.log[args.LastIncludedIndex-rf.lastIncludedIndex].Term != args.LastIncludedTerm {
		// 如果log对应位置index的term与snapshot对应最后index的term不同
		rf.log = []LogEntry{{Term: args.LastIncludedTerm, Command: "", Index: args.LastIncludedIndex}}
		rf.commitIndex = args.LastIncludedIndex
	} else {
		// 如果对应相同
		var newLog []LogEntry
		newLog = append(newLog, rf.log[args.LastIncludedIndex-rf.lastIncludedIndex:]...)
		rf.log = newLog
		rf.commitIndex = max(args.LastIncludedIndex, rf.commitIndex)
	}
	rf.passiveSnapshotting = true
	rf.lastIncludedIndex = args.LastIncludedIndex
	rf.lastIncludedTerm = args.LastIncludedTerm
	rf.persist()
	rf.persister.SaveStateAndSnapshot(rf.persister.ReadRaftState(), args.SnapshotData)
	rf.PassiveSnapshot2KVServer(args.LastIncludedIndex, args.SnapshotData)
	Logger.Debugf("Server %d accept the snapshot from leader(lastIncludedIndex=%v, lastIncludedTerm=%v).\n", rf.me, rf.lastIncludedIndex, rf.lastIncludedTerm)
}

// 主动快照：LeaderKVServer创建snapshot发送LeaderRaft，LeaderRaft主动快照存储
// 被动快照：LeaderRaft发送给所有FollowerRaft，FollowerRaft发送snapshot给FollowerKVServer
func (rf *Raft) PassiveSnapshot2KVServer(snapshotIndex int, snapshotData []byte) {
	applyMsg := ApplyMsg{
		CommandValid:      false,
		SnapshotValid:     true,
		SnapshotIndex:     snapshotIndex,
		StateMachineState: snapshotData,
	}
	rf.applyCh <- applyMsg
	Logger.Debugf("Server %d send SnapshotMsg(snapIndex=%v) to ApplyCh.\n", rf.me, snapshotIndex)
}

func (rf *Raft) recoverFromSnap(snapshopData []byte) {
	if snapshopData == nil || len(snapshopData) < 1 {
		return
	}
	rf.lastApplied = rf.lastIncludedIndex
	rf.commitIndex = rf.lastIncludedIndex
	applyMsg := ApplyMsg{
		CommandValid:      false,
		SnapshotValid:     true,
		SnapshotIndex:     rf.lastIncludedIndex,
		StateMachineState: snapshopData,
	}
	go func(applyMsg ApplyMsg) {
		rf.applyCh <- applyMsg // 将包含快照的ApplyMsg发送到applyCh，等待状态机处理
	}(applyMsg)
	Logger.Debugf("Server %d recover from crash and send SnapshotMsg to ApplyCh.\n", rf.me)
}
