package shardmaster

//
// Master shard server: assigns shards to replication groups.
//
// RPC interface:
// Join(servers) -- add a set of groups (gid -> server-list mapping).
// Leave(gids) -- delete a set of groups.
// Move(shard, gid) -- hand off one shard from current owner to gid.
// Query(num) -> fetch Config # num, or latest config if num==-1.
//
// A Config (configuration) describes a set of replica groups, and the
// replica group responsible for each shard. Configs are numbered. Config
// #0 is the initial configuration, with no groups and all shards
// assigned to group 0 (the invalid group).
//
// You will need to add fields to the RPC argument structs.
//

// The number of shards.
const NShards = 10

// A configuration -- an assignment of shards to groups.
// Please don't change this.
type Config struct {
	Num    int              // config number
	Shards [NShards]int     // shard -> gid 不同的shard可能会在相同的group中 shardNum > groupNum
	Groups map[int][]string // gid -> servers[] 每个group都有一些server组成raft集群
}

const (
	OK         = "OK"
	ErrTimeout = "ErrTimeout"
)

const (
	Join  = "Join"
	Query = "Query"
	Leave = "Leave"
	Move  = "Move"
)

type Err string

type JoinArgs struct {
	Servers  map[int][]string // new GID -> servers mappings 考虑到一次Join可能要加入多个group
	ClientId int64
	SeqNum   int
}

type JoinReply struct {
	WrongLeader bool
	Err         Err
}

type LeaveArgs struct {
	GIDs     []int // 一次Join操作可能有多个group离开
	ClientId int64
	SeqNum   int
}

type LeaveReply struct {
	WrongLeader bool
	Err         Err
}

type MoveArgs struct {
	Shard    int // 要转移的shard的序号
	GID      int // 移动到的组的GID
	ClientId int64
	SeqNum   int
}

type MoveReply struct {
	WrongLeader bool
	Err         Err
}

type QueryArgs struct {
	Num      int // desired config number 想要查询的Config的序号
	ClientId int64
	SeqNum   int
}

type QueryReply struct {
	WrongLeader bool
	Err         Err
	Config      Config
}
