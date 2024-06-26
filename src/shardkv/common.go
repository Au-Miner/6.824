package shardkv

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK             = "OK"             // 在executeClientCmd中确定
	ErrNoKey       = "ErrNoKey"       // 在executeClientCmd中确定
	ErrWrongLeader = "ErrWrongLeader" // 在Get/PutAppend中确定
	ErrWrongGroup  = "ErrWrongGroup"  // 在Get/PutAppend中确定
	ErrTimeout     = "ErrTimeout"     // 在Get/PutAppend中确定
	ErrNotReady    = "ErrNotReady"
)

type Err string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	ClientId int64
	CmdId    int
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	ClientId int64
	CmdId    int
}

type GetReply struct {
	Err   Err
	Value string
}
