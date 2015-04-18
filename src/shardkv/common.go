package shardkv

import "time"

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//

const (
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"

	ErrBadConfig   = "ErrBadConfig"
	ErrBadReconfig = "ErrBadReconfig"
)

type Err string

const (
	GetOp        = "Get"
	PutOp        = "Put"
	AppendOp     = "Append"
	FetchShardOp = "FetchShard"
	ReconfigOp   = "Reconfigure"
)

type OpType string

const (
	InitialBackoff = 10 * time.Millisecond
	MaxBackoff     = 10 * time.Second
)

type PutAppendArgs struct {
	Key   string
	Value string
	Type  OpType // "Put" or "Append"

	ClientId      int64
	TransactionId int64
}

type GetArgs struct {
	Key           string
	ClientId      int64
	TransactionId int64
}

type Reply struct {
	Err   Err
	Value string
}

type FetchShardArgs struct {
	Config int
	Shard  int
}

type FetchShardReply struct {
	KvData   map[string]string
	LastSeen map[int64]int64
	Err      Err
}
