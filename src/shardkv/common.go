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
	OK            = "OK"
	ErrNoKey      = "ErrNoKey"
	ErrWrongGroup = "ErrWrongGroup"
)

type Err string

const (
	Get         = "Get"
	Put         = "Put"
	PutAppend   = "PutAppend"
	Reconfigure = "Reconfigure"
)

type OpType string

type PutAppendArgs struct {
	Key   string
	Value string
	Op    OpType // "Put" or "PutAppend"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.

	Id int64
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Id int64
}

type Reply struct {
	Err   Err
	Value string
}

type FetchArgs struct {
	Config int
	Shard  int
}

type FetchReply struct {
	Err      Err
	KvData   map[string]string
	PreReply map[int64]string
}
