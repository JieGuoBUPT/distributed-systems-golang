package kvpaxos

import "time"

type Err string

const (
	OK       Err = "OK"
	ErrNoKey Err = "ErrNoKey"
)

const (
	InitialBackoff = 10 * time.Millisecond
	MaxBackoff     = 10 * time.Second
)

type OpType string

const (
	PutOp       OpType = "Put"
	AppendOp    OpType = "Append"
	GetOp       OpType = "Get"
	NoOp        OpType = "NoOp"
	PutAppendOp OpType = "PutAppend" // Only used for routing in server.go!!
)

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	OpType OpType // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq      int
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq      int
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}
