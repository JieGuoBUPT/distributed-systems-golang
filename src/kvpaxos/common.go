package kvpaxos

import "time"

const (
	OK       Err = "OK"
	ErrNoKey Err = "ErrNoKey"
)

const (
	InitialBackoff = 10 * time.Millisecond
	MaxBackoff     = 10 * time.Second
)

type Err string

const (
	PutOp    Op = "Put"
	AppendOp Op = "Append"
)

type Op string

// Put or Append
type PutAppendArgs struct {
	// You'll have to add definitions here.
	Key   string
	Value string
	Op    string // "Put" or "Append"
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq      int64
	ClientId int64
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Seq      int64
	ClientId int64
}

type GetReply struct {
	Err   Err
	Value string
}
