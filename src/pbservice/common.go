package pbservice

const (
	OK             = "OK"
	ErrNoKey       = "ErrNoKey"
	ErrWrongServer = "ErrWrongServer"
	PutOp          = "Put"
	AppendOp       = "Append"
)

type Err string
type Op string

// Put or Append
type PutAppendArgs struct {
	Key   string
	Value string
	// TODO: You'll have to add definitions here.
	Op       Op
	Seq      int64
	ClientId int64
	ToBackup bool

	// Field names must start with capital letters,
	// otherwise RPC will break.
}

type PutAppendReply struct {
	Err Err
}

type GetArgs struct {
	Key string
	// TODO: You'll have to add definitions here.
	Seq      int64
	ClientId int64
	ToBackup bool
}

type GetReply struct {
	Err   Err
	Value string
}

// TODO: Your RPC definitions here.
type SetStateArgs struct {
	State    map[string]string
	Requests map[int64]int64
}

type SetStateReply struct {
	Err Err
}
