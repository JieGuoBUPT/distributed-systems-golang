package shardmaster

import "net"
import "fmt"
import "net/rpc"
import "log"

import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs []Config // indexed by config num
	nextSeq int
}

type OpType string

const (
	Join  OpType = "Join"
	Leave OpType = "Leave"
	Move  OpType = "Move"
	Query OpType = "Query"
)

type Op struct {
	// Your data here.
	Id      string
	Type    OpType
	GID     int64
	Servers []string // used if Type == Join
	Shard   int      // used if Type == Move
}

func (sm *ShardMaster) Poll(seq int) interface{} {
	// TODO: something

	return nil
}

func (sm *ShardMaster) LogOperation(operation Op) int {
	// TODO: something

	return nil
}

func (sm *ShardMaster) ProcessLog(stop int) {
	// TODO: something

	return nil
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var operation = Op{}
	operation.Id = nrand()
	operation.Type = Join
	operation.GID = args.GID
	operation.Servers = args.Servers

	var currentSeq int = sm.LogOperation(operation)
	sm.ProcessLog(currentSeq)

	sm.px.Done(currentSeq)
	sm.nextSeq = currentSeq + 1

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var operation = Op{}
	operation.Id = nrand()
	operation.Type = Leave
	operation.GID = args.GID

	var currentSeq int = sm.LogOperation(operation)
	sm.ProcessLog(currentSeq)

	sm.px.Done(currentSeq)
	sm.nextSeq = currentSeq + 1

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var operation = Op{}
	operation.Id = nrand()
	operation.Type = Move
	operation.GID = args.GID
	operation.Shard = args.Shard

	var currentSeq int = sm.LogOperation(operation)
	sm.ProcessLog(currentSeq)

	sm.px.Done(currentSeq)
	sm.nextSeq = currentSeq + 1

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	var operation = Op{}
	operation.Id = nrand()
	operation.Type = Query

	var currentSeq int = sm.LogOperation(operation)
	sm.ProcessLog(currentSeq)

	sm.px.Done(currentSeq)
	sm.nextSeq = currentSeq + 1

	var config_num int = args.Num

	if config_num < 0 || config_num >= len(sm.configs) {
		config_num = len(sm.configs) - 1
	}

	reply.Config = sm.configs[config_num]

	return nil
}

// please don't change these two functions.
func (sm *ShardMaster) Kill() {
	atomic.StoreInt32(&sm.dead, 1)
	sm.l.Close()
	sm.px.Kill()
}

func (sm *ShardMaster) isdead() bool {
	return atomic.LoadInt32(&sm.dead) != 0
}

// please do not change these two functions.
func (sm *ShardMaster) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&sm.unreliable, 1)
	} else {
		atomic.StoreInt32(&sm.unreliable, 0)
	}
}

func (sm *ShardMaster) isunreliable() bool {
	return atomic.LoadInt32(&sm.unreliable) != 0
}

//
// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
//
func StartServer(servers []string, me int) *ShardMaster {
	sm := new(ShardMaster)
	sm.me = me

	sm.configs = make([]Config, 1)
	sm.configs[0].Groups = map[int64][]string{}

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	rpcs.Register(sm)
	sm.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	sm.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for sm.isdead() == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.isdead() == false {
				if sm.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.isunreliable() && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					go rpcs.ServeConn(conn)
				} else {
					go rpcs.ServeConn(conn)
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && sm.isdead() == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
