package shardkv

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "paxos"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "encoding/gob"
import "math/rand"
import "shardmaster"

const Debug = 0

func (kv *ShardKV) DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		kv.log_mu.Lock()
		defer kv.log_mu.Unlock()

		fmt.Printf("\x1b[%dm", (kv.me%6)+31)
		fmt.Printf("(Group: %d, Server: %d, Config: %d):\t", kv.gid, kv.me, kv.config.Num)
		fmt.Printf(format+"\n", a...)
		fmt.Printf("\x1b[0m")
	}
	return
}

type Op struct {
	Type      OpType
	Id        int64
	Key       string
	Value     string
	CurConfig shardmaster.Config

	// for reconfiguration
	KvData   map[string]string
	LastSeen map[int64]int64
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	log_mu sync.Mutex // just used for colorful logs

	gid int64 // my replica group Id

	config          shardmaster.Config
	kvData          map[string]string
	lastSeen        map[int64]int64
	maxInstance     int
	maxProcessedSeq int

	// Reconfiguration
	highestSeen      int
	currentConfigNum int
	reconfigKvData   map[string]string
}

// -----------------------------------------------------
// Handle RPCs
// -----------------------------------------------------

func (kv *ShardKV) Get(args *GetArgs, reply *Reply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.DPrintf("Get for %s received (%s)", args.Key, reply.Err)

	if kv.config.Shards[key2shard(args.Key)] == kv.gid {
		if kv.lastSeen[args.ClientId] != args.TransactionId { // at most once
			op := Op{Key: args.Key, Type: GetOp, Id: nrand()}
			kv.logOp(op)
		}

		if val, ok := kv.kvData[args.Key]; ok {
			reply.Value = val
			reply.Err = OK
		} else {
			reply.Err = ErrNoKey
		}
	} else {
		reply.Err = ErrWrongGroup
	}
	return nil
}

// RPC handler for client Put and Append requests
func (kv *ShardKV) PutAppend(args *PutAppendArgs, reply *Reply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	kv.DPrintf("%s for %s received (%s)", args.Type, args.Key, reply.Err)

	if kv.lastSeen[args.ClientId] != args.TransactionId {
		op := Op{Key: args.Key, Value: args.Value, Type: args.Type, Id: nrand()}
		kv.logOp(op)

		if kv.config.Shards[key2shard(args.Key)] == kv.gid {
			reply.Err = OK
		} else {
			reply.Err = ErrWrongGroup
		}
	}
	return nil
}

func (kv *ShardKV) FetchShard(args *FetchShardArgs, reply *FetchShardReply) error {
	kv.DPrintf("Fetch for config %s, shard %s received (%s)", args.Config, args.Shard, reply.Err)

	if args.Config < kv.config.Num {
		kv.mu.Lock()
		defer kv.mu.Unlock()

		shardKvData := make(map[string]string)
		for k, v := range kv.kvData {
			if key2shard(k) == args.Shard {
				shardKvData[k] = v
			}
		}

		reply.KvData = shardKvData
		reply.LastSeen = kv.lastSeen
		reply.Err = OK

		op := Op{Type: FetchShardOp, Id: nrand()}
		kv.logOp(op)
	} else {
		reply.Err = ErrBadConfig
	}
	return nil
}

// Actually apply an operation to local state
func (kv *ShardKV) doOp(op Op) {
	kv.DPrintf("Applying log entry %s of type %s", op.Id, op.Type)
	switch op.Type {
	case GetOp:
		// we don't need to do anything here
	case PutOp:
		if kv.config.Shards[key2shard(op.Key)] == kv.gid {
			kv.kvData[op.Key] = op.Value
		}
	case AppendOp:
		if kv.config.Shards[key2shard(op.Key)] == kv.gid {
			kv.kvData[op.Key] += op.Value
		}
	case ReconfigOp:
		if kv.config.Num < op.CurConfig.Num {
			for k, v := range op.KvData {
				kv.kvData[k] = v
			}
			for k, v := range op.LastSeen {
				kv.lastSeen[k] = v
			}
			kv.config = op.CurConfig
		}
	}
}

// ------------------------------------------------
// Checking for new configurations
// ------------------------------------------------

//
// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
//
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	config := kv.sm.Query(-1)

	if config.Num > kv.config.Num {
		for i := kv.config.Num + 1; i <= config.Num; i++ {
			err := kv.reconfigure(kv.sm.Query(i))
			if err != OK {
				return
			}
		}
	}
}

func (kv *ShardKV) reconfigure(config shardmaster.Config) Err {
	kv.DPrintf("Reconfiguring...")

	shardsKvData := make(map[string]string)
	shardsLastSeen := make(map[int64]int64)

	for i := 0; i < shardmaster.NShards; i++ {
		// get new data from previous owner
		prevOwner := kv.config.Shards[i]
		if config.Shards[i] == kv.gid && prevOwner != kv.gid && prevOwner != 0 {
			args := &FetchShardArgs{Shard: i, Config: kv.config.Num}
			var reply FetchShardReply
			gid := kv.config.Shards[i]
			servers, ok := kv.config.Groups[gid]
			if ok {
				for index := range servers {
					ok := call(servers[index], "ShardKV.FetchShard", args, &reply)
					if ok && reply.Err == OK {
						for k, v := range reply.KvData {
							shardsKvData[k] = v
						}
						for k, v := range reply.LastSeen {
							shardsLastSeen[k] = v
						}
						break
					}
				}
				if reply.Err == ErrBadConfig {
					return ErrBadReconfig
				}
			} else {
				return ErrBadReconfig
			}
		}
	}
	reconfigOp := Op{Type: ReconfigOp, Id: nrand(), CurConfig: config, KvData: shardsKvData, LastSeen: shardsLastSeen}

	kv.logOp(reconfigOp)
	return OK
}

// ------------------------------------------------
// Communicate with Paxos log
// ------------------------------------------------
func (kv *ShardKV) logOp(transaction Op) {
	kv.DPrintf("Logging op %s (%s)...", transaction.Id, transaction.Type)

	var seq int
	for !kv.isdead() {
		seq = kv.maxInstance + 1

		fate, value := kv.px.Status(seq)
		var val Op

		if fate != paxos.Decided {
			kv.px.Start(seq, transaction)
			val = kv.poll(seq)
		} else {
			val = value.(Op)
		}

		kv.doOp(val)
		kv.px.Done(seq)
		kv.maxInstance++

		if val.Id == transaction.Id {
			return
		}
	}
}

func (kv *ShardKV) poll(seq int) Op {
	to := InitialBackoff
	for !kv.isdead() {
		fate, value := kv.px.Status(seq)

		if fate == paxos.Decided {
			return value.(Op)
		}

		time.Sleep(to)
		if to < MaxBackoff {
			to *= 2
		}
	}
	return Op{}
}

// ---------------------------------------------
// Boilerplate staff code
// --------------------------------------------

// tell the server to shut itself down.
// please don't change these two functions.
func (kv *ShardKV) kill() {
	atomic.StoreInt32(&kv.dead, 1)
	kv.l.Close()
	kv.px.Kill()
}

func (kv *ShardKV) isdead() bool {
	return atomic.LoadInt32(&kv.dead) != 0
}

// please do not change these two functions.
func (kv *ShardKV) Setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&kv.unreliable, 1)
	} else {
		atomic.StoreInt32(&kv.unreliable, 0)
	}
}

func (kv *ShardKV) isunreliable() bool {
	return atomic.LoadInt32(&kv.unreliable) != 0
}

//
// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//   servers that implement the shardmaster.
// servers[] contains the ports of the servers
//   in this replica group.
// Me is the index of this server in servers[].
//
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Op{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)

	kv.config = shardmaster.Config{Num: -1}
	kv.kvData = make(map[string]string)
	kv.lastSeen = make(map[int64]int64)
	kv.maxInstance = -1
	kv.maxProcessedSeq = -1

	// Reconfiguration
	kv.highestSeen = -1
	kv.reconfigKvData = make(map[string]string)

	// Don't call Join().

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.isdead() == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.isdead() == false {
				if kv.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.isdead() == false {
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.isdead() == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
