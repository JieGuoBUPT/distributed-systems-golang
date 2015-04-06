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
import cryptorand "crypto/rand"
import "math/big"
import "math"
import "time"

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := cryptorand.Int(cryptorand.Reader, max)
	x := bigx.Int64()
	return x
}

type ShardMaster struct {
	mu         sync.Mutex
	state_mu   sync.Mutex
	l          net.Listener
	me         int
	dead       int32 // for testing
	unreliable int32 // for testing
	px         *paxos.Paxos

	configs     []Config // indexed by config num
	prev_config int
	next_config int
}

type OpType string

const (
	Join  OpType = "Join"
	Leave OpType = "Leave"
	Move  OpType = "Move"
	Query OpType = "Query"
	NoOp  OpType = "NoOp"
)

type Op struct {
	// Your data here.
	Id    int64
	Type  OpType
	Args  interface{}
	Reply interface{}
}

func MakeNewConfig(old_config Config) Config {
	// copy old shards
	new_shards := [NShards]int64{}
	for k, v := range old_config.Shards {
		new_shards[k] = v
	}

	//copy old groups
	new_groups := make(map[int64][]string)
	for k, v := range old_config.Groups {
		new_groups[k] = v
	}

	return Config{Num: old_config.Num + 1, Shards: new_shards, Groups: new_groups}
}

func (sm *ShardMaster) FindMin(groups_to_shards map[int64]int) (int64, int) {
	min_group := int64(0)
	min_shards := NShards * 2

	for group, shards := range groups_to_shards {
		if shards < min_shards {
			min_group = group
			min_shards = shards
		}
	}

	return min_group, min_shards
}

func (sm *ShardMaster) FindMax(groups_to_shards map[int64]int) (int64, int) {
	max_group := int64(0)
	max_shards := 0

	for group, shards := range groups_to_shards {
		if shards > max_shards {
			max_group = group
			max_shards = shards
		}
	}

	return max_group, max_shards
}

func (sm *ShardMaster) FindLeastLoadedGroup(groups_to_shards map[int64]int) int64 {
	min_group, _ := sm.FindMin(groups_to_shards)
	return min_group
}

func (sm *ShardMaster) FindMostLoadedGroup(groups_to_shards map[int64]int) int64 {
	max_group, _ := sm.FindMax(groups_to_shards)
	return max_group
}

func (sm *ShardMaster) FindFewestNumberShards(groups_to_shards map[int64]int) int {
	_, min_shards := sm.FindMin(groups_to_shards)
	return min_shards
}

func (sm *ShardMaster) FindMostNumberShards(groups_to_shards map[int64]int) int {
	_, max_shards := sm.FindMax(groups_to_shards)
	return max_shards
}

func (sm *ShardMaster) Balance(config Config) Config {
	max_shards_per_group := int(math.Ceil(float64(NShards) / float64(len(config.Groups))))
	min_shards_per_group := int(NShards / len(config.Groups))

	groups_to_shards := make(map[int64]int)

	// initialize groups_to_shards. Get groups from new groups. Get num_shards from config.Shards
	for group, _ := range config.Groups {
		groups_to_shards[group] = 0
	}

	for _, group := range config.Shards {
		_, exists := groups_to_shards[group]
		if exists {
			groups_to_shards[group]++
		}
	}

	for shard, group := range config.Shards {
		if group == 0 {
			min_group := sm.FindLeastLoadedGroup(groups_to_shards)
			config.Shards[shard] = min_group
			groups_to_shards[min_group]++
		}
	}

	for sm.FindMostNumberShards(groups_to_shards) > max_shards_per_group {
		for shard, group := range config.Shards {
			if groups_to_shards[group] > max_shards_per_group {
				min_group := sm.FindLeastLoadedGroup(groups_to_shards)
				config.Shards[shard] = min_group
				groups_to_shards[min_group]++
				groups_to_shards[group]--
			}
		}
	}

	for sm.FindFewestNumberShards(groups_to_shards) < min_shards_per_group {
		for shard, group := range config.Shards {
			if group == sm.FindMostLoadedGroup(groups_to_shards) {
				min_group := sm.FindLeastLoadedGroup(groups_to_shards)
				config.Shards[shard] = min_group
				groups_to_shards[min_group]++
				groups_to_shards[group]--
			}
		}
	}

	return config
}

func (sm *ShardMaster) Poll(config_num int) interface{} {
	to := 10 * time.Millisecond
	for {
		fate, value := sm.px.Status(config_num)

		if fate == paxos.Decided {
			return value
		}

		time.Sleep(to)
		if to < 10*time.Second {
			to *= 2
		}
	}
	return nil
}

func (sm *ShardMaster) LogOp(op Op) error {
	sm.state_mu.Lock()
	defer sm.state_mu.Unlock()

	for {
		sm.px.Start(sm.next_config, op)
		var received_op Op
		received_op = sm.Poll(sm.next_config).(Op)
		if received_op.Id == op.Id {
			break
		} else {
			sm.next_config++
		}
	}
	return nil
}

func (sm *ShardMaster) ProcessLog() Config {
	sm.state_mu.Lock()
	defer sm.state_mu.Unlock()

	var cur_config Config
	for i := sm.prev_config; i <= sm.next_config; i++ {
		_ = sm.Poll(i)
		var fate, rcv_op = sm.px.Status(i)
		var op *Op
		switch rcv_op.(type) {
		case Op:
			temp := rcv_op.(Op)
			op = &temp
		case *Op:
			op = rcv_op.(*Op)
		}

		if fate == paxos.Decided {
			cur_config = sm.DoOp(op)
		}
	}
	return cur_config
}

func (sm *ShardMaster) DoOp(op *Op) Config {
	var config Config

	switch op.Type {
	case Join:
		config = sm.DoJoinOp(op)
	case Leave:
		config = sm.DoLeaveOp(op)
	case Move:
		config = sm.DoMoveOp(op)
	case Query:
		config = sm.DoQueryOp(op)
	}

	return config
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Id: nrand(), Type: Join, Args: *args, Reply: *reply}

	sm.prev_config = sm.next_config
	sm.LogOp(op)

	sm.ProcessLog()
	sm.px.Done(sm.next_config)
	sm.next_config++

	return nil
}

func (sm *ShardMaster) DoJoinOp(op *Op) Config {
	gid := op.Args.(JoinArgs).GID
	servers := op.Args.(JoinArgs).Servers

	old_config := sm.configs[len(sm.configs)-1]

	tmp_config := MakeNewConfig(old_config)
	tmp_config.Groups[gid] = servers // add new group

	balanced_config := sm.Balance(tmp_config)

	sm.configs = append(sm.configs, balanced_config)

	return balanced_config
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Id: nrand(), Type: Leave, Args: *args, Reply: *reply}

	sm.prev_config = sm.next_config
	sm.LogOp(op)

	sm.ProcessLog()
	sm.px.Done(sm.next_config)
	sm.next_config++

	return nil
}

func (sm *ShardMaster) DoLeaveOp(op *Op) Config {
	gid := op.Args.(LeaveArgs).GID
	old_config := sm.configs[len(sm.configs)-1]

	tmp_config := MakeNewConfig(old_config)
	delete(tmp_config.Groups, gid) // delete leaving group

	for shard, group := range tmp_config.Shards {
		if group == gid {
			tmp_config.Shards[shard] = 0
		}
	}

	balanced_config := sm.Balance(tmp_config)

	sm.configs = append(sm.configs, balanced_config)

	return balanced_config
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Id: nrand(), Type: Move, Args: *args, Reply: *reply}

	sm.prev_config = sm.next_config
	sm.LogOp(op)

	sm.ProcessLog()
	sm.px.Done(sm.next_config)
	sm.next_config++

	return nil
}

func (sm *ShardMaster) DoMoveOp(op *Op) Config {
	gid := op.Args.(MoveArgs).GID
	shard := op.Args.(MoveArgs).Shard
	old_config := sm.configs[len(sm.configs)-1]

	tmp_config := MakeNewConfig(old_config)

	tmp_config.Shards[shard] = gid

	sm.configs = append(sm.configs, tmp_config)

	return tmp_config
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	sm.mu.Lock()
	defer sm.mu.Unlock()

	op := Op{Id: nrand(), Type: Query, Args: *args, Reply: *reply}

	sm.prev_config = sm.next_config
	sm.LogOp(op)

	reply.Config = sm.ProcessLog()
	sm.px.Done(sm.next_config)
	sm.next_config++

	return nil
}

func (sm *ShardMaster) DoQueryOp(op *Op) Config {
	conf_num := op.Args.(QueryArgs).Num
	reply := op.Reply.(QueryReply)

	if conf_num == -1 || conf_num > len(sm.configs)-1 {
		conf_num = len(sm.configs) - 1
	}

	reply.Config = sm.configs[conf_num]

	return reply.Config
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

	sm.prev_config = 1
	sm.next_config = 1

	rpcs := rpc.NewServer()

	gob.Register(Op{})
	gob.Register(JoinArgs{})
	gob.Register(JoinReply{})
	gob.Register(LeaveArgs{})
	gob.Register(LeaveReply{})
	gob.Register(MoveArgs{})
	gob.Register(MoveReply{})
	gob.Register(QueryArgs{})
	gob.Register(QueryReply{})

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
