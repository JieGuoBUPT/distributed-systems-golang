package kvpaxos

import "net/rpc"
import "crypto/rand"
import "math/big"
import "time"

import "fmt"

type Clerk struct {
	servers []string
	// You will have to modify this struct.
	id  int64
	seq int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(servers []string) *Clerk {
	ck := new(Clerk)
	ck.servers = servers
	// You'll have to add code here.
	ck.id = nrand()
	ck.seq = 0
	return ck
}

//
// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will return an
// error after a while if the server is dead.
// don't provide your own time-out mechanism.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
//
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

//
// fetch the current value for a key.
// returns "" if the key does not exist.
// keeps trying forever in the face of all other errors.
//
func (ck *Clerk) Get(key string) string {
	// You will have to modify this function.
	ck.seq++
	server := int(nrand()) % len(ck.servers)

	args := &GetArgs{Key: key, Seq: ck.seq, ClientId: ck.id}
	var reply GetReply

	to := InitialBackoff
	for {
		ok := call(ck.servers[server], "KVPaxos.Get", args, &reply)

		if ok && reply.Err == OK {
			DPrintf("client %d, seq %d received from server %d get reply! OK: %t", ck.id, ck.seq, server, ok)
			break
		}

		DPrintf("client %d, seq %d received from server %d failed get reply! OK: %t", ck.id, ck.seq, server, ok)

		time.Sleep(to)
		if to < MaxBackoff {
			to *= 2
		}

		server = (server + 1) % len(ck.servers)
		DPrintf("client %d, seq %d trying get again with server %d", ck.id, ck.seq, server)
	}
	return reply.Value
}

//
// shared by Put and Append.
//
func (ck *Clerk) PutAppend(key string, value string, op OpType) {
	// You will have to modify this function.
	ck.seq++
	server := int(nrand()) % len(ck.servers)

	args := &PutAppendArgs{Key: key, Value: value, OpType: op, Seq: ck.seq, ClientId: ck.id}
	var reply PutAppendReply

	to := InitialBackoff
	for {
		ok := call(ck.servers[server], "KVPaxos.PutAppend", args, &reply)

		if ok && reply.Err == OK {
			DPrintf("client %d, seq %d received from server %d put reply! OK: %t", ck.id, ck.seq, server, ok)
			return
		}

		DPrintf("client %d, seq %d received from server %d failed put reply! OK: %t", ck.id, ck.seq, server, ok)

		time.Sleep(to)
		if to < MaxBackoff {
			to *= 2
		}

		server = (server + 1) % len(ck.servers)
		DPrintf("client %d, seq %d trying put again with server %d", ck.id, ck.seq, server)
	}
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
