package pbservice

import "viewservice"
import "net/rpc"
import "fmt"
import "time"
import "crypto/rand"
import "math/big"

type Clerk struct {
	vs *viewservice.Clerk

	// Your declarations here
	id   int64
	seq  int64
	view viewservice.View
}

// this may come in handy.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)

	// Your ck.* initializations here
	ck.id = nrand()
	ck.seq = 0
	ck.view = viewservice.View{Viewnum: 0}

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
// You should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
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
// fetch a key's value from the current primary;
// if they key has never been set, return "".
// Get() must keep trying until it either the
// primary replies with the value or the primary
// says the key doesn't exist (has never been Put().
//
func (ck *Clerk) Get(key string) string {
	ck.seq++

	args := &GetArgs{Key: key, Seq: ck.seq, ClientId: ck.id, ToBackup: false}
	var reply GetReply

	ok := call(ck.view.Primary, "PBServer.Get", args, &reply)
	for !ok || reply.Err != OK {
		ck.view, _ = ck.vs.Ping(ck.view.Viewnum)
		time.Sleep(viewservice.PingInterval)

		ok = call(ck.view.Primary, "PBServer.Get", args, &reply)
	}

	return reply.Value
}

//
// send a Put or Append RPC
//
func (ck *Clerk) PutAppend(key string, value string, op Op) {
	ck.seq++

	args := &PutAppendArgs{Key: key, Value: value, Op: op, Seq: ck.seq, ClientId: ck.id, ToBackup: false}
	var reply PutAppendReply

	ok := call(ck.view.Primary, "PBServer.PutAppend", args, &reply)
	for !ok || reply.Err != OK {
		ck.view, _ = ck.vs.Ping(ck.view.Viewnum)
		time.Sleep(viewservice.PingInterval)

		ok = call(ck.view.Primary, "PBServer.PutAppend", args, &reply)
	}
}

//
// tell the primary to update key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Put(key string, value string) {
	ck.PutAppend(key, value, PutOp)
}

//
// tell the primary to append to key's value.
// must keep trying until it succeeds.
//
func (ck *Clerk) Append(key string, value string) {
	ck.PutAppend(key, value, AppendOp)
}
