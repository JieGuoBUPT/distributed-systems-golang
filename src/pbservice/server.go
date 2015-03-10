package pbservice

import "net"
import "fmt"
import "net/rpc"
import "log"
import "time"
import "viewservice"
import "sync"
import "sync/atomic"
import "os"
import "syscall"
import "math/rand"

type PBServer struct {
	mu         sync.Mutex
	l          net.Listener
	dead       int32 // for testing
	unreliable int32 // for testing
	me         string
	vs         *viewservice.Clerk

	// TODO: Your declarations here.
	view     viewservice.View
	state    map[string]string
	requests map[int64]int64
}

func (pb *PBServer) IsViewPrimary() bool {
	return pb.me == pb.view.Primary
}

func (pb *PBServer) IsViewBackup() bool {
	return pb.me == pb.view.Backup
}

func (pb *PBServer) ViewHasBackup() bool {
	return pb.view.Backup != ""
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// fmt.Printf(" get \n")

	// TODO: Your code here.

	if pb.IsViewPrimary() && args.ToBackup {
		reply.Err = ErrWrongServer
		return nil
	} else if pb.IsViewBackup() && !args.ToBackup {
		reply.Err = ErrWrongServer
		return nil
	} else {
		if val, ok := pb.state[args.Key]; ok {
			reply.Value = val
		}

		if pb.IsViewPrimary() && pb.ViewHasBackup() {
			var backupReply GetReply

			for {
				ok := call(pb.view.Backup, "PBServer.Get", &GetArgs{Key: args.Key, Seq: args.Seq, ClientId: args.ClientId, ToBackup: true}, &backupReply)
				if backupReply.Err == "" && ok {
					break
				}

				pb.UpdateViewService()
			}
		}
	}

	return nil
}

func (pb *PBServer) PutAppend(args *PutAppendArgs, reply *PutAppendReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()
	// fmt.Printf(" putappend \n")

	// TODO: Your code here.

	if pb.IsViewPrimary() && args.ToBackup {
		reply.Err = ErrWrongServer
		return nil
	} else if pb.IsViewBackup() && !args.ToBackup {
		reply.Err = ErrWrongServer
		return nil
	} else {
		if pb.requests[args.ClientId] < args.Seq {
			if args.Op == AppendOp {
				previous, exists := pb.state[args.Key]
				if !exists {
					previous = ""
				}
				args.Value = previous + args.Value
			}

			pb.requests[args.Seq] = args.Seq

			pb.state[args.Key] = args.Value
		}

		if pb.IsViewPrimary() && pb.ViewHasBackup() {
			var backupReply PutAppendReply
			for {
				ok := call(pb.view.Backup, "PBServer.PutAppend", &PutAppendArgs{Key: args.Key, Value: args.Value, Op: args.Op, Seq: args.Seq, ClientId: args.ClientId, ToBackup: true}, &backupReply)
				if backupReply.Err == "" && ok {
					break
				}

				pb.UpdateViewService()
			}
		}
	}

	return nil
}

//
// ping the viewserver periodically.
// if view changed:
//   transition to new view.
//   manage transfer of state from primary to new backup.
//
func (pb *PBServer) tick() {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	pb.UpdateViewService()
}

func (pb *PBServer) UpdateViewService() {
	view, _ := pb.vs.Ping(pb.view.Viewnum)

	if view != pb.view {
		pb.view = view

		if pb.IsViewPrimary() && pb.ViewHasBackup() {
			args := &SetStateArgs{State: pb.state, Requests: pb.requests}
			var reply SetStateReply
			call(pb.view.Backup, "PBServer.SetState", args, &reply)
		}
	}
}

func (pb *PBServer) SetState(args *SetStateArgs, reply *SetStateReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	if !pb.IsViewPrimary() {
		pb.state = args.State
		pb.requests = args.Requests
		reply.Err = OK
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}

// tell the server to shut itself down.
// please do not change these two functions.
func (pb *PBServer) kill() {
	atomic.StoreInt32(&pb.dead, 1)
	pb.l.Close()
}

func (pb *PBServer) isdead() bool {
	return atomic.LoadInt32(&pb.dead) != 0
}

// please do not change these two functions.
func (pb *PBServer) setunreliable(what bool) {
	if what {
		atomic.StoreInt32(&pb.unreliable, 1)
	} else {
		atomic.StoreInt32(&pb.unreliable, 0)
	}
}

func (pb *PBServer) isunreliable() bool {
	return atomic.LoadInt32(&pb.unreliable) != 0
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	// TODO: Your pb.* initializations here.
	pb.view = viewservice.View{0, "", ""}
	pb.state = make(map[string]string)
	pb.requests = make(map[int64]int64)

	rpcs := rpc.NewServer()
	rpcs.Register(pb)

	os.Remove(pb.me)
	l, e := net.Listen("unix", pb.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	pb.l = l

	// please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for pb.isdead() == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.isdead() == false {
				if pb.isunreliable() && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.isunreliable() && (rand.Int63()%1000) < 200 {
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
			if err != nil && pb.isdead() == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
	}()

	go func() {
		for pb.isdead() == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
	}()

	return pb
}
