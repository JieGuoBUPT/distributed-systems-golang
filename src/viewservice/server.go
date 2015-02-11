package viewservice

import "net"
import "net/rpc"
import "log"
import "time"
import "sync"
import "fmt"
import "os"
import "sync/atomic"

type ViewServer struct {
	mu       sync.Mutex
	l        net.Listener
	dead     bool  // for testing
	rpccount int32 // for testing
	me       string

	// Your declarations here.
	currentView View
	lastSeen    map[string]time.Time
	ack         bool
}

//
// Increment the view number and replace the servers with what is given
// returns true if the view was changed, false otherwise
//
func (vs *ViewServer) updateView(primary string, backup string) bool {
	if vs.currentView.Primary != primary || vs.currentView.Backup != backup {
		vs.currentView = View{
			Viewnum: vs.currentView.Viewnum + 1,
			Primary: primary,
			Backup:  backup
		}

		vs.ack = false
		return true
	}

	return false
}

//
// server Ping RPC handler.
//
func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	server, viewnum := args.Me, args.Viewnum

	switch server {
	case vs.currentView.Primary:
		if viewnum == vs.currentView.Viewnum {
			vs.ack = true
			vs.lastSeen[vs.currentView.Primary] = time.Now()
		} else {
			vs.replacePrimary()
		}
	case vs.currentView.Backup:
		if viewnum == vs.currentView.Viewnum {
			vs.lastSeen[vs.currentView.Backup] = time.Now()
		} else {
			vs.removeBackup()
		}
	default:
		vs.addServer(server)
	}

	reply.View = vs.currentView

	return nil
}

//
// server Get() RPC handler.
//
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.currentView

	return nil
}

//
// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
//
func (vs *ViewServer) tick() {
	vs.mu.Lock()
	defer vs.mu.Unlock()

	for server, timestamp := range vs.lastSeen {
		if time.Since(timestamp) >= DeadPings*PingInterval {
			vs.removeServer(server)
		}
	}
}

//
// addServer(server string) attemptes to remove the given server,
// returns true if the view was changed, false otherwise
// NOTE: only one server and one primary are supported
//
func (vs *ViewServer) addServer(server string) bool {
	if !vs.ack {
		return false
	}

	if vs.currentView.Primary == "" {
		return vs.updateView(server, "")
	} else if vs.currentView.Backup == "" {
		return vs.updateView(vs.currentView.Primary, server)
	}

	return false
}

//
// removeServer(server string) attemptes to remove the given server,
// returns true if the view was changed, false otherwise
//
func (vs *ViewServer) removeServer(server string) bool {
	switch server {
	case vs.currentView.Primary:
		return vs.replacePrimary()
	case vs.currentView.Backup:
		return vs.removeBackup()
	}
	return false
}

//
// replacePrimary() attempts to replace the primary with the backup
// returns true if the view was changed, false otherwise
//
func (vs *ViewServer) replacePrimary() bool {
	if !vs.ack || vs.currentView.Backup == "" {
		return false
	}

	return vs.updateView(vs.currentView.Backup, "")
}

//
// replacePrimary() attempts to remove the backup
// returns true if the view was changed, false otherwise
//
func (vs *ViewServer) removeBackup() bool {
	if !vs.ack {
		return false
	}

	return vs.updateView(vs.currentView.Primary, "")
}

//
// tell the server to shut itself down.
// for testing.
// please don't change this function.
//
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

// please don't change this function.
func (vs *ViewServer) GetRPCCount() int32 {
	return atomic.LoadInt32(&vs.rpccount)
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.currentView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.lastSeen = make(map[string]time.Time)
	vs.ack = true

	// tell net/rpc about our RPC server and handlers.
	rpcs := rpc.NewServer()
	rpcs.Register(vs)

	// prepare to receive connections from clients.
	// change "unix" to "tcp" to use over a network.
	os.Remove(vs.me) // only needed for "unix"
	l, e := net.Listen("unix", vs.me)
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	vs.l = l

	// please don't change any of the following code,
	// or do anything to subvert it.

	// create a thread to accept RPC connections from clients.
	go func() {
		for vs.dead == false {
			conn, err := vs.l.Accept()
			if err == nil && vs.dead == false {
				atomic.AddInt32(&vs.rpccount, 1)
				go rpcs.ServeConn(conn)
			} else if err == nil {
				conn.Close()
			}
			if err != nil && vs.dead == false {
				fmt.Printf("ViewServer(%v) accept: %v\n", me, err.Error())
				vs.Kill()
			}
		}
	}()

	// create a thread to call tick() periodically.
	go func() {
		for vs.dead == false {
			vs.tick()
			time.Sleep(PingInterval)
		}
	}()

	return vs
}
