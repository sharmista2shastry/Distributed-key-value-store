package viewservice

import (
	"fmt"
	"log"
	"net"
	"net/rpc"
	"os"
	"sync"
	"time"
)

type ViewServer struct {
	mu   sync.Mutex
	l    net.Listener
	dead bool
	me   string

	// Your declarations here.
	lastPing    map[string]time.Time
	currentView View
	idleServer  string
	primaryAck  bool
}

// server Ping RPC handler.

func (vs *ViewServer) Ping(args *PingArgs, reply *PingReply) error {

	// Your code here.
	server := args.Me
	viewNum := args.Viewnum

	vs.mu.Lock()
	defer vs.mu.Unlock()

	// update lastPing map for each server that pings the viewserver
	vs.lastPing[server] = time.Now()

	if server == vs.currentView.Primary {
		// if viewNum = 0 and the server is the primary, then the primary has just restarted after a crash
		if viewNum == 0 {
			vs.currentView.Primary = vs.currentView.Backup
			if vs.idleServer != "" {
				vs.currentView.Backup = vs.idleServer
				vs.idleServer = ""
			} else {
				vs.currentView.Backup = ""
			}
			vs.currentView.Viewnum++
			vs.primaryAck = false
		} else if viewNum == vs.currentView.Viewnum {
			vs.primaryAck = true
		}
	} else if server == vs.currentView.Backup && viewNum == 0 && vs.primaryAck {
		vs.currentView.Backup = vs.idleServer
		vs.idleServer = ""
		vs.currentView.Viewnum++
		vs.primaryAck = false
	} else if server != vs.currentView.Primary && server != vs.currentView.Backup {
		if viewNum == 0 {
			if vs.currentView.Primary == "" {
				vs.currentView.Primary = server
				vs.currentView.Viewnum++
				vs.primaryAck = false
			} else if vs.currentView.Backup == "" {
				vs.currentView.Backup = server
				vs.currentView.Viewnum++
				vs.primaryAck = false
			} else {
				vs.idleServer = server
			}
		} else {
			vs.idleServer = server
		}
	}

	// update the reply
	reply.View = vs.currentView

	return nil
}

// server Get() RPC handler.
func (vs *ViewServer) Get(args *GetArgs, reply *GetReply) error {
	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	reply.View = vs.currentView
	return nil
}

// tick() is called once per PingInterval; it should notice
// if servers have died or recovered, and change the view
// accordingly.
func (vs *ViewServer) tick() {

	// Your code here.
	vs.mu.Lock()
	defer vs.mu.Unlock()

	// check if the primary has crashed
	if time.Since(vs.lastPing[vs.currentView.Primary]) > DeadPings*PingInterval && vs.primaryAck {
		vs.currentView.Primary = vs.currentView.Backup
		if vs.idleServer != "" {
			// Promote the idle server to backup
			vs.currentView.Backup = vs.idleServer
			vs.idleServer = ""
		} else {
			// If there is no idle server, just clear the backup
			vs.currentView.Backup = ""
		}
		vs.currentView.Viewnum++
		vs.primaryAck = false
	}

	// check if the backup has crashed
	if vs.idleServer != "" && time.Since(vs.lastPing[vs.currentView.Backup]) > DeadPings*PingInterval && vs.primaryAck {
		vs.currentView.Backup = vs.idleServer
		vs.idleServer = ""
		vs.currentView.Viewnum++
		vs.primaryAck = false
	}

	// check if the idle server has crashed
	if time.Since(vs.lastPing[vs.idleServer]) > DeadPings*PingInterval {
		vs.idleServer = ""
	} else if vs.currentView.Backup == "" && vs.idleServer != "" && vs.primaryAck {
		vs.currentView.Backup = vs.idleServer
		vs.idleServer = ""
		vs.currentView.Viewnum++
		vs.primaryAck = false
	}
}

// tell the server to shut itself down.
// for testing.
// please don't change this function.
func (vs *ViewServer) Kill() {
	vs.dead = true
	vs.l.Close()
}

func StartServer(me string) *ViewServer {
	vs := new(ViewServer)
	vs.me = me
	// Your vs.* initializations here.
	vs.lastPing = make(map[string]time.Time)
	vs.currentView = View{Viewnum: 0, Primary: "", Backup: ""}
	vs.idleServer = ""
	vs.primaryAck = false

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
