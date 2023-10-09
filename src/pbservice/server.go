package pbservice

import (
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
	"time"
	"viewservice"
)

// Debugging
const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		n, err = fmt.Printf(format, a...)
	}
	return
}

type PBServer struct {
	l          net.Listener
	dead       bool // for testing
	unreliable bool // for testing
	me         string
	vs         *viewservice.Clerk
	done       sync.WaitGroup
	finish     chan interface{}
	// Your declarations here.
	mu          sync.Mutex
	kv          map[string]string
	executedOps map[int64]string
	View        viewservice.View
	putComplete sync.WaitGroup
	isSetup     bool
}

func (pb *PBServer) Put(args *PutArgs, reply *PutReply) error {
	// Sleep for a short time to simulate network delay.
	time.Sleep(6 * time.Millisecond)

	pb.mu.Lock()

	// Wait for any previous Put operations to complete before starting a new one.
	pb.putComplete.Wait()
	pb.putComplete.Add(1)

	// Check if the operation has already been executed, and return the previous value if it has.
	if previousValue, ok := pb.executedOps[args.Nrand]; ok {
		reply.PreviousValue = previousValue
		pb.putComplete.Done()
		pb.mu.Unlock()
		return nil
	}

	// If the DoHash flag is set, hash the old value and the new value and store the result as the new value.
	// Otherwise, simply store the new value.
	if args.DoHash {
		oldValue := pb.kv[args.Key]
		args.Value = strconv.Itoa(int(hash(oldValue + args.Value)))
		reply.PreviousValue = oldValue
		pb.executedOps[args.Nrand] = oldValue
	} else {
		reply.PreviousValue = pb.kv[args.Key]
		pb.executedOps[args.Nrand] = ""
	}

	// Store the new value in the key-value store.
	pb.kv[args.Key] = args.Value

	backup := pb.View.Backup
	primary := pb.View.Primary

	pb.mu.Unlock()

	// If there is no backup server, we're done.
	if backup == "" {
		pb.putComplete.Done()
		return nil
	}

	// If this server is the primary, forward the Put operation to the backup server(s).
	if primary == pb.me {
		args.DoHash = false
		for {
			backup = pb.View.Backup
			if backup == "" {
				break
			}
			if ok := call(backup, "PBServer.Put", args, &PutReply{}); ok {
				break
			}
		}
		pb.putComplete.Done()
		return nil
	}

	// If this server is the backup, we're done.
	if backup == pb.me {
		pb.putComplete.Done()
		return nil
	}

	// If this server is neither the primary nor the backup, return an error.
	reply.Err = "PutError"
	pb.putComplete.Done()
	return nil
}

func (pb *PBServer) Get(args *GetArgs, reply *GetReply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// If this server is the primary, return the value associated with the given key.
	// Otherwise, return an error indicating that this server is not the primary.
	if pb.View.Primary == pb.me {
		reply.Value = pb.kv[args.Key]
	} else {
		reply.Err = ErrWrongServer
	}

	return nil
}

func (pb *PBServer) SyncBackup(data *KeyValueStore, reply *KVreply) error {
	pb.mu.Lock()
	defer pb.mu.Unlock()

	// Copy the key-value pairs and executed operations from the primary server to this backup server.
	pb.kv = data.Values
	pb.executedOps = data.ExecutedOps

	reply.Value = "ok"

	// Set the isSetup flag to true to indicate that this backup server has been synchronized with the primary server.
	pb.isSetup = true

	return nil
}

func (pb *PBServer) tick() {
	// Ping the viewserver to get the latest view.
	v, _ := pb.vs.Ping(pb.View.Viewnum)

	// If this server is the primary, set the isSetup flag based on the view number.
	// If this server is the backup, set the isSetup flag to false.
	if v.Primary == pb.me {
		pb.isSetup = (pb.View.Viewnum == 0)
	} else if v.Backup == pb.me {
		pb.isSetup = false
	}

	// If this server is the primary and the backup has changed, synchronize the backup with this server.
	if v.Primary == pb.me && v.Backup != pb.View.Backup && v.Backup != "" {
		pb.mu.Lock()
		defer pb.mu.Unlock()

		args := KeyValueStore{
			Values:      pb.kv,
			ExecutedOps: pb.executedOps,
		}

		// Send a SyncBackup RPC to the backup server until it succeeds or the backup server changes.
		var reply KVreply
		for !call(v.Backup, "PBServer.SyncBackup", args, &reply) {
			if pb.View.Backup == "" {
				break
			}
		}
	}

	pb.View = v
}

// tell the server to shut itself down.
// please do not change this function.
func (pb *PBServer) kill() {
	pb.dead = true
	pb.l.Close()
}

func StartServer(vshost string, me string) *PBServer {
	pb := new(PBServer)
	pb.me = me
	pb.vs = viewservice.MakeClerk(me, vshost)
	pb.finish = make(chan interface{})
	// Your pb.* initializations here.

	pb.View.Viewnum = 0
	pb.View.Primary = ""
	pb.View.Backup = ""
	pb.kv = make(map[string]string)
	pb.executedOps = make(map[int64]string)
	pb.isSetup = false

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
		for pb.dead == false {
			conn, err := pb.l.Accept()
			if err == nil && pb.dead == false {
				if pb.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if pb.unreliable && (rand.Int63()%1000) < 200 {
					// process the request but force discard of reply.
					c1 := conn.(*net.UnixConn)
					f, _ := c1.File()
					err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
					if err != nil {
						fmt.Printf("shutdown: %v\n", err)
					}
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				} else {
					pb.done.Add(1)
					go func() {
						rpcs.ServeConn(conn)
						pb.done.Done()
					}()
				}
			} else if err == nil {
				conn.Close()
			}
			if err != nil && pb.dead == false {
				fmt.Printf("PBServer(%v) accept: %v\n", me, err.Error())
				pb.kill()
			}
		}
		DPrintf("%s: wait until all request are done\n", pb.me)
		pb.done.Wait()
		// If you have an additional thread in your solution, you could
		// have it read to the finish channel to hear when to terminate.
		close(pb.finish)
	}()

	pb.done.Add(1)
	go func() {
		for pb.dead == false {
			pb.tick()
			time.Sleep(viewservice.PingInterval)
		}
		pb.done.Done()
	}()

	return pb
}
