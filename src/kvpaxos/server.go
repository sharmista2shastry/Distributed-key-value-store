package kvpaxos

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"strconv"
	"sync"
	"syscall"
	"time"
)

const Debug = 0

func DPrintf(format string, a ...interface{}) (n int, err error) {
	if Debug > 0 {
		log.Printf(format, a...)
	}
	return
}

type Op struct {
	// Your definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Seq           int
	OperationType string
	Key           string
	Value         string
	OperationId   int64
}

type KVPaxos struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool // for testing
	unreliable bool // for testing
	px         *paxos.Paxos

	// Your definitions here.
	KVmap       map[string]string
	updatedTill int              //This is the sequence number till which the kv store is up to date
	ans         map[int64]string //This stores the answers to pass unreliable
}

func (kv *KVPaxos) syncOps(details Op) string {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	// Check if the result is already in the cache
	if val, ok := kv.ans[details.OperationId]; ok {
		return val
	}

	// Initialize variables
	ans := ""
	to := 10 * time.Millisecond

	// Loop until the operation is completed
	for {
		seq := kv.updatedTill
		decided, value := kv.px.Status(seq)
		if decided {
			// Update timeout if necessary
			if to > 1*time.Second {
				to = 1 * time.Second
			}

			// Update state based on operation type
			v := value.(Op)
			if v.OperationType == "Put" {
				kv.KVmap[v.Key] = v.Value
			} else if v.OperationType == "Put hash" {
				ans = kv.KVmap[v.Key]
				kv.KVmap[v.Key] = strconv.Itoa(int(hash(ans + v.Value)))
			} else if v.OperationType == "Get" {
				ans = kv.KVmap[v.Key]
			}

			// Cache the result and check if the operation is done
			kv.ans[v.OperationId] = ans
			if details.OperationId == v.OperationId {
				break
			}

			// Update sequence number and timeout
			seq++
			kv.updatedTill++
			to = 10 * time.Millisecond

		} else {
			// Start the operation and wait for it to complete or timeout
			details.Seq = seq
			kv.px.Start(seq, details)
			time.Sleep(to)
			if to < 20*time.Second {
				to *= 2
			}
		}
	}

	// Mark the operation as done and cache the result
	kv.px.Done(kv.updatedTill)
	kv.updatedTill++
	kv.ans[details.OperationId] = ans

	return ans
}

func (kv *KVPaxos) Get(args *GetArgs, reply *GetReply) error {
	// Create the operation details
	operationDetails := Op{
		OperationType: "Get",
		Key:           args.Key,
		Value:         "",
		OperationId:   args.Nrand,
	}

	// Execute the operation and return the result
	reply.Value = kv.syncOps(operationDetails)

	// Delete the cached result if requested
	if args.Delete != 0 {
		kv.mu.Lock()
		delete(kv.ans, args.Delete)
		kv.mu.Unlock()
	}

	return nil
}

func (kv *KVPaxos) Put(args *PutArgs, reply *PutReply) error {
	// Create operation details
	operationDetails := Op{
		OperationType: "Put",
		Key:           args.Key,
		Value:         args.Value,
		OperationId:   args.Nrand,
	}
	if args.DoHash {
		operationDetails.OperationType = "Put hash"
	}

	// Execute operation and update reply
	reply.PreviousValue = kv.syncOps(operationDetails)

	// Delete operation from cache if necessary
	if args.Delete != 0 {
		kv.mu.Lock()
		delete(kv.ans, args.Delete)
		kv.mu.Unlock()
	}

	return nil
}

// tell the server to shut itself down.
// please do not change this function.
func (kv *KVPaxos) kill() {
	DPrintf("Kill(%d): die\n", kv.me)
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant key/value service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *KVPaxos {
	// call gob.Register on structures you want
	// Go's RPC library to marshall/unmarshall.
	gob.Register(Op{})

	kv := new(KVPaxos)
	kv.me = me

	// Your initialization code here.
	kv.KVmap = make(map[string]string)
	kv.ans = make(map[int64]string)
	kv.updatedTill = 0
	// go kv.fetchOps()

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
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && kv.dead == false {
				fmt.Printf("KVPaxos(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	return kv
}
