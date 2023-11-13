package shardmaster

import (
	crand "crypto/rand"
	"encoding/gob"
	"fmt"
	"log"
	"math/big"
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

type ShardMaster struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool
	unreliable bool
	px         *paxos.Paxos

	configurations []Config
	maxSeq         int
}

type Op struct {
	// Your data here.
	OperationID   int64
	OperationType string
	groupID       int64
	servers       []string
	shardNo       int
	queryNo       int
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := crand.Int(crand.Reader, max)
	x := bigx.Int64()
	return x
}

func (sm *ShardMaster) Join(args *JoinArgs, reply *JoinReply) error {
	// Your code here.

	// Create a new Op struct
	op := Op{
		OperationID:   nrand(),
		OperationType: "Join",
		groupID:       args.GID,
		servers:       args.Servers,
	}

	// Call syncOps method
	Err := sm.syncOperations(op)
	if Err != nil {
		return Err
	}

	return nil
}

func (sm *ShardMaster) Leave(args *LeaveArgs, reply *LeaveReply) error {
	// Your code here.
	// Create a new Op struct
	op := Op{
		OperationID:   nrand(),
		OperationType: "Leave",
		groupID:       args.GID,
	}

	sm.syncOperations(op)

	return nil
}

func (sm *ShardMaster) Move(args *MoveArgs, reply *MoveReply) error {
	// Your code here.
	op := Op{
		OperationID:   nrand(),
		OperationType: "Move",
		groupID:       args.GID,
		shardNo:       args.Shard,
	}

	sm.syncOperations(op)

	return nil
}

func (sm *ShardMaster) Query(args *QueryArgs, reply *QueryReply) error {
	// Your code here.

	op := Op{
		OperationID:   nrand(),
		OperationType: "Query",
		queryNo:       args.Num,
	}

	sm.syncOperations(op)

	if args.Num == -1 || args.Num > (len(sm.configurations)-1) {
		args.Num = len(sm.configurations) - 1
	}
	reply.Config = sm.configurations[args.Num]
	// fmt.Println(sm.configs)

	return nil
}

// please don't change this function.
func (sm *ShardMaster) Kill() {
	sm.dead = true
	sm.l.Close()
	sm.px.Kill()
}

func (sm *ShardMaster) syncOperations(op Op) error {

	// Declare constants and configure defaults upfront
	const (
		maxWait     = 10 * time.Second
		backoffRate = 2
	)
	var (
		waitDuration = 10 * time.Millisecond
	)

	for {

		// Lock mutex for the entire loop
		sm.mu.Lock()

		curConfig := sm.configurations[len(sm.configurations)-1]
		seq := sm.maxSeq

		decided, value := sm.px.Status(seq)

		if decided {

			// Use a meaningful variable name
			newOp := value.(Op)

			// Initialize new config properly
			var newConfig Config
			newConfig.Groups = make(map[int64][]string)

			// Copy current groups
			for key, value := range curConfig.Groups {
				newConfig.Groups[key] = value
			}

			// Update config fields
			newConfig.Num = curConfig.Num + 1
			newConfig.Shards = curConfig.Shards

			switch newOp.OperationType {
			case "Join":
				newConfig.Groups[newOp.groupID] = newOp.servers
				newConfig = sm.balanceShards(newConfig)
				sm.configurations = append(sm.configurations, newConfig)

			case "Leave":
				delete(newConfig.Groups, newOp.groupID)
				newConfig = sm.balanceShards(newConfig)
				sm.configurations = append(sm.configurations, newConfig)

			case "Move":
				newConfig.Shards[newOp.shardNo] = newOp.groupID
				sm.configurations = append(sm.configurations, newConfig)
			}

			if op.OperationID == newOp.OperationID {
				break
			}

			sm.maxSeq++

		} else {

			sm.px.Start(seq, op)

			// Exponential backoff
			time.Sleep(waitDuration)
			waitDuration *= backoffRate
			if waitDuration > maxWait {
				waitDuration = maxWait
			}

		}

		sm.mu.Unlock()

	}

	sm.px.Done(sm.maxSeq)
	sm.maxSeq++

	return nil
}

func (sm *ShardMaster) balanceShards(config Config) Config {
	if len(config.Groups) == 0 {
		return config
	}

	numGroups := len(config.Groups)
	numberOfShards := 10 / numGroups
	extraShards := 10 % numGroups
	lastSeqNum := len(sm.configurations) - 1

	shardsPerGroup := make(map[int64]int)

	// Count number of shards served by a group
	for _, shardGroup := range config.Shards {
		if group, ok := config.Groups[shardGroup]; ok {
			groupID, _ := strconv.ParseInt(group[0], 10, 64)
			shardsPerGroup[groupID]++
		}
	}

	// Remove excess shards
	for groupID, servedShards := range shardsPerGroup {
		if servedShards == 0 || groupID == 0 {
			continue
		}

		if servedShards == numberOfShards+1 && extraShards > 0 {
			extraShards--
		} else if servedShards >= numberOfShards+1 {
			removeShards := servedShards - numberOfShards
			if extraShards > 0 {
				removeShards--
				extraShards--
			}
			for idx, shardGroup := range config.Shards {
				if shardGroup == groupID {
					config.Shards[idx] = 0
					removeShards--
				}
				if removeShards == 0 {
					break
				}
			}
		}
	}

	// Add missing shards
	for groupID := range config.Groups {
		servedShards := shardsPerGroup[groupID]
		if servedShards < numberOfShards || (servedShards == numberOfShards && extraShards > 0) {
			addShards := numberOfShards - servedShards
			if extraShards > 0 {
				addShards++
				extraShards--
			}
			for idx := range sm.configurations[lastSeqNum].Shards {
				if config.Shards[idx] == 0 {
					config.Shards[idx] = groupID
					addShards--
				}
				if addShards == 0 {
					break
				}
			}
		}
	}

	return config
}

// servers[] contains the ports of the set of
// servers that will cooperate via Paxos to
// form the fault-tolerant shardmaster service.
// me is the index of the current server in servers[].
func StartServer(servers []string, me int) *ShardMaster {
	gob.Register(Op{})

	sm := new(ShardMaster)
	sm.me = me

	sm.configurations = make([]Config, 1)
	sm.configurations[0].Groups = map[int64][]string{}
	sm.maxSeq = 0

	rpcs := rpc.NewServer()
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
		for sm.dead == false {
			conn, err := sm.l.Accept()
			if err == nil && sm.dead == false {
				if sm.unreliable && (rand.Int63()%1000) < 100 {
					// discard the request.
					conn.Close()
				} else if sm.unreliable && (rand.Int63()%1000) < 200 {
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
			if err != nil && sm.dead == false {
				fmt.Printf("ShardMaster(%v) accept: %v\n", me, err.Error())
				sm.Kill()
			}
		}
	}()

	return sm
}
