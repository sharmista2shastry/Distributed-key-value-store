package shardkv

import (
	"encoding/gob"
	"fmt"
	"log"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"paxos"
	"shardmaster"
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

type Operation struct {
	SeqNum       int
	Type         string
	Key          string
	Value        string
	ID           int64
	NewConfig    [shardmaster.NShards]bool
	SendShardsTo map[int64][]int
	Num          int
	DataMap      [shardmaster.NShards]map[string]string
	ShardsToCopy []int
	AnswerMap    map[int64]string
	Stale        [shardmaster.NShards]bool
}

type ShardKV struct {
	mu         sync.Mutex
	l          net.Listener
	me         int
	dead       bool
	unreliable bool
	sm         *shardmaster.Clerk
	px         *paxos.Paxos

	gid int64

	syncedTill  int
	dataStore   [shardmaster.NShards]map[string]string
	answerStore map[int64]string
	currentKeys [shardmaster.NShards]bool
	Groups      map[int64][]string
	isNew       bool
	staleData   [shardmaster.NShards]bool
	Num         int

	KnownConfig int
}

func (kv *ShardKV) syncOps(details Operation) string {
	to := 10 * time.Millisecond
	val, ok := kv.answerStore[details.ID]
	if ok {
		return val
	}

	ans := ""
	for {
		seq := kv.syncedTill
		decided, value := kv.px.Status(seq)
		if decided {
			if to > 1*time.Second {
				to = 1 * time.Second
			}
			v := value.(Operation)
			ans = ""

			switch v.Type {
			case "Recieve":
				for ansKey, ansValue := range v.AnswerMap {
					kv.answerStore[ansKey] = ansValue
				}
				kv.answerStore[v.ID] = ans

				for _, shard := range v.ShardsToCopy {
					kv.dataStore[shard] = v.DataMap[shard]
					kv.staleData[shard] = false
				}

				if details.Type == "Get" ||
					details.Type == "Put hash" || details.Type == "Put" {
					shard := key2shard(details.Key)
					if kv.staleData[shard] {
						return "KeySent"
					}
				}

			case "Config":
				kv.answerStore[v.ID] = ans
				if kv.Num < v.Num {
					kv.sendShards(v.SendShardsTo, v.Num, v.ID)
					kv.currentKeys = v.NewConfig
					kv.Num = v.Num

					for i := 0; i < shardmaster.NShards; i++ {
						kv.staleData[i] = kv.staleData[i] || v.Stale[i]
					}
				}

			case "Put":
				shard := key2shard(v.Key)
				ans = ""
				if kv.currentKeys[shard] || !kv.staleData[shard] {
					kv.dataStore[shard][v.Key] = v.Value
				} else {
					ans = "KeySent"
				}

			case "Put hash":
				shard := key2shard(v.Key)
				if kv.currentKeys[shard] || !kv.staleData[shard] {
					ans = kv.dataStore[shard][v.Key]
					kv.dataStore[shard][v.Key] = strconv.Itoa(int(hash(ans + v.Value)))
				} else {
					ans = "KeySent"
				}

			case "Get":
				shard := key2shard(v.Key)
				if kv.currentKeys[shard] || !kv.staleData[shard] {
					ans = kv.dataStore[shard][v.Key]
				} else {
					ans = "KeySent"
				}
			}

			if ans != "KeySent" {
				kv.answerStore[v.ID] = ans
			}

			if details.ID == v.ID {
				break
			}

			seq++
			kv.syncedTill++
		} else {
			details.SeqNum = seq
			kv.px.Start(seq, details)
			time.Sleep(to)
			if to < 10*time.Second {
				to *= time.Duration(2)
			}
		}
	}

	kv.syncedTill++
	return ans
}

func (kv *ShardKV) sendShards(shardsToSend map[int64][]int, Num int, operationId int64) {
	for gid, shards := range shardsToSend {
		go kv.sendShardBatch(shards, gid, Num, operationId)
	}
}

func (kv *ShardKV) sendShardBatch(shards []int, gid int64, Num int, operationId int64) {
	args := kv.buildSendArgs(shards, Num, operationId)
	kv.trySendShardBatch(args, gid)
}

func (kv *ShardKV) buildSendArgs(shards []int, Num int, operationId int64) *SendArgs {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	args := &SendArgs{
		ShardMaps:      kv.generateShardMaps(),
		ShardsToCopy:   shards,
		NumberOfShards: Num,
		RandomNumber:   operationId,
		ResponseMap:    kv.generateResponseMap(),
	}

	return args
}

func (kv *ShardKV) generateShardMaps() [shardmaster.NShards]map[string]string {
	shardMaps := [shardmaster.NShards]map[string]string{}

	for s := 0; s < shardmaster.NShards; s++ {
		shardMaps[s] = make(map[string]string)
		for key, value := range kv.dataStore[s] {
			shardMaps[s][key] = value
		}
	}

	return shardMaps
}

func (kv *ShardKV) generateResponseMap() map[int64]string {
	responseMap := make(map[int64]string)
	for k, v := range kv.answerStore {
		responseMap[k] = v
	}

	return responseMap
}

func (kv *ShardKV) trySendShardBatch(args *SendArgs, gid int64) {
	var reply GetReply

	for {
		for _, server := range kv.Groups[gid] {
			if call(server, "ShardKV.RecieveShards", args, &reply) && (reply.Err == OK || reply.Err == ErrInvalidGroup) {
				return
			}
		}
	}
}

func (kv *ShardKV) RecieveShards(args *SendArgs, reply *SendReply) error {
	if kv.Num > args.NumberOfShards {
		reply.Err = OK
		return nil
	}

	kv.mu.Lock()
	defer kv.mu.Unlock()

	operationDetails := &Operation{
		DataMap:      args.ShardMaps,
		Type:         "Recieve",
		ShardsToCopy: args.ShardsToCopy,
		ID:           args.RandomNumber,
		Num:          args.NumberOfShards,
		AnswerMap:    args.ResponseMap,
	}

	kv.syncOps(*operationDetails)

	reply.Err = OK
	return nil
}

func (kv *ShardKV) Get(args *GetArgs, reply *GetReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(args.Key)
	servingKey := kv.currentKeys[shard]

	if !servingKey || kv.staleData[shard] {
		reply.Err = ErrInvalidGroup
		return nil
	}

	operationDetails := &Operation{
		Type:  "Get",
		Key:   args.Key,
		Value: "",
		ID:    args.Nrand,
	}

	reply.Val = kv.syncOps(*operationDetails)
	reply.Err = OK

	if reply.Val == "KeySent" {
		reply.Err = ErrInvalidGroup
	}

	return nil
}

func (kv *ShardKV) Put(args *PutArgs, reply *PutReply) error {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	shard := key2shard(args.Key)
	servingKey := kv.currentKeys[shard]

	if !servingKey || kv.staleData[shard] {
		reply.Err = ErrInvalidGroup
		return nil
	}

	operationDetails := &Operation{
		Key:   args.Key,
		Value: args.Value,
		ID:    args.Nrand,
	}

	if !args.DoHash {
		operationDetails.Type = "Put"
	} else {
		operationDetails.Type = "Put hash"
	}

	reply.Prev_val = kv.syncOps(*operationDetails)
	reply.Err = OK

	if reply.Prev_val == "KeySent" {
		reply.Err = ErrInvalidGroup
	}

	return nil
}

// Ask the shardmaster if there's a new configuration;
// if so, re-configure.
func (kv *ShardKV) tick() {
	kv.mu.Lock()
	defer kv.mu.Unlock()

	config := kv.sm.Query(kv.KnownConfig)
	kv.KnownConfig++

	if config.Num == 1 && config.Shards[0] == kv.gid {
		for k := 0; k < shardmaster.NShards; k++ {
			kv.staleData[k] = false
			kv.currentKeys[k] = true
		}
	}

	kv.Groups = config.Groups

	var newConfig [shardmaster.NShards]bool
	var stale [shardmaster.NShards]bool
	shardsToSend := make(map[int64][]int)

	for k, v := range config.Shards {
		if v == kv.gid {
			newConfig[k] = true
		} else {
			newConfig[k] = false
			stale[k] = true

			if kv.currentKeys[k] {
				shardsToSend[v] = append(shardsToSend[v], k)
			}
		}
	}

	if config.Num >= kv.Num {
		operationDetails := &Operation{
			SendShardsTo: shardsToSend,
			Type:         "Config",
			ID:           nrand(),
			Num:          config.Num,
			NewConfig:    newConfig,
			Stale:        stale,
		}
		kv.syncOps(*operationDetails)
	}
}

// tell the server to shut itself down.
func (kv *ShardKV) kill() {
	kv.dead = true
	kv.l.Close()
	kv.px.Kill()
}

// Start a shardkv server.
// gid is the ID of the server's replica group.
// shardmasters[] contains the ports of the
//
//	servers that implement the shardmaster.
//
// servers[] contains the ports of the servers
//
//	in this replica group.
//
// Me is the index of this server in servers[].
func StartServer(gid int64, shardmasters []string,
	servers []string, me int) *ShardKV {
	gob.Register(Operation{})

	kv := new(ShardKV)
	kv.me = me
	kv.gid = gid
	kv.sm = shardmaster.MakeClerk(shardmasters)
	// Your initialization code here.
	// Don't call Join().
	for k := 0; k < shardmaster.NShards; k++ {
		kv.dataStore[k] = make(map[string]string)
		kv.staleData[k] = true
	}
	kv.answerStore = make(map[int64]string)
	kv.Groups = make(map[int64][]string)
	// kv.currentKeys = make(map[int]bool)
	kv.syncedTill = 0
	kv.isNew = true
	kv.KnownConfig = 0
	kv.Num = -1
	// kv.stale = true

	rpcs := rpc.NewServer()
	rpcs.Register(kv)

	kv.px = paxos.Make(servers, me, rpcs)

	os.Remove(servers[me])
	l, e := net.Listen("unix", servers[me])
	if e != nil {
		log.Fatal("listen error: ", e)
	}
	kv.l = l

	// Please do not change any of the following code,
	// or do anything to subvert it.

	go func() {
		for kv.dead == false {
			conn, err := kv.l.Accept()
			if err == nil && kv.dead == false {
				if kv.unreliable && (rand.Int63()%1000) < 100 {
					// Discard the request.
					conn.Close()
				} else if kv.unreliable && (rand.Int63()%1000) < 200 {
					// Process the request but force discard of reply.
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
				fmt.Printf("ShardKV(%v) accept: %v\n", me, err.Error())
				kv.kill()
			}
		}
	}()

	go func() {
		for kv.dead == false {
			kv.tick()
			time.Sleep(250 * time.Millisecond)
		}
	}()

	return kv
}
