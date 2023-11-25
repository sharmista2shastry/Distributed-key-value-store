package shardkv

import (
	"crypto/rand"
	"errors"
	"fmt"
	"math/big"
	"net/rpc"
	"sync"
	"time"

	"shardmaster"
)

type Clerk struct {
	mu     sync.Mutex
	sm     *shardmaster.Clerk
	config shardmaster.Config
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}

func MakeClerk(shardmasters []string) *Clerk {
	ck := new(Clerk)
	ck.sm = shardmaster.MakeClerk(shardmasters)
	return ck
}

func call(srv string, rpcname string, args interface{}, reply interface{}) bool {
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

func key2shard(key string) int {
	const numShards = 10

	if len(key) == 0 {
		return 0
	}

	b := []byte(key)[0]
	return int(b) % numShards
}

func (ck *Clerk) Get(key string) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	for {
		if servers, ok := ck.config.Groups[ck.config.Shards[key2shard(key)]]; ok {
			for _, server := range servers {
				args := &GetArgs{Key: key, Nrand: nrand()}
				var reply GetReply
				if ok := call(server, "ShardKV.Get", args, &reply); ok && (reply.Err == OK || reply.Err == ErrKeyNotFound) {
					return reply.Val
				} else if ok && reply.Err == ErrInvalidGroup {
					break
				}
			}
		}
		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	ck.mu.Lock()
	defer ck.mu.Unlock()

	operationID := nrand()

	for {
		shard := key2shard(key)
		gid := ck.config.Shards[shard]
		servers, ok := ck.config.Groups[gid]

		if ok {
			if reply, err := ck.attemptPutOnServers(servers, key, value, dohash, operationID); err == nil {
				return reply
			}
		}

		time.Sleep(100 * time.Millisecond)
		ck.config = ck.sm.Query(-1)
	}
}

func (ck *Clerk) attemptPutOnServers(servers []string, key string, value string, dohash bool, operationID int64) (string, error) {
	args := &PutArgs{
		Key:    key,
		Value:  value,
		DoHash: dohash,
		Nrand:  operationID,
	}

	for _, srv := range servers {
		var reply PutReply
		ok := call(srv, "ShardKV.Put", args, &reply)
		if ok && reply.Err == OK {
			return reply.Prev_val, nil
		}
		if ok && (reply.Err == ErrInvalidGroup) {
			break
		}
	}

	return "", errors.New("put operation failed on all servers")
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}

func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}
