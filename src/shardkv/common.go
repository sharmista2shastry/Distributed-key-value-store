package shardkv

import (
	"hash/fnv"
	"shardmaster"
)

//
// Sharded key/value server.
// Lots of replica groups, each running op-at-a-time paxos.
// Shardmaster decides which group serves each shard.
// Shardmaster may change shard assignment from time to time.
//
// You will have to modify these definitions.
//

const (
	OK              = "OK"
	ErrKeyNotFound  = "ErrNoKey"
	ErrInvalidGroup = "ErrWrongGroup"
)

type Err string

type PutArgs struct {
	Key    string
	Value  string
	DoHash bool // For PutHash
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Nrand int64
}

type PutReply struct {
	Err      Err
	Prev_val string // For PutHash
}

type GetArgs struct {
	Key string
	// You'll have to add definitions here.
	Nrand int64
}

type GetReply struct {
	Err Err
	Val string
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}

// You'll have to add definitions here.
type SendArgs struct {
	ShardMaps      [shardmaster.NShards]map[string]string
	ShardsToCopy   []int
	NumberOfShards int
	RandomNumber   int64
	ResponseMap    map[int64]string
}

type SendReply struct {
	Err Err
	Val string
}
