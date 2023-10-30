package kvpaxos

import "hash/fnv"

const (
	OK       = "OK"
	ErrNoKey = "ErrNoKey"
)

type Err string

type PutArgs struct {
	// You'll have to add definitions here.
	Key    string
	Value  string
	DoHash bool // For PutHash
	Nrand  int64
	// You'll have to add definitions here.
	// Field names must start with capital letters,
	// otherwise RPC will break.
	Delete int64
}

type PutReply struct {
	Err           Err
	PreviousValue string // For PutHash
}

type GetArgs struct {
	Key   string
	Nrand int64
	// You'll have to add definitions here.
	Delete int64
}

type GetReply struct {
	Err   Err
	Value string
}

func hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
