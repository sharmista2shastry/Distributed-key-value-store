package kvpaxos

import (
	"crypto/rand"
	"fmt"
	"math/big"
	mrand "math/rand"
	"net/rpc"
	"time"
)

// nrand generates a random int64 value.
func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigInt, _ := rand.Int(rand.Reader, max)
	x := bigInt.Int64()
	return x
}

// Clerk represents a client that interacts with the KVPaxos servers.
type Clerk struct {
	servers  []string
	deleteOp int64 // Used to remove successful operations from the cache.
}

// MakeClerk creates a new Clerk instance with the given server addresses.
func MakeClerk(servers []string) *Clerk {
	return &Clerk{
		servers:  servers,
		deleteOp: 0,
	}
}

// call sends an RPC to the rpcname handler on server srv with arguments args,
// waits for the reply, and leaves the reply in reply. The reply argument should
// be a pointer to a reply structure. The return value is true if the server
// responded, and false if call was not able to contact the server.
func call(srv string, rpcName string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		return false
	}
	defer c.Close()

	err = c.Call(rpcName, args, reply)
	if err == nil {
		return true
	}

	fmt.Println(err)
	return false
}

// Get fetches the current value for a key. It returns "" if the key does not exist.
// It keeps trying forever in the face of all other errors.
func (ck *Clerk) Get(key string) string {
	args := GetArgs{
		Key:    key,
		Nrand:  nrand(),
		Delete: ck.deleteOp,
	}
	r := 100
	to := time.Duration(r) * time.Millisecond

	// Try forever.
	for {
		// Pick a random server.
		k := mrand.Intn(len(ck.servers))
		var reply GetReply

		// Send an RPC request to the server to get the value.
		ok := call(ck.servers[k], "KVPaxos.Get", args, &reply)
		if ok {
			// Delete the operation from the cache.
			ck.deleteOp = args.Nrand
			return reply.Value
		}
		// If it fails, then the server should retry after a while (100 milliseconds in our case).
		time.Sleep(to)
	}
}

// PutExt sets the value for a key. It keeps trying until it succeeds.
func (ck *Clerk) PutExt(key string, value string, doHash bool) string {
	args := PutArgs{
		Key:    key,
		Value:  value,
		DoHash: doHash,
		Nrand:  nrand(),
		Delete: ck.deleteOp,
	}
	r := 100
	to := time.Duration(r) * time.Millisecond

	for {
		// Pick a random server.
		k := mrand.Intn(len(ck.servers))
		var reply PutReply

		// Send an RPC request to the server to put the value.
		ok := call(ck.servers[k], "KVPaxos.Put", args, &reply)
		if ok {
			// Delete the operation from the cache.
			ck.deleteOp = args.Nrand
			return reply.PreviousValue
		}
		time.Sleep(to)
	}
	return ""
}

// Put sets the value for a key.
func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}

// PutHash sets the value for a key using a hash function.
func (ck *Clerk) PutHash(key string, value string) string {
	return ck.PutExt(key, value, true)
}
