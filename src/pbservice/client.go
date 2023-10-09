package pbservice

import (
	"crypto/rand"
	"math/big"
	"net/rpc"
	"time"
	"viewservice"
)

// You'll probably need to uncomment these:

type Clerk struct {
	vs *viewservice.Clerk
	// Your declarations here
}

func MakeClerk(vshost string, me string) *Clerk {
	ck := new(Clerk)
	ck.vs = viewservice.MakeClerk(me, vshost)
	// Your ck.* initializations here

	return ck
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the reply's contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it doesn't get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please don't change this function.
func call(srv string, rpcname string,
	args interface{}, reply interface{}) bool {
	c, errx := rpc.Dial("unix", srv)
	if errx != nil {
		return false
	}
	defer c.Close()

	err := c.Call(rpcname, args, reply)
	if err == nil {
		return true
	}

	// fmt.Println(err)
	return false
}

// get a specific key's value from the current primary;
// if the key doesn't exist, return an empty string.
// Get() should return only when it has completed all operations
// All operations should provide at-most-once semantics.
func (ck *Clerk) Get(key string) string {

	var reply GetReply
	args := &GetArgs{Key: key}

	for {
		// get the current view
		v, _ := ck.vs.Get()

		reply.Err = ""
		reply.Value = ""

		// try to get the key's value from the current primary
		ok := call(v.Primary, "PBServer.Get", args, &reply)

		if ok && reply.Err == "" {
			return reply.Value
		}

		// if the primary is not responding, wait for a while
		time.Sleep(viewservice.PingInterval)
	}
}

// Primary should keep trying to update the key's value until it succeeds.
func (ck *Clerk) PutExt(key string, value string, dohash bool) string {
	// Your code here.

	args := PutArgs{
		Key:    key,
		Value:  value,
		DoHash: dohash,
		// The nrand() function generates a random number that is used to ensure that each Put operation has a unique identifier, which is used to detect duplicate operations and to ensure that the operations are executed in the correct order
		Nrand: nrand(),
	}

	// PutExt sends a Put RPC to the primary server in the current view with the given arguments.
	// It retries the RPC until it succeeds or the reply contains an error.
	// It returns the previous value associated with the key, if any.

	var reply PutReply
	for {
		view, _ := ck.vs.Get()
		if ok := call(view.Primary, "PBServer.Put", args, &reply); ok {
			break
		} else if reply.Err != "" {
			continue
		}
	}
	return reply.PreviousValue
}

func (ck *Clerk) Put(key string, value string) {
	ck.PutExt(key, value, false)
}

func (ck *Clerk) PutHash(key string, value string) string {
	v := ck.PutExt(key, value, true)
	return v
}

func nrand() int64 {
	max := big.NewInt(int64(1) << 62)
	bigx, _ := rand.Int(rand.Reader, max)
	x := bigx.Int64()
	return x
}
