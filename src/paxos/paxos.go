package paxos

// Paxos library, to be included in an application.
// Multiple applications will run, each including
// a Paxos peer.
//
// Manages a sequence of agreed-on values.
// The set of peers is fixed.
// Copes with network failures (partition, msg loss, &c).
// Does not store anything persistently, so cannot handle crash+restart.
//
// The application interface:
//
// px = paxos.Make(peers []string, me string)
// px.Start(seq int, v interface{}) -- start agreement on new instance
// px.Status(seq int) (decided bool, v interface{}) -- get info about an instance
// px.Done(seq int) -- ok to forget all instances <= seq
// px.Max() int -- highest instance seq known, or -1
// px.Min() int -- instances before this seq have been forgotten
//

import (
	"fmt"
	"log"
	"math"
	"math/rand"
	"net"
	"net/rpc"
	"os"
	"strconv"
	"sync"
	"syscall"
)

//import "time"

type Paxos struct {
	mu         sync.Mutex
	l          net.Listener
	dead       bool
	unreliable bool
	rpcCount   int
	peers      []string
	me         int

	// Your data here.
	InstanceStates map[int]*State
	MinSeq         []int
	maxSeq         int
}

type Error string

const (
	OK    Error = "OK"
	ERROR Error = "REJECTED"
)

type RPCArgs struct {
	Seq         int
	N           int
	Val         interface{}
	InitiatorID int
	MinSeq      int
}

type RPCReply struct {
	Err         Error
	N           int
	Val         interface{}
	MinSeq      int
	ResponderID int
}

type State struct {
	Np      int
	Na      int
	Va      interface{}
	Decided bool
}

func GenerateProposalNum(num, serverNum int) int {
	idStr := strconv.Itoa(num) + strconv.Itoa(serverNum)
	id, _ := strconv.Atoi(idStr)
	return id
}

func (px *Paxos) CleanUpInstances(peer int, min int) {
	px.mu.Lock()
	defer px.mu.Unlock()

	if px.MinSeq[peer] >= min {
		return
	}

	px.MinSeq[peer] = min

	overallMin := px.Min()
	for seq := range px.InstanceStates {
		if seq < overallMin {
			delete(px.InstanceStates, seq)
		}
	}
}

func (px *Paxos) Prepare(args *RPCArgs, reply *RPCReply) error {
	px.mu.Lock()
	defer px.mu.Unlock()

	reply.MinSeq = px.MinSeq[px.me]
	reply.ResponderID = px.me

	seq := args.Seq
	currentState, ok := px.InstanceStates[seq]
	if !ok {
		currentState = new(State)
		px.InstanceStates[seq] = currentState
	}

	if args.N > currentState.Np {
		currentState.Np = args.N
		reply.N = currentState.Na
		reply.Val = currentState.Va
		reply.Err = OK
	} else {
		reply.Err = ERROR
	}

	return nil
}

func (px *Paxos) Accept(args *RPCArgs, reply *RPCReply) error {
	px.CleanUpInstances(args.InitiatorID, args.MinSeq)

	px.mu.Lock()
	defer px.mu.Unlock()

	reply.MinSeq = px.MinSeq[px.me]
	reply.ResponderID = px.me
	reply.N = args.N
	reply.Val = args.Val

	state, ok := px.InstanceStates[args.Seq]
	if !ok {
		state = new(State)
		px.InstanceStates[args.Seq] = state
	}

	if args.N >= state.Np {
		state.Np = args.N
		state.Na = args.N
		state.Va = args.Val
		reply.Err = OK
	} else {
		reply.Err = ERROR
	}

	return nil
}

// Commit the value of a sequence number
func (px *Paxos) Decide(args *RPCArgs, reply *RPCReply) error {

	px.CleanUpInstances(args.InitiatorID, args.MinSeq)

	px.mu.Lock()
	defer px.mu.Unlock()

	state, ok := px.InstanceStates[args.Seq]
	if !ok {
		state = &State{}
	}

	state.Decided = true
	state.Va = args.Val
	reply.Err = OK

	px.InstanceStates[args.Seq] = state

	return nil
}

// call() sends an RPC to the rpcname handler on server srv
// with arguments args, waits for the reply, and leaves the
// reply in reply. the reply argument should be a pointer
// to a reply structure.
//
// the return value is true if the server responded, and false
// if call() was not able to contact the server. in particular,
// the replys contents are only valid if call() returned true.
//
// you should assume that call() will time out and return an
// error after a while if it does not get a reply from the server.
//
// please use call() to send all RPCs, in client.go and server.go.
// please do not change this function.
func call(srv string, name string, args interface{}, reply interface{}) bool {
	c, err := rpc.Dial("unix", srv)
	if err != nil {
		err1 := err.(*net.OpError)
		if err1.Err != syscall.ENOENT && err1.Err != syscall.ECONNREFUSED {
			//fmt.Printf("paxos Dial() failed: %v\n", err1)
		}
		return false
	}
	defer c.Close()

	err = c.Call(name, args, reply)
	if err == nil {
		return true
	}

	//fmt.Println(err)
	return false
}

func (px *Paxos) Propose(value interface{}, seq int) {
	args := &RPCArgs{
		Val:         value,
		Seq:         seq,
		InitiatorID: px.me,
	}

	N := GenerateProposalNum(seq, px.me)
	for {
		N++

		if px.dead {
			return
		}

		// get ready to send rpcs
		args.N = N
		args.MinSeq = px.MinSeq[px.me]

		replyChannel := make(chan *RPCReply, len(px.peers))
		var replies []*RPCReply

		for i, p := range px.peers {
			if i == px.me {
				reply := new(RPCReply)
				px.Prepare(args, reply)
				replyChannel <- reply
			} else {
				go func(p string) {
					reply := new(RPCReply)
					ok := call(p, "Paxos.Prepare", args, reply)
					if !ok {
						reply.Err = ERROR
					}
					replyChannel <- reply
				}(p)
			}
		}

		acceptedCount := 0
		for range px.peers {
			reply := <-replyChannel
			replies = append(replies, reply)
			if reply.Err == OK {
				acceptedCount++
			}
		}

		if acceptedCount >= len(px.peers)/2+1 {
			maxN := 0
			var maxV interface{}
			for _, reply := range replies {
				if int(reply.N) > maxN {
					maxN, maxV = int(reply.N), reply.Val
				}
			}

			if maxV != nil {
				args.Val = maxV
			}

			replyChannel = make(chan *RPCReply, len(px.peers))
			replies = nil

			for i, p := range px.peers {
				if i == px.me {
					reply := new(RPCReply)
					px.Accept(args, reply)
					replyChannel <- reply
				} else {
					go func(p string) {
						reply := new(RPCReply)
						ok := call(p, "Paxos.Accept", args, reply)
						if !ok {
							reply.Err = ERROR
						}
						replyChannel <- reply
					}(p)
				}
			}

			acceptedCount = 0
			for range px.peers {
				reply := <-replyChannel
				replies = append(replies, reply)
				if reply.Err == OK {
					acceptedCount++
				}
			}

			if acceptedCount >= len(px.peers)/2+1 {
				break
			}

			N = int(math.Max(float64(N), float64(maxN)))
		}
	}

	args.MinSeq = px.MinSeq[px.me]

	for i, p := range px.peers {
		if i == px.me {
			reply := new(RPCReply)
			px.Decide(args, reply)
		} else {
			call(p, "Paxos.Decide", args, new(RPCReply))
		}
	}
}

// the application wants paxos to start agreement on
// instance seq, with proposed value v.
// Start() returns right away; the application will
// call Status() to find out if/when agreement
// is reached.
func (px *Paxos) Start(seq int, v interface{}) {
	// Your code here.
	go px.Propose(v, seq)
}

// the application on this machine is done with
// all instances <= seq.
//
// see the comments for Min() for more explanation.
func (px *Paxos) Done(seq int) {
	// Your code here.
	px.CleanUpInstances(px.me, seq)
}

// the application wants to know the
// highest instance sequence known to
// this peer.
func (px *Paxos) Max() int {
	px.mu.Lock()
	defer px.mu.Unlock()

	maxSeq := 0
	for seq := range px.InstanceStates {
		if seq > maxSeq {
			maxSeq = seq
		}
	}
	px.maxSeq = maxSeq

	return px.maxSeq
}

// Min() should return one more than the minimum among z_i,
// where z_i is the highest number ever passed
// to Done() on peer i. A peer's z_i is -1 if it has
// never called Done().
//
// Paxos is required to have forgotten all information
// about any instances it knows that are < Min().
// The point is to free up memory in long-running
// Paxos-based servers.
//
// Paxos peers need to exchange their highest Done()
// arguments in order to implement Min(). These
// exchanges can be piggybacked on ordinary Paxos
// agreement protocol messages, so it is OK if one
// peer's Min does not reflect another Peers Done()
// until after the next instance is agreed to.
//
// The fact that Min() is defined as a minimum over
// *all* Paxos peers means that Min() cannot increase until
// all peers have been heard from. So if a peer is dead
// or unreachable, other peers' Min()s will not increase
// even if all reachable peers call Done. The reason for
// this is that when the unreachable peer comes back to
// life, it will need to catch up on instances that it
// missed -- the other peers therefor cannot forget these
// instances.
func (px *Paxos) Min() int {
	// You code here.

	OverallMin := math.MaxInt32
	for _, minNum := range px.MinSeq {
		if minNum < OverallMin {
			OverallMin = minNum
		}
	}
	return OverallMin + 1
}

// the application wants to know whether this
// peer thinks an instance has been decided,
// and if so what the agreed value is. Status()
// should just inspect the local peer state;
// it should not contact other Paxos peers.
func (px *Paxos) Status(seq int) (bool, interface{}) {
	// Your code here.

	px.mu.Lock()
	defer px.mu.Unlock()

	currentState := px.InstanceStates[seq]
	if currentState == nil {
		return false, nil
	} else {
		return currentState.Decided, currentState.Va
	}
}

// tell the peer to shut itself down.
// for testing.
// please do not change this function.
func (px *Paxos) Kill() {
	px.dead = true
	if px.l != nil {
		px.l.Close()
	}
}

// the application wants to create a paxos peer.
// the ports of all the paxos peers (including this one)
// are in peers[]. this servers port is peers[me].
func Make(peers []string, me int, rpcs *rpc.Server) *Paxos {
	px := &Paxos{
		peers:          peers,
		me:             me,
		InstanceStates: make(map[int]*State),
		MinSeq:         make([]int, len(peers)),
		maxSeq:         -1,
	}

	for i := range px.MinSeq {
		px.MinSeq[i] = -1
	}

	// Your initialization code here.

	if rpcs != nil {
		// caller will create socket &c
		rpcs.Register(px)
	} else {
		rpcs = rpc.NewServer()
		rpcs.Register(px)

		// prepare to receive connections from clients.
		// change "unix" to "tcp" to use over a network.
		os.Remove(peers[me]) // only needed for "unix"
		l, e := net.Listen("unix", peers[me])
		if e != nil {
			log.Fatal("listen error: ", e)
		}
		px.l = l

		// please do not change any of the following code,
		// or do anything to subvert it.

		// create a thread to accept RPC connections
		go func() {
			for px.dead == false {
				conn, err := px.l.Accept()
				if err == nil && px.dead == false {
					if px.unreliable && (rand.Int63()%1000) < 100 {
						// discard the request.
						conn.Close()
					} else if px.unreliable && (rand.Int63()%1000) < 200 {
						// process the request but force discard of reply.
						c1 := conn.(*net.UnixConn)
						f, _ := c1.File()
						err := syscall.Shutdown(int(f.Fd()), syscall.SHUT_WR)
						if err != nil {
							fmt.Printf("shutdown: %v\n", err)
						}
						px.rpcCount++
						go rpcs.ServeConn(conn)
					} else {
						px.rpcCount++
						go rpcs.ServeConn(conn)
					}
				} else if err == nil {
					conn.Close()
				}
				if err != nil && px.dead == false {
					fmt.Printf("Paxos(%v) accept: %v\n", me, err.Error())
				}
			}
		}()
	}

	return px
}
