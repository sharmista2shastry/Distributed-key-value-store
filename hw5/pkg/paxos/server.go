package paxos

import (
	"coms4113/hw5/pkg/base"
	"fmt"
)

const (
	Propose = "propose"
	Accept  = "accept"
	Decide  = "decide"
)

type Proposer struct {
	N             int
	Phase         string
	N_a_max       int
	V             interface{}
	SuccessCount  int
	ResponseCount int
	// To indicate if response from peer is received, should be initialized as []bool of len(server.peers)
	Responses []bool
	// Use this field to check if a message is latest.
	SessionId int

	// in case node will propose again - restore initial value
	InitialValue interface{}
}

type ServerAttribute struct {
	peers []base.Address
	me    int

	// Paxos parameter
	n_p int
	n_a int
	v_a interface{}

	// final result
	agreedValue interface{}

	// Propose parameter
	proposer Proposer

	// retry
	timeout *TimeoutTimer
}

type Server struct {
	base.CoreNode
	ServerAttribute
}

func NewServer(peers []base.Address, me int, proposedValue interface{}) *Server {
	response := make([]bool, len(peers))
	return &Server{
		CoreNode: base.CoreNode{},
		ServerAttribute: ServerAttribute{
			peers: peers,
			me:    me,
			proposer: Proposer{
				InitialValue: proposedValue,
				Responses:    response,
			},
			timeout: &TimeoutTimer{},
		},
	}
}

func (server *Server) handleProposeRequest(msg *ProposeRequest) []base.Node {
	// Create a copy of the server to avoid mutating the original server
	newServer := server.copy()

	// Check if the proposal is accepted or not
	ok := msg.N > newServer.n_p

	var response *ProposeResponse

	response = &ProposeResponse{
		CoreMessage: base.MakeCoreMessage(base.Address(fmt.Sprint(msg.To())), msg.From()),
		Ok:          ok,
		N_p:         newServer.n_p,
		N_a:         newServer.n_a,
		V_a:         newServer.v_a,
		SessionId:   msg.SessionId,
	}

	// If the proposal is accepted, update the state of the server attribute n_p with the received proposal number
	if ok {
		newServer.n_p = msg.N
		// fmt.Printf("Proposal accepted, updated n_p to: %v", newServer.n_p)
	} else {
		// fmt.Println("Proposal not accepted")
	}

	newServer.CoreNode.Response = []base.Message{response}

	// Generate new states that represent the possible next states of the server
	newNodes := []base.Node{newServer}
	// fmt.Printf("Generated new nodes: %v", newNodes)

	return newNodes
}

func (server *Server) handleProposalResponse(msg *ProposeResponse) []base.Node {
	// Create a copy of the server to avoid mutating the original server
	newServer := server.copy()

	// Increment the response count upon receiving a response
	newServer.proposer.ResponseCount++

	msgFromPeer := msg.From()

	var index = -1
	for i, peer := range newServer.peers {

		if msgFromPeer == peer && newServer.proposer.Responses[i] {
			return []base.Node{newServer}
		}

		if msgFromPeer == peer {
			index = i
			break
		}
	}

	if index != -1 {
		if msg.Ok {
			// Increment the success count upon receiving an OK response
			newServer.proposer.SuccessCount++
			newServer.proposer.Responses[index] = true

			if msg.V_a != nil && newServer.proposer.N_a_max < msg.N_a {
				newServer.proposer.V = msg.V_a
				newServer.proposer.N_a_max = msg.N_a
			}

			// Generate a new server node representing the new state of the server
			newServerNextPhase := newServer.copy()

			// fmt.Println("SuccessCount:", newServer.proposer.SuccessCount, "Number of peers:", len(newServer.peers))

			// If a majority of OK responses is received, the proposer may proceed to the next phase
			if newServer.proposer.SuccessCount > len(newServer.peers)/2 { // Changed >= to > for majority
				// fmt.Println("Transitioning to accept phase")
				newServerNextPhase.proposer.Phase = Accept

				// Clearing and resetting the Response and Success counts for the new phase
				newServerNextPhase.proposer.Responses = make([]bool, len(newServer.peers))
				newServerNextPhase.proposer.SuccessCount = 0
				newServerNextPhase.proposer.ResponseCount = 0

				// Perform actions required for the new phase, like sending AcceptRequests to acceptors
				for _, peer := range newServerNextPhase.peers {
					acceptRequest := &AcceptRequest{
						CoreMessage: base.MakeCoreMessage(base.Address(fmt.Sprint(msg.To())), peer),
						N:           newServerNextPhase.proposer.N,
						V:           newServerNextPhase.proposer.V,
						SessionId:   newServerNextPhase.proposer.SessionId,
					}
					newServerNextPhase.CoreNode.Response = append(newServerNextPhase.CoreNode.Response, acceptRequest)
				}
				// fmt.Println("Response:", newServerNextPhase.CoreNode.Response)

				// Return a slice containing both the current state and the new state
				// Debug: print end time
				// fmt.Println("End time:", time.Now())
				return []base.Node{newServerNextPhase, newServer}
			}
		}
	} else {
		// Debug: print a message when a response is received from an invalid peer
		// fmt.Println("Received response from invalid peer:", msgFromPeer)
	}

	// If the response is not OK or the peer index wasn't found, return a slice containing the current state
	// Debug: print end time
	// fmt.Println("End time:", time.Now())
	return []base.Node{newServer}
}

func (server *Server) handleAcceptRequest(msg *AcceptRequest) []base.Node {
	// Create a copy of the server to avoid mutating the original server
	newServer := server.copy()

	// Check if the proposal number of the incoming message is greater than any proposal number the server has seen before
	ok := msg.N >= newServer.n_p && msg.N > newServer.n_a

	// Set the response
	response := &AcceptResponse{
		CoreMessage: base.MakeCoreMessage(base.Address(fmt.Sprint(msg.To())), msg.From()),
		Ok:          ok,
		N_p:         newServer.n_p,
		SessionId:   msg.SessionId,
	}

	// If the proposal is accepted, update the state of the server attribute n_p, n_a and v_a with the received proposal number and value
	if ok {
		newServer.n_a = msg.N
		newServer.n_p = msg.N
		newServer.v_a = msg.V
		// newServer.agreedValue = msg.V
	}

	newServer.CoreNode.Response = []base.Message{response}

	// Return a slice containing the new state of the server
	return []base.Node{newServer}
}

func (server *Server) handleAcceptResponse(msg *AcceptResponse) []base.Node {
	// Debug: print start time
	// fmt.Println("Start time:", time.Now())

	// Create a copy of the server to avoid mutating the original server
	newServer := server.copy()

	// Increment the response count upon receiving a response
	newServer.proposer.ResponseCount++

	msgFromPeer := msg.From()

	var index = -1
	for i, peer := range newServer.peers {
		// Debug: print the values of msgFromPeer and peer
		// fmt.Println("msgFromPeer:", msgFromPeer, "peer:", peer)

		if msgFromPeer == peer && newServer.proposer.Responses[i] {
			return []base.Node{newServer}
		}

		if msgFromPeer == peer {
			index = i
			break
		}
	}

	if index != -1 {
		// Debug: print whether the response is affirmative
		// fmt.Println("Response from peer:", msgFromPeer, "is affirmative:", msg.Ok)

		// Check if the response is affirmative (an OK response)
		if msg.Ok {
			// Increment the success count upon receiving an OK response
			newServer.proposer.SuccessCount++
			newServer.proposer.Responses[index] = true

			// Generate a new server node representing the new state of the server
			newServerNextPhase := newServer.copy()

			// fmt.Println("SuccessCount:", newServer.proposer.SuccessCount, "Number of peers:", len(newServer.peers))

			// If a majority of OK responses is received, the proposer may proceed to the next phase
			if newServer.proposer.SuccessCount > len(newServer.peers)/2 { // Changed >= to > for majority
				// fmt.Println("Transitioning to decide phase")
				newServerNextPhase.proposer.Phase = Decide

				// Clearing and resetting the Response and Success counts for the new phase
				newServerNextPhase.proposer.Responses = make([]bool, len(newServer.peers))
				newServerNextPhase.proposer.SuccessCount = 0
				newServerNextPhase.proposer.ResponseCount = 0

				// Perform actions required for the new phase, like sending DecideRequests to acceptors
				for _, peer := range newServerNextPhase.peers {
					decideRequest := &DecideRequest{
						CoreMessage: base.MakeCoreMessage(base.Address(fmt.Sprint(msg.To())), peer),
						V:           newServerNextPhase.proposer.V,
						SessionId:   newServerNextPhase.proposer.SessionId,
					}
					newServerNextPhase.CoreNode.Response = append(newServerNextPhase.CoreNode.Response, decideRequest)
				}
				// fmt.Println("Response:", newServerNextPhase.CoreNode.Response)

				// Return a slice containing both the current state and the new state
				// Debug: print end time
				// fmt.Println("End time:", time.Now())
				return []base.Node{newServerNextPhase, newServer}
			}
		}
	} else {
		// Debug: print a message when a response is received from an invalid peer
		// fmt.Println("Received response from invalid peer:", msgFromPeer)
	}

	// If the response is not OK or the peer index wasn't found, return a slice containing the current state
	// Debug: print end time
	// fmt.Println("End time:", time.Now())
	return []base.Node{newServer}
}

func (server *Server) handleDecideRequest(msg *DecideRequest) []base.Node {
	// Create a copy of the server to avoid mutating the original server
	newServer := server.copy()

	// Update the agreed value of the server
	newServer.agreedValue = msg.V

	// Generate a new server node representing the new state of the server
	newNode := newServer.copy()

	// Return a slice containing the new server node
	return []base.Node{newNode}
}

func (server *Server) MessageHandler(message base.Message) []base.Node {
	// Create a copy of the server to avoid mutating the original server
	newServer := server.copy()

	// Determine the type of the incoming message and call the appropriate handler
	switch msg := message.(type) {
	case *ProposeRequest:
		return newServer.handleProposeRequest(msg)
	case *ProposeResponse:
		return newServer.handleProposalResponse(msg)
	case *AcceptRequest:
		return newServer.handleAcceptRequest(msg)
	case *AcceptResponse:
		return newServer.handleAcceptResponse(msg)
	case *DecideRequest:
		return newServer.handleDecideRequest(msg)
	}

	return []base.Node{newServer}
}

// To start a new round of Paxos.
// To start a new round of Paxos.
func (server *Server) StartPropose() []base.Message {
	// Set the proposer's fields
	server.proposer.N = 1
	server.proposer.Phase = Propose
	server.proposer.N_a_max = 0
	server.proposer.V = server.proposer.InitialValue
	server.proposer.SuccessCount = 0
	server.proposer.ResponseCount = 0
	for i := range server.proposer.Responses {
		server.proposer.Responses[i] = false
	}
	server.proposer.SessionId = 1

	// Create a slice to hold the messages
	messages := make([]base.Message, len(server.peers))

	// Send the ProposeRequest to all peers, including itself
	for i, peerAddress := range server.peers {
		// Create a ProposeRequest
		msg := &ProposeRequest{
			CoreMessage: base.MakeCoreMessage(server.Address(), peerAddress),
			N:           server.proposer.N,
			SessionId:   server.proposer.SessionId,
		}

		// Add the message to the slice
		messages[i] = msg
	}

	// Set the messages in the server's Response field
	server.CoreNode.SetResponse(messages)

	// Return the messages
	return messages
}

// Returns a deep copy of server node
func (server *Server) copy() *Server {
	response := make([]bool, len(server.peers))
	for i, flag := range server.proposer.Responses {
		response[i] = flag
	}

	var copyServer Server
	copyServer.me = server.me
	// shallow copy is enough, assuming it won't change
	copyServer.peers = server.peers
	copyServer.n_a = server.n_a
	copyServer.n_p = server.n_p
	copyServer.v_a = server.v_a
	copyServer.agreedValue = server.agreedValue
	copyServer.proposer = Proposer{
		N:             server.proposer.N,
		Phase:         server.proposer.Phase,
		N_a_max:       server.proposer.N_a_max,
		V:             server.proposer.V,
		SuccessCount:  server.proposer.SuccessCount,
		ResponseCount: server.proposer.ResponseCount,
		Responses:     response,
		InitialValue:  server.proposer.InitialValue,
		SessionId:     server.proposer.SessionId,
	}

	// doesn't matter, timeout timer is state-less
	copyServer.timeout = server.timeout

	return &copyServer
}

func (server *Server) NextTimer() base.Timer {
	return server.timeout
}

// A TimeoutTimer tick simulates the situation where a proposal procedure times out.
// It will close the current Paxos round and start a new one if no consensus reached so far,
// i.e. the server after timer tick will reset and restart from the first phase if Paxos not decided.
// The timer will not be activated if an agreed value is set.
func (server *Server) TriggerTimer() []base.Node {
	if server.timeout == nil {
		return nil
	}

	subNode := server.copy()
	subNode.StartPropose()

	return []base.Node{subNode}
}

func (server *Server) Attribute() interface{} {
	return server.ServerAttribute
}

func (server *Server) Copy() base.Node {
	return server.copy()
}

func (server *Server) Hash() uint64 {
	return base.Hash("paxos", server.ServerAttribute)
}

func (server *Server) Equals(other base.Node) bool {
	otherServer, ok := other.(*Server)

	if !ok || server.me != otherServer.me ||
		server.n_p != otherServer.n_p || server.n_a != otherServer.n_a || server.v_a != otherServer.v_a ||
		(server.timeout == nil) != (otherServer.timeout == nil) {
		return false
	}

	if server.proposer.N != otherServer.proposer.N || server.proposer.V != otherServer.proposer.V ||
		server.proposer.N_a_max != otherServer.proposer.N_a_max || server.proposer.Phase != otherServer.proposer.Phase ||
		server.proposer.InitialValue != otherServer.proposer.InitialValue ||
		server.proposer.SuccessCount != otherServer.proposer.SuccessCount ||
		server.proposer.ResponseCount != otherServer.proposer.ResponseCount {
		return false
	}

	for i, response := range server.proposer.Responses {
		if response != otherServer.proposer.Responses[i] {
			return false
		}
	}

	return true
}

func (server *Server) Address() base.Address {
	return server.peers[server.me]
}
