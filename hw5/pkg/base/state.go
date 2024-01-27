package base

import (
	"encoding/binary"
	"hash/fnv"
	"log"
	"math/rand"
)

type State struct {
	nodes      map[Address]Node
	addresses  []Address
	blockLists map[Address][]Address
	Network    []Message
	Depth      int

	isDropOff   bool
	isDuplicate bool

	// inheritance
	Prev  *State
	Event Event

	// auxiliary information
	nodeHash    uint64
	networkHash uint64
	// If the network is sorted by hash, then no need to do it again
	// Thus, use this indicator to record if it has been sorted
	hashSorted bool
}

func NewState(depth int, isDropOff, isDuplicate bool) *State {
	return &State{
		nodes:       map[Address]Node{},
		blockLists:  map[Address][]Address{},
		Network:     make([]Message, 0, 8),
		Depth:       depth,
		isDropOff:   isDropOff,
		isDuplicate: isDuplicate,
		nodeHash:    0,
		networkHash: 0,
		hashSorted:  false,
	}
}

func (s *State) AddNode(address Address, node Node, blockList []Address) {
	if old, ok := s.nodes[address]; ok {
		s.nodeHash -= old.Hash()
	} else {
		s.addresses = append(s.addresses, address)
	}

	s.nodes[address] = node
	s.blockLists[address] = blockList
	s.nodeHash += node.Hash()
	return
}

func (s *State) UpdateNode(address Address, node Node) {
	if old, ok := s.nodes[address]; ok {
		s.nodeHash -= old.Hash()
	} else {
		panic("node does not exist")
	}

	s.nodes[address] = node
	s.nodeHash += node.Hash()
	s.Receive(node.HandlerResponse())
}

func (s *State) Nodes() map[Address]Node {
	return s.nodes
}

func (s *State) GetNode(address Address) Node {
	return s.nodes[address]
}

func (s *State) Send(meg Message) {
	s.Network = append(s.Network, meg)
	return
}

func (s *State) Clone() *State {
	newState := NewState(s.Depth+1, s.isDropOff, s.isDuplicate)
	for address, node := range s.nodes {
		newState.nodes[address] = node
	}

	// Assume these fields are identical among every state and their children
	newState.addresses = s.addresses
	newState.blockLists = s.blockLists

	for _, message := range s.Network {
		newState.Network = append(newState.Network, message)
	}

	newState.nodeHash = s.nodeHash
	newState.networkHash = s.networkHash
	newState.hashSorted = s.hashSorted

	return newState
}

func (s *State) Inherit(event Event) *State {
	newState := s.Clone()
	newState.Prev = s
	newState.Event = event
	return newState
}

// blockList will not be compared in the Equal operation
func (s *State) Equals(other *State) bool {
	if other == nil {
		return false
	}

	if len(s.nodes) != len(other.nodes) || len(s.Network) != len(other.Network) ||
		s.nodeHash != other.nodeHash || s.networkHash != other.networkHash {
		return false
	}

	for address, node := range s.nodes {
		otherNode, ok := other.nodes[address]
		if !ok || !node.Equals(otherNode) {
			return false
		}
	}

	if !s.hashSorted {
		hashSort(s.Network)
		s.hashSorted = true
	}

	if !other.hashSorted {
		hashSort(other.Network)
		other.hashSorted = true
	}

	for i, message := range s.Network {
		if !message.Equals(other.Network[i]) {
			return false
		}
	}

	return true
}

func isBlocked(blockList []Address, candidate Address) bool {
	if blockList == nil {
		return false
	}

	for _, addr := range blockList {
		if addr == candidate {
			return true
		}
	}

	return false
}

func (s *State) isLocalCall(index int) bool {
	message := s.Network[index]
	return message.From() == message.To()
}

func (s *State) isMessageReachable(index int) (bool, *State) {
	message := s.Network[index]
	to := message.To()

	_, ok := s.nodes[to]
	if !ok {
		newState := s.Inherit(UnknownDestinationEvent(message))
		newState.DeleteMessage(index)
		return false, newState
	}

	if isBlocked(s.blockLists[to], message.From()) {
		newState := s.Inherit(PartitionEvent(message))
		newState.DeleteMessage(index)
		return false, newState
	}

	return true, nil
}

func (s *State) isMessageDropped(index int) (bool, *State) {
	if !s.isDropOff {
		return false, s
	}

	message := s.Network[index]
	dropCandidate := rand.Float64() < 0.5 // Adjust this probability as needed

	if dropCandidate {
		newState := s.Inherit(DropOffEvent(message))
		newState.DeleteMessage(index)
		return true, newState
	}

	return false, s
}

func (s *State) isMessageDuplicated(index int) (bool, *State) {
	if !s.isDuplicate {
		return false, s
	}

	duplicateCandidate := rand.Float64() < 0.5 // Adjust this probability as needed

	if duplicateCandidate {
		newStates := s.HandleMessage(index, false) // Don't delete the message
		if len(newStates) > 0 {
			return true, newStates[0] // Return the first new state
		}
	}

	return false, s
}

func (s *State) isMessageArrivedNormally(index int) (bool, *State) {
	reachable, newState := s.isMessageReachable(index)
	if !reachable {
		return false, newState
	}

	newStates := s.HandleMessage(index, true)
	if len(newStates) > 0 {
		return true, newStates[0] // Return the first new state
	}

	return false, s
}

func (s *State) HandleMessage(index int, deleteMessage bool) []*State {
	message := s.Network[index]
	recipientAddress := message.To()
	recipientNode := s.nodes[recipientAddress]

	newNodes := recipientNode.MessageHandler(message)

	var result []*State

	for _, newNode := range newNodes {
		newState := s.Clone()                       // Clone the current state
		newState.Event = HandleEvent(message)       // Set the event for the new state
		newState.nodes[recipientAddress] = newNode  // Update the node in the new state
		newState.Receive(newNode.HandlerResponse()) // New state receives the messages from the new node

		if deleteMessage {
			newState.DeleteMessage(index) // Delete the message from the new state if required
		}

		result = append(result, newState) // Append the new state to the result
	}

	if len(result) == 0 {
		return nil // Return nil if no new states are created
	}

	return result
}

func (s *State) DeleteMessage(index int) {
	// Remove the i-th message
	message := s.Network[index]
	s.Network[index] = s.Network[len(s.Network)-1]
	s.Network = s.Network[:len(s.Network)-1]

	// remove from the hash
	s.networkHash -= message.Hash()
	s.hashSorted = false
}

func (s *State) Receive(messages []Message) {
	for _, message := range messages {
		s.Network = append(s.Network, message)
		s.networkHash += message.Hash()
		s.hashSorted = false
	}
}

func (s *State) NextStates() []*State {
	nextStates := make([]*State, 0, 4)

	for i := range s.Network {
		// check if it is a local call
		if s.isLocalCall(i) {
			newStates := s.HandleMessage(i, true)
			nextStates = append(nextStates, newStates...)
			continue
		}

		// check Network Partition
		reachable, newState := s.isMessageReachable(i)
		if !reachable {
			nextStates = append(nextStates, newState)
			continue
		}

		// Drop off a message
		if s.isDropOff {
			dropped, newState := s.isMessageDropped(i)
			if dropped {
				nextStates = append(nextStates, newState)
			}
		}

		// Message arrives Normally. (use HandleMessage)
		arrived, _ := s.isMessageArrivedNormally(i)
		if arrived {
			newStates := s.HandleMessage(i, true)
			nextStates = append(nextStates, newStates...)
		}

		// Message arrives but the message is duplicated. The same message may come later again
		// (use HandleMessage)
		if s.isDuplicate {
			duplicated, _ := s.isMessageDuplicated(i)
			if duplicated {
				newStates := s.HandleMessage(i, false) // Don't delete the message
				nextStates = append(nextStates, newStates...)
			}
		}

	}

	// You must iterate through the addresses, because every iteration on map is random...
	// Weird feature in Go
	for _, address := range s.addresses {
		node := s.nodes[address]

		// call the timer (use TriggerNodeTimer)
		newStates := s.TriggerNodeTimer(address, node)
		nextStates = append(nextStates, newStates...)
	}

	return nextStates
}

func (s *State) TriggerNodeTimer(address Address, node Node) []*State {
	// Create a slice to hold the new states
	newStates := make([]*State, 0)

	// Trigger the timer on the node
	newNodes := node.TriggerTimer()

	if len(newNodes) == 0 {
		return nil // return nil if no new nodes are created
	}

	// For each new node, create a new state
	for _, newNode := range newNodes {
		// Create a new state by inheriting the current state
		newState := s.Inherit(TriggerEvent(address, node.NextTimer()))

		// Update the new state with the new node
		newState.UpdateNode(address, newNode)

		// Append the new state to the newStates slice
		newStates = append(newStates, newState)
	}

	return newStates
}

func (s *State) RandomNextState() *State {
	log.Println("Entering RandomNextState")
	timerAddresses := make([]Address, 0, len(s.nodes))
	for addr, node := range s.nodes {
		if IsNil(node.NextTimer()) {
			continue
		}
		timerAddresses = append(timerAddresses, addr)
	}

	totalLength := len(s.Network) + len(timerAddresses)

	if totalLength <= 0 {
		log.Println("Exiting RandomNextState: totalLength <= 0")
		return &State{
			nodes:       map[Address]Node{},
			blockLists:  map[Address][]Address{},
			Network:     []Message{},
			Depth:       s.Depth + 1,
			isDropOff:   false,
			isDuplicate: false,
			nodeHash:    0,
			networkHash: 0,
			hashSorted:  false,
		}
	}

	roll := rand.Intn(totalLength)

	if roll < len(s.Network) {
		// check Network Partition
		log.Println("In Network section")
		reachable, newState := s.isMessageReachable(roll)
		if !reachable {
			log.Println("Exiting RandomNextState: message not reachable")
			return newState
		}

		// handle message and return one state
		newStates := s.HandleMessage(roll, true)
		if len(newStates) > 0 {
			log.Println("Exiting RandomNextState: after handling message")
			return newStates[0] // return the first new state
		}
	} else if roll-len(s.Network) < len(timerAddresses) {
		log.Println("In Timer section")
		// trigger timer and return one state
		address := timerAddresses[roll-len(s.Network)]
		node, ok := s.nodes[address]
		if !ok {
			log.Println("Exiting RandomNextState: node not found")
			// handle the error, for example by returning the current state or logging an error
			return s
		}
		newStates := s.TriggerNodeTimer(address, node)
		if len(newStates) > 0 {
			log.Println("Exiting RandomNextState: after triggering timer")
			return newStates[0] // return the first new state
		}
	}

	log.Println("Exiting RandomNextState: no new state created")
	return s // return the current state if no new state is created
}

// Calculate the hash function of a State based on its nodeHash and networkHash.
// It doesn't consider the group information because we assume the group information does not change
// during the evaluation.
func (s *State) Hash() uint64 {
	b := make([]byte, 8)
	h := fnv.New64()

	binary.BigEndian.PutUint64(b, s.nodeHash)
	_, _ = h.Write(b)

	binary.BigEndian.PutUint64(b, s.networkHash)
	_, _ = h.Write(b)

	return h.Sum64()
}
