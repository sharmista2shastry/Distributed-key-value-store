package base

import (
	"container/list"
	"fmt"
	"reflect"
)

// SearchResult is the result of a search.
type SearchResult struct {
	Success    bool     // whether the search is successful
	Targets    []*State // the list of states that are the targets
	Invalidate *State   // the state that is invalidated by the search (if any).
	// Number of explored states
	N int
}

// stateHashMap is a hash map that maps a hash value to a list of states.
type stateHashMap map[uint64][]*State

// add adds a state to the hash map. If the state is already in the hash map, it will not be added.
func (m stateHashMap) add(s *State) {
	m[s.Hash()] = append(m[s.Hash()], s)
}

// has checks if a state is in the hash map.
func (m stateHashMap) has(s *State) bool {
	states := m[s.Hash()]
	if states == nil {
		return false
	}

	for _, state := range states {
		if state.Equals(s) {
			return true
		}
	}

	return false
}

// BfsFind performs a breadth-first search.
func BfsFind(initState *State, validate, goalPredicate func(*State) bool, limitDepth int) (result SearchResult) {
	queue := list.New()
	queue.PushBack(initState) // add the initial state into the queue

	explored := stateHashMap{} // a hash map that maps a hash value to a list of states.
	explored.add(initState)    // add the initial state into the hash map

	// initialize the result
	result.N = 0

	// BFS search loop
	for queue.Len() > 0 {
		v := queue.Remove(queue.Front())
		state := v.(*State)
		result.N++

		// Check if the goal is reached
		if goalPredicate(state) {
			result.Success = true
			result.Targets = []*State{state}
			return
		}

		// No need to add more states into the queue if the depth is too large
		if limitDepth >= 0 && state.Depth == limitDepth {
			continue
		}

		// Add the next states into the queue if they are not explored yet
		for _, newState := range state.NextStates() {
			if explored.has(newState) {
				continue
			}

			// Check if the state is valid before adding it into the queue
			if !validate(newState) {
				result.Success = false
				result.Invalidate = newState
				return
			}

			// Add the state into the queue and the hash map
			explored.add(newState)
			queue.PushBack(newState)
		}
	}

	result.Success = false
	return
}

// BfsFindAll performs a breadth-first search and returns all the targets.
func BfsFindAll(initState *State, validate, goalPredicate func(*State) bool, depth int) (result SearchResult) {
	if depth < 0 {
		panic("depth must be non-negative")
	}

	if !validate(initState) {
		result.Success = false
		result.Invalidate = initState
		return
	}

	queue := list.New()
	queue.PushBack(initState)

	explored := stateHashMap{}
	explored.add(initState)

	result.N = 0

	for queue.Len() > 0 {
		v := queue.Remove(queue.Front())
		state := v.(*State)
		result.N++

		if goalPredicate != nil && goalPredicate(state) {
			result.Targets = append(result.Targets, state)
		}

		// No need to add more states into the queue if the depth is too large
		if state.Depth == depth {
			continue
		}

		for _, newState := range state.NextStates() {
			if explored.has(newState) {
				continue
			}

			if !validate(newState) {
				result.Success = false
				result.Invalidate = newState
				return
			}

			explored.add(newState)
			queue.PushBack(newState)
		}
	}

	result.Success = true
	return
}

func RandomWalkFind(initState *State, validate, goalPredicate func(*State) bool, depth int) (result SearchResult) {
	if depth < 0 {
		panic("depth must be non-negative")
	}

	state := initState

	for state.Depth <= depth {
		if !validate(state) {
			result.Success = false
			result.Invalidate = state
			return
		}

		if goalPredicate(state) {
			result.Success = true
			result.Targets = append(result.Targets, state)
			return
		}

		state = state.RandomNextState() // pick a random state from the next states
		if state == nil {
			break
		}
	}

	return
}

func RandomWalkValidate(initState *State, validate, goalPredicate func(*State) bool, depth int) (result SearchResult) {
	if depth < 0 {
		panic("depth must be non-negative")
	}

	state := initState
	for state.Depth <= depth {
		if !validate(state) {
			result.Success = false
			result.Targets = nil
			result.Invalidate = state
			return
		}

		if goalPredicate != nil && goalPredicate(state) {
			result.Targets = []*State{state}
		}

		state = state.RandomNextState()
		if state == nil {
			break
		}
	}

	if state != nil {
		result.Success = true
		result.N = state.Depth
	}
	return
}

// BatchRandomWalkFind performs a batch random walk search.
// It will perform a random walk search for each batch.
// It will return the first successful search result.
func BatchRandomWalkFind(initState *State, validate, goalPredicate func(*State) bool,
	depth, batch int) (result SearchResult) {

	for i := 0; i < batch; i++ {
		res := RandomWalkFind(initState, validate, goalPredicate, depth)
		if res.Success {
			res.N = i + 1
			return res
		}
	}
	result.N = batch
	result.Success = false
	return
}

// BatchRandomWalkValidate performs a batch random walk search and returns the targets.
// Similar to BatchRandomWalkFind but focuses on validating states while conducting batched random walks.
func BatchRandomWalkValidate(initState *State, validate, goalPredicate func(*State) bool,
	depth, batch int) (result SearchResult) {

	for i := 0; i < batch; i++ {
		res := RandomWalkValidate(initState, validate, goalPredicate, depth)
		if !res.Success {
			return res
		}

		if len(res.Targets) > 0 {
			result.Targets = append(result.Targets, res.Targets[0])
		}
		result.N += res.N
	}

	result.Success = true
	return
}

// StateEdge represents an edge in the path from the final state to the initial state.
type StateEdge struct {
	From  *State // the previous state
	Event Event  // the event that leads to the previous state
	To    *State // the next state
}

// FindPath returns the final state and the path from the final state to the initial state.
func FindPath(final *State) (*State, []StateEdge) {
	edges := make([]StateEdge, 0, 16) // allocate enough space for the path

	// Follow the path from the final state to the initial state
	for final.Prev != nil {
		edge := StateEdge{
			From:  final.Prev,
			Event: final.Event,
			To:    final,
		}
		edges = append(edges, edge)
		final = final.Prev
	}
	reversePath(edges)
	return final, edges
}

func reversePath(path []StateEdge) {
	for i, j := 0, len(path)-1; i < len(path)/2; i, j = i+1, j-1 {
		path[i], path[j] = path[j], path[i]
	}
}

func PrintPath(path []StateEdge) {
	for _, edge := range path {
		instanceClass := reflect.TypeOf(edge.Event.Instance)
		fmt.Printf("Event: %s %s %v\n", edge.Event.Action, instanceClass, edge.Event.Instance)
	}
}
