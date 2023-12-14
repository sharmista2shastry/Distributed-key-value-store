package base

// Event types.

const (
	Empty           = "empty"
	UnknownDest     = "unknown destination"
	Partition       = "partition"
	DropOff         = "drop off"
	Handle          = "handle"
	HandleDuplicate = "handle duplicate"
	Trigger         = "trigger"
)

// Event type is used when a new state is inherit from an old one.
// It contains the action and the instance.

type Event struct {
	Action   string // action type
	Instance interface{}
}

// EmptyEvent is used when there is no event.
func EmptyEvent() Event {
	return Event{
		Action:   Empty,
		Instance: nil,
	}
}

// UnknownDestinationEvent is used when the message is sent to an unknown destination.
func UnknownDestinationEvent(m Message) Event {
	return Event{
		Action:   UnknownDest,
		Instance: m,
	}
}

// PartitionEvent is used when there is a network partition and the message never reaches the destination.
func PartitionEvent(m Message) Event {
	return Event{
		Action:   Partition,
		Instance: m,
	}
}

// DropOffEvent is used when the message is dropped during the transmission.
func DropOffEvent(m Message) Event {
	return Event{
		Action:   DropOff,
		Instance: m,
	}
}

// HandleEvent is used when the message arrives at the destination normally.
func HandleEvent(m Message) Event {
	return Event{
		Action:   Handle,
		Instance: m,
	}
}

// HandleDuplicateEvent is used when the message arrives at the destination but is never deleted from the network (i.e. it is duplicate and may arrive again later).
func HandleDuplicateEvent(m Message) Event {
	return Event{
		Action:   HandleDuplicate,
		Instance: m,
	}
}

// TimerInstance is a struct that contains the address of the node that triggers the timer and the timer itself.
type TimerInstance struct {
	Address
	Timer
}

// TriggerEvent is used when the timer expires.
func TriggerEvent(addr Address, t Timer) Event {
	return Event{
		Action: Trigger,
		Instance: TimerInstance{
			Address: addr,
			Timer:   t,
		},
	}
}
