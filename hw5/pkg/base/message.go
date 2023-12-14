package base

// Addess is a type for node address.
type Address string

// Message is a type for messages.
type Message interface {
	From() Address // sender address

	To() Address // receiver address

	Hash() uint64 // hash of the message

	Equals(Message) bool // check if two messages are equal
}

// A helper type for Message type.
// You could include this as an embedding field.
// Example:
// ```go
//
//	type MyMessage {
//	    CoreMessage
//	    ... // other fields
//	}
//
// ```
//
// The good thing to do this is that it could help you implement From and To methods.
type CoreMessage struct {
	from Address
	to   Address
}

func MakeCoreMessage(from, to Address) CoreMessage {
	return CoreMessage{
		from: from,
		to:   to,
	}
}

func (m *CoreMessage) From() Address {
	return m.from
}

func (m *CoreMessage) To() Address {
	return m.to
}

func (m *CoreMessage) Equals(other *CoreMessage) bool {
	return m.from == other.from && m.to == other.to
}
