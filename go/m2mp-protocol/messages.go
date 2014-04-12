package m2mprotocol

// Disconnection
type EventDisconnected struct {
	Error error
}

type MessageIdentRequest struct {
	Ident string
}

type MessageIdentResponse struct {
	Ok bool
}

// Ping request
type MessagePingRequest struct {
	Data byte
}

// Ping response
type MessagePingResponse struct {
	Data byte
}

// Simple message
type MessageDataSimple struct {
	Channel string
	Data    []byte
}

// Array of data message
type MessageDataArray struct {
	Channel string
	Data    [][]byte
}
