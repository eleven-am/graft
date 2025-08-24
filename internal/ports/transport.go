package ports

import "context"

type TransportPort interface {
	Start(ctx context.Context, address string, port int) error
	Stop() error
	SendToLeader(ctx context.Context, message Message) (*Response, error)
	SetMessageHandler(handler MessageHandler)
	UpdateLeader(nodeID string, address string)
	GetAddress() string
	SetRaft(raft RaftPort)
	RequestJoinFromPeer(ctx context.Context, peerAddress string, nodeInfo NodeInfo) error
}

type Message struct {
	Type    MessageType
	From    string
	Payload []byte
}

type Response struct {
	Success bool
	Data    []byte
	Error   string
}

type MessageHandler interface {
	HandleMessage(ctx context.Context, message Message) (*Response, error)
}

type MessageType int

const (
	StorageRead MessageType = iota
	StorageWrite
	StorageDelete
	HealthCheck
)
