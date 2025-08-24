package transport

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestGRPCTransport_BasicLifecycle(t *testing.T) {
	transport := NewGRPCTransport(nil)

	ctx := context.Background()
	err := transport.Start(ctx, "127.0.0.1", 0) // Use port 0 for auto-assignment
	require.NoError(t, err)

	address := transport.GetAddress()
	assert.NotEmpty(t, address)
	assert.Contains(t, address, "127.0.0.1:")

	time.Sleep(10 * time.Millisecond)

	err = transport.Stop()
	assert.NoError(t, err)
}

func TestGRPCTransport_CannotStartTwice(t *testing.T) {
	transport := NewGRPCTransport(nil)
	ctx := context.Background()

	err := transport.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport.Stop()

	time.Sleep(10 * time.Millisecond)

	err = transport.Start(ctx, "127.0.0.1", 0)
	assert.Error(t, err)
	assert.True(t, domain.IsTransportError(err))
	assert.True(t, domain.IsAlreadyStarted(err))
}

func TestGRPCTransport_CannotStopWhenNotStarted(t *testing.T) {
	transport := NewGRPCTransport(nil)

	err := transport.Stop()
	assert.Error(t, err)
	assert.True(t, domain.IsTransportError(err))
	assert.True(t, domain.IsNotStarted(err))
}

func TestGRPCTransport_MessageHandlerRegistration(t *testing.T) {
	transport := NewGRPCTransport(nil)

	handler := &mockMessageHandler{}
	transport.SetMessageHandler(handler)

	ctx := context.Background()
	err := transport.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport.Stop()

	newHandler := &mockMessageHandler{}
	transport.SetMessageHandler(newHandler)
}

func TestGRPCTransport_LeaderUpdateAndConnection(t *testing.T) {
	transport := NewGRPCTransport(nil)
	ctx := context.Background()

	err := transport.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport.Stop()

	address := transport.GetAddress()
	assert.NotEmpty(t, address)

	transport.UpdateLeader("leader-node-1", "127.0.0.1:9999")

	transport.UpdateLeader("leader-node-1", "127.0.0.1:9999")

	transport.UpdateLeader("leader-node-2", "127.0.0.1:9998")

	transport.UpdateLeader("", "")
}

func TestGRPCTransport_SendToLeaderWithoutLeader(t *testing.T) {
	transport := NewGRPCTransport(nil)
	ctx := context.Background()

	err := transport.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport.Stop()

	message := ports.Message{
		Type:    ports.HealthCheck,
		From:    "test-node",
		Payload: []byte("test"),
	}

	resp, err := transport.SendToLeader(ctx, message)
	assert.Error(t, err)
	assert.Nil(t, resp)

	var leaderErr *domain.LeaderError
	assert.True(t, errors.As(err, &leaderErr))
	assert.Equal(t, "send", leaderErr.Op)
}

func TestGRPCTransport_EndToEndCommunication(t *testing.T) {
	transport1 := NewGRPCTransport(nil)
	transport2 := NewGRPCTransport(nil)

	ctx := context.Background()

	err := transport1.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport1.Stop()

	err = transport2.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport2.Stop()

	handler := &mockMessageHandler{
		response: &ports.Response{
			Success: true,
			Data:    []byte("response-data"),
			Error:   "",
		},
	}
	transport2.SetMessageHandler(handler)

	transport1.UpdateLeader("leader-node", transport2.GetAddress())

	time.Sleep(100 * time.Millisecond)

	message := ports.Message{
		Type:    ports.StorageRead,
		From:    "node-1",
		Payload: []byte("test-payload"),
	}

	resp, err := transport1.SendToLeader(ctx, message)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.True(t, resp.Success)
	assert.Equal(t, []byte("response-data"), resp.Data)
	assert.Empty(t, resp.Error)

	assert.True(t, handler.called)
	assert.Equal(t, ports.StorageRead, handler.lastMessage.Type)
	assert.Equal(t, "node-1", handler.lastMessage.From)
	assert.Equal(t, []byte("test-payload"), handler.lastMessage.Payload)
}

func TestGRPCTransport_MessageTypeConversion(t *testing.T) {
	transport := NewGRPCTransport(nil)

	testCases := []ports.MessageType{
		ports.StorageRead,
		ports.StorageWrite,
		ports.StorageDelete,
		ports.HealthCheck,
	}

	for _, msgType := range testCases {
		protoType := transport.convertMessageType(msgType)
		backToPortsType := transport.convertFromProtoMessageType(protoType)
		assert.Equal(t, msgType, backToPortsType, "Round-trip conversion failed for %v", msgType)
	}
}

func TestGRPCTransport_MessageHandlingWithoutHandler(t *testing.T) {
	transport1 := NewGRPCTransport(nil)
	transport2 := NewGRPCTransport(nil)

	ctx := context.Background()

	err := transport1.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport1.Stop()

	err = transport2.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport2.Stop()

	transport1.UpdateLeader("leader-node", transport2.GetAddress())
	time.Sleep(100 * time.Millisecond)

	message := ports.Message{
		Type:    ports.HealthCheck,
		From:    "node-1",
		Payload: []byte("test"),
	}

	resp, err := transport1.SendToLeader(ctx, message)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.False(t, resp.Success)
	assert.Contains(t, resp.Error, "no message handler configured")
}

func TestGRPCTransport_HandlerError(t *testing.T) {
	transport1 := NewGRPCTransport(nil)
	transport2 := NewGRPCTransport(nil)

	ctx := context.Background()

	err := transport1.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport1.Stop()

	err = transport2.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport2.Stop()

	handler := &mockMessageHandler{
		shouldError: true,
	}
	transport2.SetMessageHandler(handler)

	transport1.UpdateLeader("leader-node", transport2.GetAddress())
	time.Sleep(100 * time.Millisecond)

	message := ports.Message{
		Type:    ports.StorageWrite,
		From:    "node-1",
		Payload: []byte("test"),
	}

	resp, err := transport1.SendToLeader(ctx, message)
	require.NoError(t, err)
	require.NotNil(t, resp)

	assert.False(t, resp.Success)
	assert.Equal(t, "handler error", resp.Error)
}

func TestGRPCTransport_HealthCheck(t *testing.T) {
	transport := NewGRPCTransport(nil)
	ctx := context.Background()

	err := transport.Start(ctx, "127.0.0.1", 0)
	require.NoError(t, err)
	defer transport.Stop()

	address := transport.GetAddress()
	assert.NotEmpty(t, address)

}

type mockMessageHandler struct {
	called      bool
	lastMessage ports.Message
	response    *ports.Response
	shouldError bool
}

func (m *mockMessageHandler) HandleMessage(ctx context.Context, message ports.Message) (*ports.Response, error) {
	m.called = true
	m.lastMessage = message

	if m.shouldError {
		return nil, errors.New("handler error")
	}

	if m.response != nil {
		return m.response, nil
	}

	return &ports.Response{
		Success: true,
		Data:    []byte("default-response"),
		Error:   "",
	}, nil
}
