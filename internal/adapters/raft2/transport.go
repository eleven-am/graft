package raft2

import (
	"context"
	"fmt"
	"io"
	"net"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/helpers/netutil"
	"github.com/hashicorp/raft"
)

// TCPTransportProvider builds hashicorp/raft TCP transports using the
// controller's bind address.
type TCPTransportProvider struct {
	MaxPool int
	Timeout time.Duration
}

// Create implements the TransportProvider interface.
func (p *TCPTransportProvider) Create(_ context.Context, opts domain.RaftControllerOptions) (raft.Transport, raft.ServerAddress, error) {
	bind := opts.BindAddress
	if bind == "" {
		return nil, "", fmt.Errorf("raft2: bind address required for transport")
	}

	host, portStr, err := net.SplitHostPort(bind)
	if err != nil {
		host = bind
		portStr = "0"
	}

	var port int
	if portStr != "" {
		_, err := fmt.Sscanf(portStr, "%d", &port)
		if err != nil {
			port = 0
		}
	}

	listener, actualPort, err := netutil.ListenTCP(host, port)
	if err != nil {
		return nil, "", fmt.Errorf("raft2: listen tcp: %w", err)
	}
	listener.Close()

	actualAddr := fmt.Sprintf("%s:%d", host, actualPort)
	tcpAddr, err := net.ResolveTCPAddr("tcp", actualAddr)
	if err != nil {
		return nil, "", fmt.Errorf("raft2: resolve tcp addr: %w", err)
	}

	maxPool := p.MaxPool
	if maxPool <= 0 {
		maxPool = 3
	}

	timeout := p.Timeout
	if timeout <= 0 {
		timeout = 10 * time.Second
	}

	transport, err := raft.NewTCPTransport(actualAddr, tcpAddr, maxPool, timeout, io.Discard)
	if err != nil {
		return nil, "", fmt.Errorf("raft2: create tcp transport: %w", err)
	}

	return transport, raft.ServerAddress(transport.LocalAddr()), nil
}
