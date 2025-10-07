package netutil

import (
	"fmt"
	"net"

	"github.com/eleven-am/graft/internal/domain"
)

// ListenTCP creates a TCP listener on the specified address and port.
// If port is 0, the OS will automatically assign an available port.
// Returns the listener and the actual port number used.
func ListenTCP(host string, port int) (net.Listener, int, error) {
	addr := fmt.Sprintf("%s:%d", host, port)
	listener, err := net.Listen("tcp", addr)
	if err != nil {
		return nil, 0, domain.NewNetworkError(
			fmt.Sprintf("failed to listen on %s", addr),
			err,
			domain.WithComponent("helpers.netutil.ListenTCP"),
			domain.WithContextDetail("address", addr),
		)
	}

	actualPort := listener.Addr().(*net.TCPAddr).Port
	return listener, actualPort, nil
}
