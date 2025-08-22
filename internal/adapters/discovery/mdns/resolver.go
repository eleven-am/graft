package mdns

import (
	"context"
	"log/slog"
	"strings"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/hashicorp/mdns"
)

type MDNSResolver struct {
	logger      *slog.Logger
	serviceName string
}

func NewResolver(logger *slog.Logger, serviceName string) *MDNSResolver {
	if logger == nil {
		logger = slog.Default()
	}

	return &MDNSResolver{
		logger:      logger.With("component", "discovery", "module", "resolver"),
		serviceName: serviceName,
	}
}

func (r *MDNSResolver) Discover(timeout time.Duration) ([]*mdns.ServiceEntry, error) {
	entriesCh := make(chan *mdns.ServiceEntry, 10)
	var entries []*mdns.ServiceEntry

	go func() {
		for entry := range entriesCh {
			entries = append(entries, entry)
			r.logger.Debug("discovered service",
				"name", entry.Name,
				"host", entry.Host,
				"addr", entry.AddrV4,
				"port", entry.Port)
		}
	}()

	params := &mdns.QueryParam{
		Service:             r.serviceName,
		Domain:              "local",
		Timeout:             timeout,
		Entries:             entriesCh,
		WantUnicastResponse: true,
	}

	err := mdns.Query(params)
	close(entriesCh)

	if err != nil {
		if isNetworkUnavailableError(err) {
			r.logger.Debug("mDNS network unavailable, returning empty results",
				"service", r.serviceName,
				"error", err.Error())
			return entries, nil
		}
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "mDNS query failed",
			Details: map[string]interface{}{
				"service_name": r.serviceName,
				"timeout":      timeout.String(),
				"error":        err.Error(),
			},
		}
	}

	r.logger.Debug("mDNS discovery completed",
		"service", r.serviceName,
		"found", len(entries))

	return entries, nil
}

func (r *MDNSResolver) Browse(ctx context.Context) (<-chan *mdns.ServiceEntry, error) {
	entriesCh := make(chan *mdns.ServiceEntry, 10)

	params := &mdns.QueryParam{
		Service:             r.serviceName,
		Domain:              "local",
		Entries:             entriesCh,
		WantUnicastResponse: false,
	}

	go func() {
		ticker := time.NewTicker(30 * time.Second)
		defer ticker.Stop()
		defer close(entriesCh)

		for {
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
				if err := mdns.Query(params); err != nil {
					if isNetworkUnavailableError(err) {
						r.logger.Debug("mDNS network unavailable during browse",
							"error", err.Error())
					} else {
						r.logger.Error("mDNS browse query failed",
							"error", err.Error())
					}
				}
			}
		}
	}()

	return entriesCh, nil
}

func isNetworkUnavailableError(err error) bool {
	if err == nil {
		return false
	}
	errStr := err.Error()
	return strings.Contains(errStr, "no route to host") ||
		strings.Contains(errStr, "network is unreachable") ||
		strings.Contains(errStr, "connection refused") ||
		strings.Contains(errStr, "address not available")
}