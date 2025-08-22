package mdns

import (
	"log/slog"
	"net"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/hashicorp/mdns"
)

type Broadcaster struct {
	logger      *slog.Logger
	server      *mdns.Server
	serviceName string
	servicePort int
}

func NewBroadcaster(logger *slog.Logger, serviceName string, servicePort int) *Broadcaster {
	if logger == nil {
		logger = slog.Default()
	}

	return &Broadcaster{
		logger:      logger.With("component", "discovery", "module", "broadcaster"),
		serviceName: serviceName,
		servicePort: servicePort,
	}
}

func (b *Broadcaster) Start() error {
	host, err := getHostname()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get hostname for mDNS broadcaster",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	ips, err := getLocalIPs()
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to get local IP addresses for mDNS",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	if len(ips) == 0 {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "no local IP addresses found for mDNS broadcasting",
			Details: map[string]interface{}{
				"service_name": b.serviceName,
				"service_port": b.servicePort,
			},
		}
	}

	info := []string{"graft-node"}
	service, err := mdns.NewMDNSService(
		b.serviceName,
		"_graft._tcp",
		"",
		"",
		b.servicePort,
		ips,
		info,
	)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create mDNS service",
			Details: map[string]interface{}{
				"service_name": b.serviceName,
				"service_port": b.servicePort,
				"ip_count":     len(ips),
				"error":        err.Error(),
			},
		}
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: service})
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create mDNS server",
			Details: map[string]interface{}{
				"service_name": b.serviceName,
				"error":        err.Error(),
			},
		}
	}

	b.server = server

	b.logger.Info("mDNS broadcaster started",
		"service", b.serviceName,
		"port", b.servicePort,
		"host", host,
		"ip", ips[0].String())

	return nil
}

func (b *Broadcaster) Stop() {
	if b.server != nil {
		b.server.Shutdown()
		b.server = nil
		b.logger.Info("mDNS broadcaster stopped")
	}
}

func (b *Broadcaster) UpdateService(info ports.ServiceInfo) error {
	b.logger.Debug("service info updated",
		"service_id", info.ID,
		"metadata_count", len(info.Metadata))
	
	return nil
}

func getHostname() (string, error) {
	hostname, err := net.LookupAddr("127.0.0.1")
	if err == nil && len(hostname) > 0 {
		return hostname[0], nil
	}
	
	return "graft.local", nil
}

func getLocalIPs() ([]net.IP, error) {
	var ips []net.IP
	
	interfaces, err := net.Interfaces()
	if err != nil {
		return nil, err
	}

	for _, iface := range interfaces {
		if iface.Flags&net.FlagUp == 0 || iface.Flags&net.FlagLoopback != 0 {
			continue
		}

		addrs, err := iface.Addrs()
		if err != nil {
			continue
		}

		for _, addr := range addrs {
			var ip net.IP
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}

			if ip == nil || ip.IsLoopback() {
				continue
			}

			ip = ip.To4()
			if ip == nil {
				continue
			}

			ips = append(ips, ip)
		}
	}

	if len(ips) == 0 {
		ips = append(ips, net.IPv4(127, 0, 0, 1))
	}

	return ips, nil
}