package discovery

import (
	"context"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	"log/slog"
)

type Strategy string

const (
	DefaultMDNSService         = "_graft._tcp"
	DefaultDomain              = "local."
	DefaultKubernetesNamespace = "default"
	DefaultKubernetesService   = "graft-cluster"
)

type Manager struct {
	NodeID   string
	Service  string
	Domain   string
	logger   *slog.Logger
	peers    []ports.Peer
	Services []ports.DiscoveryPort
}

func NewManager(nodeID string, logger *slog.Logger) *Manager {
	return &Manager{
		NodeID:   nodeID,
		logger:   logger.With("component", "discovery", "subcomponent", "manager"),
		peers:    make([]ports.Peer, 0),
		Services: make([]ports.DiscoveryPort, 0),
	}
}

func (d *Manager) MDNS(args ...string) {
	mdnsService := DefaultMDNSService
	mdnsDomain := DefaultDomain
	host := d.NodeID

	if len(args) > 0 && args[0] != "" {
		mdnsService = args[0]
	}
	if len(args) > 1 && args[1] != "" {
		mdnsDomain = args[1]
	}
	if len(args) > 2 && args[2] != "" {
		host = args[2]
	}

	mdnsAdapter := NewMDNSAdapter(mdnsService, mdnsDomain, host, d.logger)
	d.Services = append(d.Services, mdnsAdapter)
}

func (d *Manager) Kubernetes(args ...string) {
	namespace := DefaultKubernetesNamespace
	serviceName := DefaultKubernetesService

	if len(args) > 0 && args[0] != "" {
		if len(args) == 1 {
			namespace = args[0]
		} else {
			serviceName = args[0]
		}
	}
	if len(args) > 1 && args[1] != "" {
		namespace = args[1]
	}

	config := &ports.KubernetesConfig{
		AuthMethod: ports.AuthInCluster,
		Namespace:  namespace,
		Discovery: ports.DiscoveryStrategy{
			Method:      ports.DiscoveryService,
			ServiceName: serviceName,
		},
		PeerID: ports.PeerIDStrategy{
			Source: ports.PeerIDPodName,
		},
		NetworkingMode: ports.NetworkingPodIP,
		WatchInterval:  30 * time.Second,
		BufferSize:     100,
	}

	k8sAdapter := NewKubernetesAdapter(config, d.logger)
	d.Services = append(d.Services, k8sAdapter)
}

func (d *Manager) Static(peers []ports.Peer) {
	d.peers = append(d.peers, peers...)
}

func (d *Manager) GetPeers() []ports.Peer {
	allPeers := make([]ports.Peer, len(d.peers))
	copy(allPeers, d.peers)

	for _, service := range d.Services {
		servicePeers := service.GetPeers()
		allPeers = append(allPeers, servicePeers...)
	}

	uniquePeers := make(map[string]ports.Peer)
	for _, peer := range allPeers {
		uniquePeers[peer.ID] = peer
	}

	result := make([]ports.Peer, 0, len(uniquePeers))
	for _, peer := range uniquePeers {
		result = append(result, peer)
	}

	return result
}

func (d *Manager) Start(ctx context.Context, address string, port int) error {
	node := ports.NodeInfo{
		ID:      d.NodeID,
		Address: address,
		Port:    port,
		Metadata: map[string]string{
			"version": "1.0.0",
		},
	}

	d.logger.Debug("announcing static peers", "count", len(d.peers))
	for _, service := range d.Services {
		err := service.Announce(node)
		if err != nil {
			return err
		}

		err = service.Start(ctx)
		if err != nil {
			return err
		}
	}

	return nil
}

func (d *Manager) Stop() error {
	for _, service := range d.Services {
		err := service.Stop()
		if err != nil {
			return err
		}
	}

	return nil
}
