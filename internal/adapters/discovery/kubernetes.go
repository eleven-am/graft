package discovery

import (
	"context"
	"fmt"
	"log/slog"
	"net"
	"sort"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

const (
	DefaultKubernetesRefreshPeriod = 10 * time.Second
	DefaultKubernetesQueryTimeout  = 5 * time.Second
	DefaultKubernetesDNSSuffix     = "svc.cluster.local"
	DefaultKubernetesGRPCPortName  = "_grpc._tcp"
	DefaultStaleCacheMultiplier    = 5
)

type KubernetesConfig struct {
	ServiceName   string
	Namespace     string
	DNSSuffix     string
	GRPCPortName  string
	DefaultPort   int
	RefreshPeriod time.Duration
	QueryTimeout  time.Duration
}

func (c *KubernetesConfig) applyDefaults() {
	if c.DNSSuffix == "" {
		c.DNSSuffix = DefaultKubernetesDNSSuffix
	}
	if c.GRPCPortName == "" {
		c.GRPCPortName = DefaultKubernetesGRPCPortName
	}
	if c.RefreshPeriod <= 0 {
		c.RefreshPeriod = DefaultKubernetesRefreshPeriod
	}
	if c.QueryTimeout <= 0 {
		c.QueryTimeout = DefaultKubernetesQueryTimeout
	}
}

func (c *KubernetesConfig) fqdn() string {
	return fmt.Sprintf("%s.%s.%s", c.ServiceName, c.Namespace, c.DNSSuffix)
}

func (c *KubernetesConfig) srvName() string {
	parts := strings.SplitN(c.GRPCPortName, ".", 2)
	service := strings.TrimPrefix(parts[0], "_")
	proto := "tcp"
	if len(parts) > 1 {
		proto = strings.TrimPrefix(parts[1], "_")
	}
	return fmt.Sprintf("_%s._%s.%s", service, proto, c.fqdn())
}

type cachedResult struct {
	peers     []ports.Peer
	expiresAt time.Time
	ttl       time.Duration
}

func (c *cachedResult) isStale(now time.Time) bool {
	return now.After(c.expiresAt)
}

func (c *cachedResult) isTooStale(now time.Time) bool {
	maxStale := c.expiresAt.Add(c.ttl * DefaultStaleCacheMultiplier)
	return now.After(maxStale)
}

type DNSResolver interface {
	LookupSRV(ctx context.Context, service, proto, name string) (string, []*net.SRV, error)
	LookupHost(ctx context.Context, host string) ([]string, error)
}

type netResolver struct {
	resolver *net.Resolver
}

func newNetResolver() *netResolver {
	return &netResolver{
		resolver: net.DefaultResolver,
	}
}

func (r *netResolver) LookupSRV(ctx context.Context, service, proto, name string) (string, []*net.SRV, error) {
	return r.resolver.LookupSRV(ctx, service, proto, name)
}

func (r *netResolver) LookupHost(ctx context.Context, host string) ([]string, error) {
	return r.resolver.LookupHost(ctx, host)
}

type KubernetesProvider struct {
	config    KubernetesConfig
	clusterID string
	localInfo ports.NodeInfo
	events    chan ports.Event
	logger    *slog.Logger
	resolver  DNSResolver

	mu     sync.RWMutex
	ctx    context.Context
	cancel context.CancelFunc
	cache  *cachedResult
	peers  map[string]ports.Peer
	wg     sync.WaitGroup
}

func NewKubernetesProvider(config KubernetesConfig, logger *slog.Logger) *KubernetesProvider {
	if logger == nil {
		logger = slog.Default()
	}
	config.applyDefaults()

	return &KubernetesProvider{
		config:   config,
		logger:   logger.With("component", "discovery", "provider", "kubernetes"),
		resolver: newNetResolver(),
		peers:    make(map[string]ports.Peer),
		events:   make(chan ports.Event, 100),
	}
}

func (k *KubernetesProvider) WithResolver(resolver DNSResolver) *KubernetesProvider {
	k.resolver = resolver
	return k
}

func (k *KubernetesProvider) Start(ctx context.Context, announce ports.NodeInfo) error {
	k.mu.Lock()
	defer k.mu.Unlock()

	if k.ctx != nil {
		return fmt.Errorf("kubernetes provider already started")
	}

	k.ctx, k.cancel = context.WithCancel(ctx)
	k.localInfo = announce
	if k.events == nil {
		k.events = make(chan ports.Event, 100)
	}

	if clusterID, ok := announce.Metadata["cluster_id"]; ok {
		k.clusterID = clusterID
	}

	k.logger.Debug("starting kubernetes discovery provider",
		"service", k.config.ServiceName,
		"namespace", k.config.Namespace,
		"fqdn", k.config.fqdn())

	k.wg.Add(1)
	go func() {
		defer k.wg.Done()
		k.discoveryLoop()
	}()

	k.logger.Debug("kubernetes discovery provider started")
	return nil
}

func (k *KubernetesProvider) Stop() error {
	k.mu.Lock()
	if k.cancel == nil {
		k.mu.Unlock()
		return fmt.Errorf("kubernetes provider not started")
	}

	cancel := k.cancel
	events := k.events

	k.logger.Debug("stopping kubernetes discovery provider")

	k.cancel = nil
	k.ctx = nil
	k.peers = make(map[string]ports.Peer)
	k.cache = nil
	k.events = nil

	k.mu.Unlock()

	cancel()
	k.wg.Wait()

	k.logger.Debug("kubernetes discovery provider stopped")
	if events != nil {
		close(events)
	}
	return nil
}

func (k *KubernetesProvider) Snapshot() []ports.Peer {
	k.mu.RLock()
	defer k.mu.RUnlock()

	peers := make([]ports.Peer, 0, len(k.peers))
	for _, peer := range k.peers {
		peers = append(peers, peer)
	}
	return peers
}

func (k *KubernetesProvider) Events() <-chan ports.Event {
	return k.events
}

func (k *KubernetesProvider) Name() string {
	return "kubernetes"
}

func (k *KubernetesProvider) discoveryLoop() {
	ticker := time.NewTicker(k.config.RefreshPeriod)
	defer ticker.Stop()

	k.performDiscovery()

	for {
		k.mu.RLock()
		ctx := k.ctx
		k.mu.RUnlock()

		if ctx == nil {
			return
		}

		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			k.performDiscovery()
		}
	}
}

func (k *KubernetesProvider) performDiscovery() {
	k.mu.RLock()
	ctx := k.ctx
	k.mu.RUnlock()

	if ctx == nil {
		return
	}

	queryCtx, cancel := context.WithTimeout(ctx, k.config.QueryTimeout)
	defer cancel()

	peers, ttl, err := k.discoverPeers(queryCtx)
	if err != nil {
		k.logger.Warn("DNS discovery failed, checking cache",
			"error", err)

		k.mu.RLock()
		cache := k.cache
		k.mu.RUnlock()

		if cache != nil {
			now := time.Now()
			if !cache.isTooStale(now) {
				k.logger.Debug("using stale cache",
					"expires_at", cache.expiresAt,
					"max_stale_at", cache.expiresAt.Add(cache.ttl*DefaultStaleCacheMultiplier))
				peers = cache.peers
			} else {
				k.logger.Error("cache too stale, cannot serve peers",
					"expires_at", cache.expiresAt,
					"max_stale_at", cache.expiresAt.Add(cache.ttl*DefaultStaleCacheMultiplier))
				return
			}
		} else {
			k.logger.Error("no cache available, cannot serve peers")
			return
		}
	} else {
		k.mu.Lock()
		k.cache = &cachedResult{
			peers:     peers,
			expiresAt: time.Now().Add(ttl),
			ttl:       ttl,
		}
		k.mu.Unlock()
	}

	newPeers := make(map[string]ports.Peer)
	for _, peer := range peers {
		key := k.peerKey(peer)
		newPeers[key] = peer
	}

	k.mu.Lock()
	oldPeers := k.peers
	k.peers = newPeers
	eventsToSend := k.computeEvents(oldPeers, newPeers)
	events := k.events
	k.mu.Unlock()

	for _, event := range eventsToSend {
		if events == nil {
			break
		}
		select {
		case events <- event:
		case <-ctx.Done():
			return
		default:
			k.logger.Warn("event channel full, dropping event",
				"type", event.Type,
				"peer_id", event.Peer.ID)
		}
	}
}

func (k *KubernetesProvider) discoverPeers(ctx context.Context) ([]ports.Peer, time.Duration, error) {
	peers, ttl, srvErr := k.lookupSRV(ctx)
	if srvErr == nil {
		return peers, ttl, nil
	}

	k.logger.Debug("SRV lookup failed, falling back to A records",
		"srv_name", k.config.srvName(),
		"error", srvErr)

	peers, aErr := k.fallbackToARecords(ctx)
	if aErr != nil {
		return nil, 0, fmt.Errorf("SRV lookup failed (%v) and A record fallback failed (%v)", srvErr, aErr)
	}

	return peers, k.config.RefreshPeriod, nil
}

func (k *KubernetesProvider) lookupSRV(ctx context.Context) ([]ports.Peer, time.Duration, error) {
	parts := strings.SplitN(k.config.GRPCPortName, ".", 2)
	service := strings.TrimPrefix(parts[0], "_")
	proto := "tcp"
	if len(parts) > 1 {
		proto = strings.TrimPrefix(parts[1], "_")
	}

	_, records, err := k.resolver.LookupSRV(ctx, service, proto, k.config.fqdn())
	if err != nil {
		return nil, 0, fmt.Errorf("SRV lookup: %w", err)
	}

	if len(records) == 0 {
		return []ports.Peer{}, k.config.RefreshPeriod, nil
	}

	var peers []ports.Peer
	ttl := k.config.RefreshPeriod

	for _, srv := range records {
		hostname := strings.TrimSuffix(srv.Target, ".")
		addrs, err := k.resolver.LookupHost(ctx, hostname)
		if err != nil {
			k.logger.Debug("failed to resolve SRV target",
				"target", hostname,
				"error", err)
			continue
		}
		if len(addrs) == 0 {
			continue
		}

		ordinal := k.extractOrdinal(hostname)
		peerID := k.generatePeerID(hostname, ordinal)

		if peerID == k.localInfo.ID {
			continue
		}

		addr := addrs[0]
		port := int(srv.Port)

		peer := ports.Peer{
			ID:      peerID,
			Address: addr,
			Port:    port,
			Metadata: map[string]string{
				"hostname":  hostname,
				"ordinal":   strconv.Itoa(ordinal),
				"grpc_addr": fmt.Sprintf("%s:%d", addr, port),
			},
		}

		if k.clusterID != "" {
			peer.Metadata["cluster_id"] = k.clusterID
		}

		peers = append(peers, peer)
	}

	sort.Slice(peers, func(i, j int) bool {
		ordI, _ := strconv.Atoi(peers[i].Metadata["ordinal"])
		ordJ, _ := strconv.Atoi(peers[j].Metadata["ordinal"])
		return ordI < ordJ
	})

	return peers, ttl, nil
}

func (k *KubernetesProvider) fallbackToARecords(ctx context.Context) ([]ports.Peer, error) {
	if k.config.DefaultPort <= 0 {
		return nil, fmt.Errorf("default port not configured for A record fallback")
	}

	addrs, err := k.resolver.LookupHost(ctx, k.config.fqdn())
	if err != nil {
		return nil, fmt.Errorf("A record lookup: %w", err)
	}

	if len(addrs) == 0 {
		return nil, fmt.Errorf("no A records found")
	}

	var peers []ports.Peer
	for _, addr := range addrs {
		peerID := fmt.Sprintf("%s-%s", k.config.ServiceName, addr)

		if addr == k.localInfo.Address {
			continue
		}

		peer := ports.Peer{
			ID:      peerID,
			Address: addr,
			Port:    k.config.DefaultPort,
			Metadata: map[string]string{
				"ordinal":   "-1",
				"grpc_addr": fmt.Sprintf("%s:%d", addr, k.config.DefaultPort),
				"fallback":  "true",
			},
		}

		if k.clusterID != "" {
			peer.Metadata["cluster_id"] = k.clusterID
		}

		peers = append(peers, peer)
	}

	return peers, nil
}

func (k *KubernetesProvider) extractOrdinal(hostname string) int {
	podName := strings.Split(hostname, ".")[0]

	parts := strings.Split(podName, "-")
	if len(parts) == 0 {
		return -1
	}

	lastPart := parts[len(parts)-1]
	ordinal, err := strconv.Atoi(lastPart)
	if err != nil {
		return -1
	}
	return ordinal
}

func (k *KubernetesProvider) generatePeerID(hostname string, ordinal int) string {
	if ordinal >= 0 {
		return fmt.Sprintf("%s-%d", k.config.ServiceName, ordinal)
	}
	return hostname
}

func (k *KubernetesProvider) peerKey(peer ports.Peer) string {
	return fmt.Sprintf("%s:%d", peer.Address, peer.Port)
}

func (k *KubernetesProvider) computeEvents(oldPeers, newPeers map[string]ports.Peer) []ports.Event {
	var events []ports.Event

	for key, newPeer := range newPeers {
		if oldPeer, exists := oldPeers[key]; exists {
			if k.peerChanged(oldPeer, newPeer) {
				events = append(events, ports.Event{
					Type: ports.PeerUpdated,
					Peer: newPeer,
				})
			}
		} else {
			events = append(events, ports.Event{
				Type: ports.PeerAdded,
				Peer: newPeer,
			})
		}
	}

	for key, oldPeer := range oldPeers {
		if _, exists := newPeers[key]; !exists {
			events = append(events, ports.Event{
				Type: ports.PeerRemoved,
				Peer: oldPeer,
			})
		}
	}

	return events
}

func (k *KubernetesProvider) peerChanged(old, new ports.Peer) bool {
	if old.ID != new.ID || old.Address != new.Address || old.Port != new.Port {
		return true
	}

	for key, newVal := range new.Metadata {
		if oldVal, ok := old.Metadata[key]; !ok || oldVal != newVal {
			return true
		}
	}

	return false
}

func (k *KubernetesProvider) GetSelfOrdinal() int {
	k.mu.RLock()
	defer k.mu.RUnlock()

	return k.extractOrdinal(k.localInfo.ID)
}
