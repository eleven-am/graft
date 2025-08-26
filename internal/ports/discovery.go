package ports

import (
	"context"
	"time"
)

type Peer struct {
	ID       string
	Address  string
	Port     int
	Metadata map[string]string
}

type NodeInfo = Peer

type DiscoveryPort interface {
	Start(ctx context.Context) error
	Stop() error
	GetPeers() []Peer
	Announce(nodeInfo NodeInfo) error
}

type DiscoveryManager interface {
	Start(ctx context.Context, address string, port int) error
	Stop() error
	GetPeers() []Peer
	MDNS(args ...string)
	Kubernetes(args ...string)
	Static(peers []Peer)
}

type AuthMethod int

const (
	AuthInCluster AuthMethod = iota
	AuthKubeconfig
	AuthExplicitToken
)

type DiscoveryMethod int

const (
	DiscoveryLabelSelector DiscoveryMethod = iota
	DiscoveryService
	DiscoveryDNS
	DiscoveryStatefulSet
	DiscoveryNamespace
	DiscoverySiblings
)

type PeerIDSource int

const (
	PeerIDPodName PeerIDSource = iota
	PeerIDAnnotation
	PeerIDLabel
	PeerIDTemplate
)

type PortSource int

const (
	PortNamedPort PortSource = iota
	PortAnnotation
	PortFirstPort
	PortEnvVar
)

type NetworkingMode int

const (
	NetworkingPodIP NetworkingMode = iota
	NetworkingServiceIP
	NetworkingNodePort
)

type RetryStrategy struct {
	MaxRetries    int
	InitialDelay  time.Duration
	MaxDelay      time.Duration
	BackoffFactor float64
}

type DiscoveryStrategy struct {
	Method DiscoveryMethod

	LabelSelector   map[string]string
	ServiceName     string
	StatefulSetName string
}

type PeerIDStrategy struct {
	Source   PeerIDSource
	Key      string
	Template string
}

type PortStrategy struct {
	Source        PortSource
	PortName      string
	AnnotationKey string
	EnvVarName    string
	DefaultPort   int
}

type KubernetesConfig struct {
	AuthMethod     AuthMethod
	KubeconfigPath string
	Token          string
	APIServer      string

	Namespace         string
	AnnotationFilters map[string]string
	FieldSelector     string
	RequireReady      bool

	Discovery DiscoveryStrategy
	PeerID    PeerIDStrategy
	Port      PortStrategy

	NetworkingMode NetworkingMode

	WatchInterval time.Duration
	RetryStrategy RetryStrategy
	BufferSize    int
}