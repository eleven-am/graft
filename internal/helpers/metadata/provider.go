package metadata

import (
	"sync"
	"time"

	"github.com/google/uuid"
)

// Provider is a shared bootstrap metadata provider that ensures consistent
// boot ID and launch timestamp across all components within a single node.
type Provider struct {
	once          sync.Once
	bootID        string
	launchTime    int64
	formattedTime string
}

var globalProvider = &Provider{}

// GetProvider returns the global shared metadata provider instance.
func GetProvider() *Provider {
	return globalProvider
}

// GetMetadata returns immutable bootstrap metadata from the shared provider.
// This ensures all subsystems (discovery, raft, manager) use the same values.
func (p *Provider) GetMetadata() map[string]string {
	p.once.Do(p.initialize)

	return map[string]string{
		BootIDKey:          p.bootID,
		LaunchTimestampKey: p.formattedTime,
	}
}

// GetBootID returns the boot ID from the shared provider.
func (p *Provider) GetBootID() string {
	p.once.Do(p.initialize)
	return p.bootID
}

// GetLaunchTimestamp returns the raw launch timestamp from the shared provider.
func (p *Provider) GetLaunchTimestamp() int64 {
	p.once.Do(p.initialize)
	return p.launchTime
}

// initialize sets up the bootstrap metadata once per provider lifetime.
func (p *Provider) initialize() {
	p.bootID = uuid.New().String()
	p.launchTime = time.Now().UnixNano()
	p.formattedTime = time.Unix(0, p.launchTime).Format(time.RFC3339Nano)
}

// Reset resets the global provider state - only for testing purposes.
func Reset() {
	globalProvider = &Provider{}
}
