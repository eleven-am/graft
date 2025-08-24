package resource_manager

import (
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type ResourcePoolManager struct {
	pools map[string]*Pool
	mu    sync.RWMutex
}

type Pool struct {
	nodeType     string
	totalSlots   int
	activeSlots  int
	priority     int
	lastActivity time.Time
	mu           sync.RWMutex
}

func NewResourcePoolManager() *ResourcePoolManager {
	return &ResourcePoolManager{
		pools: make(map[string]*Pool),
	}
}

func (rpm *ResourcePoolManager) CreatePool(nodeType string, totalSlots, priority int) {
	rpm.mu.Lock()
	defer rpm.mu.Unlock()

	rpm.pools[nodeType] = &Pool{
		nodeType:     nodeType,
		totalSlots:   totalSlots,
		activeSlots:  0,
		priority:     priority,
		lastActivity: time.Now(),
	}
}

func (rpm *ResourcePoolManager) AcquireSlot(nodeType string) bool {
	rpm.mu.RLock()
	pool, exists := rpm.pools[nodeType]
	rpm.mu.RUnlock()

	if !exists {
		return false
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.activeSlots >= pool.totalSlots {
		return false
	}

	pool.activeSlots++
	pool.lastActivity = time.Now()
	return true
}

func (rpm *ResourcePoolManager) ReleaseSlot(nodeType string) bool {
	rpm.mu.RLock()
	pool, exists := rpm.pools[nodeType]
	rpm.mu.RUnlock()

	if !exists {
		return false
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if pool.activeSlots <= 0 {
		return false
	}

	pool.activeSlots--
	pool.lastActivity = time.Now()
	return true
}

func (rpm *ResourcePoolManager) GetPools() []ports.ResourcePool {
	rpm.mu.RLock()
	defer rpm.mu.RUnlock()

	pools := make([]ports.ResourcePool, 0, len(rpm.pools))
	for _, pool := range rpm.pools {
		pool.mu.RLock()
		pools = append(pools, ports.ResourcePool{
			NodeType:     pool.nodeType,
			TotalSlots:   pool.totalSlots,
			ActiveSlots:  pool.activeSlots,
			IdleSlots:    pool.totalSlots - pool.activeSlots,
			Priority:     pool.priority,
			LastActivity: pool.lastActivity,
		})
		pool.mu.RUnlock()
	}

	return pools
}

func (rpm *ResourcePoolManager) UpdatePool(nodeType string, totalSlots, priority int) bool {
	rpm.mu.RLock()
	pool, exists := rpm.pools[nodeType]
	rpm.mu.RUnlock()

	if !exists {
		return false
	}

	pool.mu.Lock()
	defer pool.mu.Unlock()

	if totalSlots < pool.activeSlots {
		return false
	}

	pool.totalSlots = totalSlots
	pool.priority = priority
	return true
}

func (rpm *ResourcePoolManager) GetPoolUtilization(nodeType string) float64 {
	rpm.mu.RLock()
	pool, exists := rpm.pools[nodeType]
	rpm.mu.RUnlock()

	if !exists {
		return 0.0
	}

	pool.mu.RLock()
	defer pool.mu.RUnlock()

	if pool.totalSlots == 0 {
		return 0.0
	}

	return float64(pool.activeSlots) / float64(pool.totalSlots)
}
