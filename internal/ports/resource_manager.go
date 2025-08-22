package ports

type ResourceManagerPort interface {
	CanExecuteNode(nodeType string) bool
	AcquireNode(nodeType string) error
	ReleaseNode(nodeType string) error
	GetExecutionStats() ExecutionStats
	UpdateConfig(config ResourceConfig) error
	IsHealthy() bool
}

type ResourceConfig struct {
	MaxConcurrentTotal   int            `json:"max_concurrent_total"`
	MaxConcurrentPerType map[string]int `json:"max_concurrent_per_type"`
	DefaultPerTypeLimit  int            `json:"default_per_type_limit"`
}

type ExecutionStats struct {
	TotalExecuting    int            `json:"total_executing"`
	PerTypeExecuting  map[string]int `json:"per_type_executing"`
	TotalCapacity     int            `json:"total_capacity"`
	PerTypeCapacity   map[string]int `json:"per_type_capacity"`
	AvailableSlots    int            `json:"available_slots"`
}