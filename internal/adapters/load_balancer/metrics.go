package load_balancer

import "sync"

type RollingWindow struct {
	mu        sync.Mutex
	results   []bool
	maxSize   int
	pointer   int
	fullCycle bool
}

func NewRollingWindow(size int) *RollingWindow {
	return &RollingWindow{
		results: make([]bool, size),
		maxSize: size,
	}
}

func (r *RollingWindow) Record(success bool) {
	r.mu.Lock()
	defer r.mu.Unlock()

	r.results[r.pointer] = success
	r.pointer = (r.pointer + 1) % r.maxSize

	if r.pointer == 0 {
		r.fullCycle = true
	}
}

func (r *RollingWindow) GetErrorRate() float64 {
	r.mu.Lock()
	defer r.mu.Unlock()

	count := r.maxSize
	if !r.fullCycle {
		count = r.pointer
	}

	if count == 0 {
		return 0
	}

	failures := 0
	for i := 0; i < count; i++ {
		if !r.results[i] {
			failures++
		}
	}

	return float64(failures) / float64(count)
}

func UpdateEWMA(oldValue, newValue, alpha float64) float64 {
	if oldValue == 0 {
		return newValue
	}
	return oldValue*(1-alpha) + newValue*alpha
}
