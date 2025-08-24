package resource_manager

import (
	"sort"
	"sync"
)

type PriorityManager struct {
	nodePriorities map[string]int
	roundRobin     map[int][]string
	lastIndex      map[int]int
	mu             sync.RWMutex
}

func NewPriorityManager() *PriorityManager {
	return &PriorityManager{
		nodePriorities: make(map[string]int),
		roundRobin:     make(map[int][]string),
		lastIndex:      make(map[int]int),
	}
}

func (pm *PriorityManager) SetNodePriority(nodeType string, priority int) {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	oldPriority, existed := pm.nodePriorities[nodeType]
	if existed {
		pm.removeNodeFromPriority(nodeType, oldPriority)
	}

	pm.nodePriorities[nodeType] = priority
	pm.addNodeToPriority(nodeType, priority)
}

func (pm *PriorityManager) GetNodePriority(nodeType string) int {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	priority, exists := pm.nodePriorities[nodeType]
	if !exists {
		return 0
	}
	return priority
}

func (pm *PriorityManager) GetNextNodeByPriority() string {
	pm.mu.Lock()
	defer pm.mu.Unlock()

	priorities := make([]int, 0, len(pm.roundRobin))
	for priority := range pm.roundRobin {
		priorities = append(priorities, priority)
	}

	if len(priorities) == 0 {
		return ""
	}

	sort.Sort(sort.Reverse(sort.IntSlice(priorities)))

	for _, priority := range priorities {
		nodes := pm.roundRobin[priority]
		if len(nodes) == 0 {
			continue
		}

		lastIdx := pm.lastIndex[priority]
		nextIdx := (lastIdx + 1) % len(nodes)
		pm.lastIndex[priority] = nextIdx

		return nodes[nextIdx]
	}

	return ""
}

func (pm *PriorityManager) GetNodesByPriority() map[int][]string {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	result := make(map[int][]string)
	for priority, nodes := range pm.roundRobin {
		result[priority] = make([]string, len(nodes))
		copy(result[priority], nodes)
	}
	return result
}

func (pm *PriorityManager) removeNodeFromPriority(nodeType string, priority int) {
	nodes := pm.roundRobin[priority]
	for i, node := range nodes {
		if node == nodeType {
			pm.roundRobin[priority] = append(nodes[:i], nodes[i+1:]...)
			break
		}
	}

	if len(pm.roundRobin[priority]) == 0 {
		delete(pm.roundRobin, priority)
		delete(pm.lastIndex, priority)
	}
}

func (pm *PriorityManager) addNodeToPriority(nodeType string, priority int) {
	if pm.roundRobin[priority] == nil {
		pm.roundRobin[priority] = make([]string, 0)
		pm.lastIndex[priority] = -1
	}

	pm.roundRobin[priority] = append(pm.roundRobin[priority], nodeType)
}

func (pm *PriorityManager) CanExecuteWithPriority(nodeType string, requestPriority int) bool {
	pm.mu.RLock()
	defer pm.mu.RUnlock()

	nodePriority, exists := pm.nodePriorities[nodeType]
	if !exists {
		nodePriority = 0
	}

	return requestPriority <= nodePriority
}
