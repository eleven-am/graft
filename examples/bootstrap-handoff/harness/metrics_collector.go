package harness

import (
	"sync"
	"time"
)

type HandoffMetrics struct {
	StartTime             time.Time
	EndTime               time.Time
	Duration              time.Duration
	ProvisionalNodeID     string
	SeniorNodeID          string
	StateTransitions      []StateTransition
	WorkflowInterruptions int
	PeerDiscoveryTime     time.Duration
	DemotionTime          time.Duration
	JoinTime              time.Duration
	ReadinessTime         time.Duration
}

type StateTransition struct {
	NodeID    string
	FromState string
	ToState   string
	Timestamp time.Time
}

type PerformanceMetrics struct {
	HandoffLatency          time.Duration
	WorkflowThroughput      float64
	ClusterFormationTime    time.Duration
	MetadataPropagationTime time.Duration
	ReadinessResponseTime   time.Duration
}

type MetricsCollector struct {
	handoffMetrics     *HandoffMetrics
	stateTransitions   []StateTransition
	performanceMetrics PerformanceMetrics
	mu                 sync.RWMutex
}

func NewMetricsCollector() *MetricsCollector {
	return &MetricsCollector{
		stateTransitions: make([]StateTransition, 0),
	}
}

func (mc *MetricsCollector) StartHandoffTracking(provisionalNodeID, seniorNodeID string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	mc.handoffMetrics = &HandoffMetrics{
		StartTime:         time.Now(),
		ProvisionalNodeID: provisionalNodeID,
		SeniorNodeID:      seniorNodeID,
		StateTransitions:  make([]StateTransition, 0),
	}
}

func (mc *MetricsCollector) RecordStateTransition(nodeID, fromState, toState string) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	transition := StateTransition{
		NodeID:    nodeID,
		FromState: fromState,
		ToState:   toState,
		Timestamp: time.Now(),
	}

	mc.stateTransitions = append(mc.stateTransitions, transition)

	if mc.handoffMetrics != nil {
		mc.handoffMetrics.StateTransitions = append(mc.handoffMetrics.StateTransitions, transition)
	}
}

func (mc *MetricsCollector) CompleteHandoffTracking() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.handoffMetrics != nil {
		mc.handoffMetrics.EndTime = time.Now()
		mc.handoffMetrics.Duration = mc.handoffMetrics.EndTime.Sub(mc.handoffMetrics.StartTime)
	}
}

func (mc *MetricsCollector) RecordPeerDiscoveryTime(duration time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.handoffMetrics != nil {
		mc.handoffMetrics.PeerDiscoveryTime = duration
	}
}

func (mc *MetricsCollector) RecordDemotionTime(duration time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.handoffMetrics != nil {
		mc.handoffMetrics.DemotionTime = duration
	}
}

func (mc *MetricsCollector) RecordJoinTime(duration time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.handoffMetrics != nil {
		mc.handoffMetrics.JoinTime = duration
	}
}

func (mc *MetricsCollector) RecordReadinessTime(duration time.Duration) {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.handoffMetrics != nil {
		mc.handoffMetrics.ReadinessTime = duration
	}
	mc.performanceMetrics.ReadinessResponseTime = duration
}

func (mc *MetricsCollector) IncrementWorkflowInterruptions() {
	mc.mu.Lock()
	defer mc.mu.Unlock()

	if mc.handoffMetrics != nil {
		mc.handoffMetrics.WorkflowInterruptions++
	}
}

func (mc *MetricsCollector) SetPerformanceMetrics(metrics PerformanceMetrics) {
	mc.mu.Lock()
	defer mc.mu.Unlock()
	mc.performanceMetrics = metrics
}

func (mc *MetricsCollector) GetHandoffMetrics() *HandoffMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	if mc.handoffMetrics == nil {
		return nil
	}

	metrics := *mc.handoffMetrics
	metrics.StateTransitions = make([]StateTransition, len(mc.handoffMetrics.StateTransitions))
	copy(metrics.StateTransitions, mc.handoffMetrics.StateTransitions)

	return &metrics
}

func (mc *MetricsCollector) GetPerformanceMetrics() PerformanceMetrics {
	mc.mu.RLock()
	defer mc.mu.RUnlock()
	return mc.performanceMetrics
}

func (mc *MetricsCollector) GetStateTransitions() []StateTransition {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	transitions := make([]StateTransition, len(mc.stateTransitions))
	copy(transitions, mc.stateTransitions)
	return transitions
}

func (mc *MetricsCollector) CalculateHandoffLatency() time.Duration {
	metrics := mc.GetHandoffMetrics()
	if metrics == nil {
		return 0
	}
	return metrics.Duration
}

func (mc *MetricsCollector) CalculateWorkflowThroughput(workflowCount int, duration time.Duration) float64 {
	if duration.Seconds() == 0 {
		return 0
	}
	return float64(workflowCount) / duration.Seconds()
}

func (mc *MetricsCollector) GenerateReport() map[string]interface{} {
	mc.mu.RLock()
	defer mc.mu.RUnlock()

	report := make(map[string]interface{})

	if mc.handoffMetrics != nil {
		report["handoff"] = map[string]interface{}{
			"duration_ms":            mc.handoffMetrics.Duration.Milliseconds(),
			"provisional_node":       mc.handoffMetrics.ProvisionalNodeID,
			"senior_node":            mc.handoffMetrics.SeniorNodeID,
			"state_transitions":      len(mc.handoffMetrics.StateTransitions),
			"workflow_interruptions": mc.handoffMetrics.WorkflowInterruptions,
			"peer_discovery_ms":      mc.handoffMetrics.PeerDiscoveryTime.Milliseconds(),
			"demotion_ms":            mc.handoffMetrics.DemotionTime.Milliseconds(),
			"join_ms":                mc.handoffMetrics.JoinTime.Milliseconds(),
			"readiness_ms":           mc.handoffMetrics.ReadinessTime.Milliseconds(),
		}
	}

	report["performance"] = map[string]interface{}{
		"handoff_latency_ms":         mc.performanceMetrics.HandoffLatency.Milliseconds(),
		"workflow_throughput":        mc.performanceMetrics.WorkflowThroughput,
		"cluster_formation_ms":       mc.performanceMetrics.ClusterFormationTime.Milliseconds(),
		"metadata_propagation_ms":    mc.performanceMetrics.MetadataPropagationTime.Milliseconds(),
		"readiness_response_time_ms": mc.performanceMetrics.ReadinessResponseTime.Milliseconds(),
	}

	return report
}
