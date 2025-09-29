package harness

import (
	"context"
	"fmt"
	"time"
)

type HandoffAssertion struct {
	NodeID               string
	ExpectedState        string
	ExpectedProvisional  bool
	MaxTransitionTime    time.Duration
	ShouldHaveWorkflows  bool
	WorkflowsShouldPause bool
}

type AssertionResult struct {
	Success    bool
	Message    string
	ActualData map[string]interface{}
	Timestamp  time.Time
}

func AssertHandoffBehavior(ctx context.Context, instance *NodeInstance, assertion HandoffAssertion) AssertionResult {
	result := AssertionResult{
		Timestamp:  time.Now(),
		ActualData: make(map[string]interface{}),
	}

	health := instance.Manager.GetHealth()
	clusterInfo := instance.Manager.GetClusterInfo()

	result.ActualData["readiness_state"] = instance.Manager.GetReadinessState()
	result.ActualData["is_ready"] = instance.Manager.IsReady()
	result.ActualData["is_leader"] = clusterInfo.IsLeader
	result.ActualData["peer_count"] = len(clusterInfo.Peers)

	if bootstrapData, ok := health.Details["bootstrap"].(map[string]interface{}); ok {
		if provisional, exists := bootstrapData["provisional"]; exists {
			result.ActualData["is_provisional"] = provisional
		}
	}

	if assertion.ExpectedState != "" {
		actualState := instance.Manager.GetReadinessState()
		if actualState != assertion.ExpectedState {
			result.Success = false
			result.Message = fmt.Sprintf("expected readiness state '%s', got '%s'",
				assertion.ExpectedState, actualState)
			return result
		}
	}

	if assertion.ExpectedProvisional {
		if provisional, ok := result.ActualData["is_provisional"].(bool); !ok || !provisional {
			result.Success = false
			result.Message = fmt.Sprintf("expected node to be provisional, got %v",
				result.ActualData["is_provisional"])
			return result
		}
	}

	result.Success = true
	result.Message = "assertion passed"
	return result
}

func AssertMetadataPropagation(ctx context.Context, instances []*NodeInstance) AssertionResult {
	result := AssertionResult{
		Timestamp:  time.Now(),
		ActualData: make(map[string]interface{}),
	}

	bootIDs := make(map[string]string)
	timestamps := make(map[string]int64)

	for _, instance := range instances {
		health := instance.Manager.GetHealth()

		if bootstrapData, ok := health.Details["bootstrap"].(map[string]interface{}); ok {
			if bootID, exists := bootstrapData["boot_id"].(string); exists {
				bootIDs[instance.Config.NodeID] = bootID
			}
			if timestamp, exists := bootstrapData["timestamp"].(int64); exists {
				timestamps[instance.Config.NodeID] = timestamp
			}
		}
	}

	result.ActualData["boot_ids"] = bootIDs
	result.ActualData["timestamps"] = timestamps

	for nodeID, bootID := range bootIDs {
		if bootID == "" {
			result.Success = false
			result.Message = fmt.Sprintf("node %s missing boot ID", nodeID)
			return result
		}
	}

	uniqueBootIDs := make(map[string]bool)
	for _, bootID := range bootIDs {
		if uniqueBootIDs[bootID] {
			result.Success = false
			result.Message = "duplicate boot IDs detected"
			return result
		}
		uniqueBootIDs[bootID] = true
	}

	result.Success = true
	result.Message = "metadata propagation verified"
	return result
}

func AssertDemotionSequence(ctx context.Context, provisionalNode, seniorNode *NodeInstance, timeout time.Duration) AssertionResult {
	result := AssertionResult{
		Timestamp:  time.Now(),
		ActualData: make(map[string]interface{}),
	}

	startTime := time.Now()

	demotionCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()

	initialState := provisionalNode.Manager.GetReadinessState()
	result.ActualData["initial_state"] = initialState

	stateTransitions := []string{initialState}

	for {
		select {
		case <-demotionCtx.Done():
			result.Success = false
			result.Message = fmt.Sprintf("demotion sequence timeout after %v", time.Since(startTime))
			result.ActualData["state_transitions"] = stateTransitions
			return result

		case <-ticker.C:
			currentState := provisionalNode.Manager.GetReadinessState()

			if len(stateTransitions) == 0 || stateTransitions[len(stateTransitions)-1] != currentState {
				stateTransitions = append(stateTransitions, currentState)
			}

			if currentState == "ready" && !provisionalNode.Manager.IsReady() {
				continue
			}

			if currentState == "ready" && provisionalNode.Manager.IsReady() {
				clusterInfo := provisionalNode.Manager.GetClusterInfo()
				if len(clusterInfo.Peers) > 0 {
					result.Success = true
					result.Message = fmt.Sprintf("demotion sequence completed in %v", time.Since(startTime))
					result.ActualData["state_transitions"] = stateTransitions
					result.ActualData["final_peer_count"] = len(clusterInfo.Peers)
					result.ActualData["duration_ms"] = time.Since(startTime).Milliseconds()
					return result
				}
			}
		}
	}
}

func WaitForClusterFormation(ctx context.Context, instances []*NodeInstance, expectedPeers int, timeout time.Duration) AssertionResult {
	result := AssertionResult{
		Timestamp:  time.Now(),
		ActualData: make(map[string]interface{}),
	}

	formationCtx, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()

	ticker := time.NewTicker(500 * time.Millisecond)
	defer ticker.Stop()

	startTime := time.Now()

	for {
		select {
		case <-formationCtx.Done():
			result.Success = false
			result.Message = fmt.Sprintf("cluster formation timeout after %v", time.Since(startTime))
			return result

		case <-ticker.C:
			allFormed := true
			peerCounts := make(map[string]int)

			for _, instance := range instances {
				if !instance.Manager.IsReady() {
					allFormed = false
					continue
				}

				clusterInfo := instance.Manager.GetClusterInfo()
				peerCount := len(clusterInfo.Peers)
				peerCounts[instance.Config.NodeID] = peerCount

				if peerCount < expectedPeers {
					allFormed = false
				}
			}

			result.ActualData["peer_counts"] = peerCounts

			if allFormed {
				result.Success = true
				result.Message = fmt.Sprintf("cluster formation completed in %v", time.Since(startTime))
				result.ActualData["formation_time_ms"] = time.Since(startTime).Milliseconds()
				return result
			}
		}
	}
}
