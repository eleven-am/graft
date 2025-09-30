package main

import (
	"context"
	"fmt"
	"time"

	"github.com/eleven-am/graft"
)

type TestWorkflowNode struct{}

func (n *TestWorkflowNode) Name() string {
	return "test_node"
}

func (n *TestWorkflowNode) Execute(ctx context.Context, state interface{}, config interface{}) (graft.NodeResult, error) {
	configMap, ok := config.(map[string]interface{})
	if !ok {
		return graft.NodeResult{}, fmt.Errorf("invalid config type")
	}

	processor, _ := configMap["processor"].(string)
	duration, _ := configMap["duration"].(float64)

	if duration > 0 {
		time.Sleep(time.Duration(duration) * time.Millisecond)
	}

	stateMap, ok := state.(map[string]interface{})
	if !ok {
		stateMap = make(map[string]interface{})
	}

	result := map[string]interface{}{
		"processed_by": processor,
		"processed_at": time.Now().Unix(),
		"status":       "completed",
		"test_result":  "success",
	}

	for k, v := range stateMap {
		result[k] = v
	}

	return graft.NodeResult{
		GlobalState: result,
		NextNodes:   nil,
	}, nil
}

func init() {
}
