package node_registry

import (
	"context"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
	json "github.com/goccy/go-json"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type MinimalNode struct {
	name string
}

func (n *MinimalNode) GetName() string {
	return n.name
}

func (n *MinimalNode) Execute() (*TestResult, error) {
	return &TestResult{
		GlobalState: map[string]interface{}{"executed": true},
	}, nil
}

type TestResult struct {
	GlobalState interface{} `json:"global_state"`
	NextNodes   []TestNext  `json:"next_nodes"`
}

type TestNext struct {
	NodeName string      `json:"node_name"`
	Config   interface{} `json:"config"`
}

type NodeWithCanStart struct {
	name     string
	canStart bool
}

func (n *NodeWithCanStart) GetName() string {
	return n.name
}

func (n *NodeWithCanStart) CanStart() bool {
	return n.canStart
}

func (n *NodeWithCanStart) Execute() (*TestResult, error) {
	return &TestResult{
		GlobalState: map[string]interface{}{"executed": true},
	}, nil
}

type NodeWithContext struct {
	name string
}

func (n *NodeWithContext) GetName() string {
	return n.name
}

func (n *NodeWithContext) Execute(ctx context.Context) (*TestResult, error) {
	select {
	case <-ctx.Done():
		return nil, ctx.Err()
	default:
		return &TestResult{
			GlobalState: map[string]interface{}{"with_context": true},
		}, nil
	}
}

type NodeWithState struct {
	name string
}

func (n *NodeWithState) GetName() string {
	return n.name
}

func (n *NodeWithState) Execute(state map[string]interface{}) (*TestResult, error) {
	if state == nil {
		state = make(map[string]interface{})
	}
	state["processed"] = true
	return &TestResult{
		GlobalState: state,
	}, nil
}

type NodeWithContextAndState struct {
	name string
}

func (n *NodeWithContextAndState) GetName() string {
	return n.name
}

func (n *NodeWithContextAndState) Execute(ctx context.Context, state map[string]interface{}) (*TestResult, error) {
	if state == nil {
		state = make(map[string]interface{})
	}
	state["processed"] = true
	return &TestResult{
		GlobalState: state,
	}, nil
}

func (n *NodeWithContextAndState) CanStart(ctx context.Context, state map[string]interface{}) bool {
	return state != nil && state["ready"] == true
}

type NodeWithAllParams struct {
	name string
}

func (n *NodeWithAllParams) GetName() string {
	return n.name
}

func (n *NodeWithAllParams) Execute(ctx context.Context, state map[string]interface{}, config map[string]interface{}) (*TestResult, error) {
	if state == nil {
		state = make(map[string]interface{})
	}
	if config != nil {
		state["config_value"] = config["value"]
	}
	return &TestResult{
		GlobalState: state,
		NextNodes: []TestNext{
			{NodeName: "next-node", Config: config},
		},
	}, nil
}

func (n *NodeWithAllParams) CanStart(ctx context.Context, state map[string]interface{}, config map[string]interface{}) bool {
	return config != nil && config["enabled"] == true
}

type InvalidNodeNoGetName struct{}

func (n *InvalidNodeNoGetName) Execute() (*TestResult, error) {
	return nil, nil
}

type InvalidNodeNoExecute struct{}

func (n *InvalidNodeNoExecute) GetName() string {
	return "invalid"
}

type InvalidNodeWrongReturn struct{}

func (n *InvalidNodeWrongReturn) GetName() string {
	return "invalid"
}

func (n *InvalidNodeWrongReturn) Execute() string {
	return "wrong"
}

type InvalidNodeTooManyParams struct{}

func (n *InvalidNodeTooManyParams) GetName() string {
	return "invalid"
}

func (n *InvalidNodeTooManyParams) Execute(a, b, c, d interface{}) (*TestResult, error) {
	return nil, nil
}

type CustomState struct {
	ID        string    `json:"id"`
	Count     int       `json:"count"`
	Timestamp time.Time `json:"timestamp"`
}

type CustomConfig struct {
	Enabled bool   `json:"enabled"`
	Mode    string `json:"mode"`
}

type CustomNode struct {
	name string
}

func (n *CustomNode) GetName() string {
	return n.name
}

func (n *CustomNode) Execute(ctx context.Context, state CustomState, config CustomConfig) (*TestResult, error) {
	state.Count++
	return &TestResult{
		GlobalState: state,
	}, nil
}

type NilReturningNode struct {
	name string
}

func (n *NilReturningNode) GetName() string {
	return n.name
}

func (n *NilReturningNode) Execute() (*TestResult, error) {
	return nil, nil
}

type ErrorReturningNode struct {
	name string
}

func (n *ErrorReturningNode) GetName() string {
	return n.name
}

func (n *ErrorReturningNode) Execute() (*TestResult, error) {
	return nil, assert.AnError
}

func TestNewNodeAdapter_Validation(t *testing.T) {
	tests := []struct {
		name    string
		node    interface{}
		wantErr bool
		errMsg  string
	}{
		{
			name:    "nil node",
			node:    nil,
			wantErr: true,
			errMsg:  "node cannot be nil",
		},
		{
			name:    "no GetName method",
			node:    &InvalidNodeNoGetName{},
			wantErr: true,
			errMsg:  "node must have GetName() method",
		},
		{
			name:    "no Execute method",
			node:    &InvalidNodeNoExecute{},
			wantErr: true,
			errMsg:  "node must have Execute() method",
		},
		{
			name:    "Execute wrong return type",
			node:    &InvalidNodeWrongReturn{},
			wantErr: true,
			errMsg:  "Execute() must return exactly 2 values",
		},
		{
			name:    "Execute too many params",
			node:    &InvalidNodeTooManyParams{},
			wantErr: true,
			errMsg:  "Execute() can have at most 3 parameters",
		},
		{
			name:    "valid minimal node",
			node:    &MinimalNode{name: "test"},
			wantErr: false,
		},
		{
			name:    "valid node with CanStart",
			node:    &NodeWithCanStart{name: "test", canStart: true},
			wantErr: false,
		},
		{
			name:    "valid node with context",
			node:    &NodeWithContext{name: "test"},
			wantErr: false,
		},
		{
			name:    "valid node with all params",
			node:    &NodeWithAllParams{name: "test"},
			wantErr: false,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			adapter, err := NewNodeAdapter(tt.node)
			if tt.wantErr {
				assert.Error(t, err)
				assert.Contains(t, err.Error(), tt.errMsg)
				assert.Nil(t, adapter)
			} else {
				assert.NoError(t, err)
				assert.NotNil(t, adapter)
			}
		})
	}
}

func TestNodeAdapter_GetName(t *testing.T) {
	node := &MinimalNode{name: "test-node"}
	adapter, err := NewNodeAdapter(node)
	require.NoError(t, err)

	assert.Equal(t, "test-node", adapter.GetName())
}

func TestNodeAdapter_CanStart(t *testing.T) {
	t.Run("no CanStart method returns true", func(t *testing.T) {
		node := &MinimalNode{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		result := adapter.CanStart(context.Background(), nil, nil)
		assert.True(t, result)
	})

	t.Run("CanStart returns true", func(t *testing.T) {
		node := &NodeWithCanStart{name: "test", canStart: true}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		result := adapter.CanStart(context.Background(), nil, nil)
		assert.True(t, result)
	})

	t.Run("CanStart returns false", func(t *testing.T) {
		node := &NodeWithCanStart{name: "test", canStart: false}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		result := adapter.CanStart(context.Background(), nil, nil)
		assert.False(t, result)
	})

	t.Run("CanStart with state check", func(t *testing.T) {
		node := &NodeWithContextAndState{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		state := map[string]interface{}{"ready": true}
		stateBytes, _ := json.Marshal(state)

		result := adapter.CanStart(context.Background(), stateBytes, nil)
		assert.True(t, result)

		state["ready"] = false
		stateBytes, _ = json.Marshal(state)

		result = adapter.CanStart(context.Background(), stateBytes, nil)
		assert.False(t, result)
	})

	t.Run("CanStart with config check", func(t *testing.T) {
		node := &NodeWithAllParams{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		config := map[string]interface{}{"enabled": true}
		configBytes, _ := json.Marshal(config)

		result := adapter.CanStart(context.Background(), nil, configBytes)
		assert.True(t, result)

		config["enabled"] = false
		configBytes, _ = json.Marshal(config)

		result = adapter.CanStart(context.Background(), nil, configBytes)
		assert.False(t, result)
	})
}

func TestNodeAdapter_Execute(t *testing.T) {
	t.Run("minimal node execute", func(t *testing.T) {
		node := &MinimalNode{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		result, err := adapter.Execute(context.Background(), nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		state, ok := result.GlobalState.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, state["executed"])
	})

	t.Run("node with context", func(t *testing.T) {
		node := &NodeWithContext{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		result, err := adapter.Execute(context.Background(), nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		state, ok := result.GlobalState.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, state["with_context"])
	})

	t.Run("node with context cancelled", func(t *testing.T) {
		node := &NodeWithContext{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		ctx, cancel := context.WithCancel(context.Background())
		cancel()

		_, err = adapter.Execute(ctx, nil, nil)
		assert.Error(t, err)
		assert.Equal(t, context.Canceled, err)
	})

	t.Run("node with state processing", func(t *testing.T) {
		node := &NodeWithState{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		initialState := map[string]interface{}{"initial": true}
		stateBytes, _ := json.Marshal(initialState)

		result, err := adapter.Execute(context.Background(), stateBytes, nil)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		state, ok := result.GlobalState.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, true, state["initial"])
		assert.Equal(t, true, state["processed"])
	})

	t.Run("node with all params and next nodes", func(t *testing.T) {
		node := &NodeWithAllParams{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		state := map[string]interface{}{"initial": true}
		stateBytes, _ := json.Marshal(state)

		config := map[string]interface{}{"value": "test-value"}
		configBytes, _ := json.Marshal(config)

		result, err := adapter.Execute(context.Background(), stateBytes, configBytes)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		resultState, ok := result.GlobalState.(map[string]interface{})
		assert.True(t, ok)
		assert.Equal(t, "test-value", resultState["config_value"])

		assert.Len(t, result.NextNodes, 1)
		assert.Equal(t, "next-node", result.NextNodes[0].NodeName)
	})

	t.Run("node returning nil result", func(t *testing.T) {
		node := &NilReturningNode{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		result, err := adapter.Execute(context.Background(), nil, nil)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.Nil(t, result.GlobalState)
		assert.Nil(t, result.NextNodes)
	})

	t.Run("node returning error", func(t *testing.T) {
		node := &ErrorReturningNode{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		_, err = adapter.Execute(context.Background(), nil, nil)
		assert.Error(t, err)
		assert.Equal(t, assert.AnError, err)
	})
}

func TestNodeAdapter_TypeConversion(t *testing.T) {
	t.Run("handles empty json", func(t *testing.T) {
		node := &NodeWithState{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		result, err := adapter.Execute(context.Background(), []byte{}, nil)
		assert.NoError(t, err)
		assert.NotNil(t, result)
	})

	t.Run("returns error on malformed json", func(t *testing.T) {
		node := &NodeWithState{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		malformedJSON := json.RawMessage(`{invalid json`)

		result, err := adapter.Execute(context.Background(), malformedJSON, nil)
		assert.Error(t, err)
		assert.Nil(t, result)
	})

	t.Run("complex type conversion", func(t *testing.T) {

		node := &CustomNode{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		now := time.Now()
		state := CustomState{
			ID:        "test-id",
			Count:     5,
			Timestamp: now,
		}
		stateBytes, _ := json.Marshal(state)

		config := CustomConfig{
			Enabled: true,
			Mode:    "test",
		}
		configBytes, _ := json.Marshal(config)

		result, err := adapter.Execute(context.Background(), stateBytes, configBytes)
		assert.NoError(t, err)
		assert.NotNil(t, result)

		resultBytes, err := json.Marshal(result.GlobalState)
		assert.NoError(t, err)

		var resultState CustomState
		err = json.Unmarshal(resultBytes, &resultState)
		assert.NoError(t, err)
		assert.Equal(t, "test-id", resultState.ID)
		assert.Equal(t, 6, resultState.Count)
	})
}

func TestNodeAdapter_Integration(t *testing.T) {
	t.Run("adapter implements ports.NodePort", func(t *testing.T) {
		node := &MinimalNode{name: "test"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		var _ ports.NodePort = adapter
	})

	t.Run("full workflow simulation", func(t *testing.T) {
		node := &NodeWithAllParams{name: "workflow-node"}
		adapter, err := NewNodeAdapter(node)
		require.NoError(t, err)

		ctx := context.Background()
		state := json.RawMessage(`{"step": 1}`)
		config := json.RawMessage(`{"enabled": true, "value": "test"}`)

		canStart := adapter.CanStart(ctx, state, config)
		assert.True(t, canStart)

		result, err := adapter.Execute(ctx, state, config)
		assert.NoError(t, err)
		assert.NotNil(t, result)
		assert.NotNil(t, result.GlobalState)
		assert.Len(t, result.NextNodes, 1)

		config = json.RawMessage(`{"enabled": false}`)
		canStart = adapter.CanStart(ctx, state, config)
		assert.False(t, canStart)
	})
}
