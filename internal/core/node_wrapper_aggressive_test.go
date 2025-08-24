package core

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/eleven-am/graft/internal/ports"
)

type NoGetNameNode struct{}

func (n *NoGetNameNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	return nil, nil
}

type GetNameWithArgsNode struct{}

func (n *GetNameWithArgsNode) GetName(arg string) string { return "invalid" }
func (n *GetNameWithArgsNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	return nil, nil
}

type GetNameMultiReturnNode struct{}

func (n *GetNameMultiReturnNode) GetName() (string, error) { return "invalid", nil }
func (n *GetNameMultiReturnNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	return nil, nil
}

type NoExecuteNode struct{}

func (n *NoExecuteNode) GetName() string { return "no-execute" }

type ExecuteTooManyArgsNode struct{}

func (n *ExecuteTooManyArgsNode) GetName() string { return "too-many-args" }
func (n *ExecuteTooManyArgsNode) Execute(ctx context.Context, a, b, c, d, e interface{}) (*ports.NodeResult, error) {
	return nil, nil
}

type ExecuteWrongReturnNode struct{}

func (n *ExecuteWrongReturnNode) GetName() string { return "wrong-return" }
func (n *ExecuteWrongReturnNode) Execute(ctx context.Context) string {
	return "invalid"
}

type ExecuteOneReturnNode struct{}

func (n *ExecuteOneReturnNode) GetName() string { return "one-return" }
func (n *ExecuteOneReturnNode) Execute(ctx context.Context) error {
	return nil
}

type CanStartTooManyArgsNode struct{}

func (n *CanStartTooManyArgsNode) GetName() string { return "canstart-too-many" }
func (n *CanStartTooManyArgsNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	return nil, nil
}
func (n *CanStartTooManyArgsNode) CanStart(ctx context.Context, a, b, c, d, e interface{}) bool {
	return true
}

type CanStartWrongReturnNode struct{}

func (n *CanStartWrongReturnNode) GetName() string { return "canstart-wrong-return" }
func (n *CanStartWrongReturnNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	return nil, nil
}
func (n *CanStartWrongReturnNode) CanStart(ctx context.Context) (bool, error) {
	return true, nil
}

type PanicNode struct{}

func (n *PanicNode) GetName() string { return "panic-node" }
func (n *PanicNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	panic("intentional panic")
}

type NilReturnNode struct{}

func (n *NilReturnNode) GetName() string { return "nil-return" }
func (n *NilReturnNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	return nil, errors.New("nil result with error")
}

type WrongResultTypeNode struct{}

func (n *WrongResultTypeNode) GetName() string { return "wrong-result-type" }
func (n *WrongResultTypeNode) Execute(ctx context.Context) (string, error) {
	return "not a NodeResult", nil
}

type ComplexTypesNode struct {
	data map[string]interface{}
}

func (n *ComplexTypesNode) GetName() string { return "complex-types" }
func (n *ComplexTypesNode) Execute(ctx context.Context, state ComplexState, config ComplexConfig) (*ports.NodeResult, error) {
	return &ports.NodeResult{
		GlobalState: ComplexState{
			Nested: NestedStruct{
				Channels: []chan int{make(chan int)}, // Unconvertible type
				Funcs:    []func(){func() {}},        // Unconvertible type
			},
		},
	}, nil
}

type ComplexState struct {
	Nested   NestedStruct                `json:"nested"`
	Private  int                         `json:"-"`
	Circular *ComplexState               `json:"circular,omitempty"`
	Map      map[interface{}]interface{} `json:"map"`
}

type NestedStruct struct {
	Channels []chan int `json:"channels"`
	Funcs    []func()   `json:"funcs"`
}

type ComplexConfig struct {
	Pointer *ComplexConfig `json:"pointer,omitempty"`
	Time    time.Time      `json:"time"`
}

type DeepRecursionNode struct{}

func (n *DeepRecursionNode) GetName() string { return "deep-recursion" }
func (n *DeepRecursionNode) Execute(ctx context.Context, state RecursiveState, config interface{}) (*ports.NodeResult, error) {
	return &ports.NodeResult{GlobalState: state}, nil
}

type RecursiveState struct {
	Level int             `json:"level"`
	Next  *RecursiveState `json:"next,omitempty"`
}

type EmptyNameNode struct{}

func (n *EmptyNameNode) GetName() string { return "" }
func (n *EmptyNameNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	return nil, nil
}

type VeryLongNameNode struct{}

func (n *VeryLongNameNode) GetName() string {
	return string(make([]byte, 10000)) // 10KB name
}
func (n *VeryLongNameNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	return nil, nil
}

type UnicodeNameNode struct{}

func (n *UnicodeNameNode) GetName() string { return "🚀💥🔥💀👾🌟⚡️🎯" }
func (n *UnicodeNameNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	return nil, nil
}

type StressTestNode struct {
	counter int
}

func (n *StressTestNode) GetName() string { return "stress-test" }
func (n *StressTestNode) Execute(ctx context.Context, state map[string]interface{}, config []interface{}) (*ports.NodeResult, error) {
	n.counter++
	if n.counter > 1000 {
		return nil, errors.New("stress limit exceeded")
	}

	largeData := make(map[string]interface{})
	for i := 0; i < 1000; i++ {
		largeData[string(rune(i))] = make([]byte, 1000)
	}

	return &ports.NodeResult{
		GlobalState: largeData,
		NextNodes: []ports.NextNode{
			{NodeName: "next1", Config: largeData},
			{NodeName: "next2", Config: largeData},
		},
	}, nil
}
func (n *StressTestNode) CanStart(ctx context.Context, state map[string]interface{}, config []interface{}) bool {
	return len(state) < 100
}

func TestNodeWrapper_MalformedNodes(t *testing.T) {
	tests := []struct {
		name        string
		node        interface{}
		expectError bool
	}{
		{"no GetName method", &NoGetNameNode{}, true},
		{"GetName with args", &GetNameWithArgsNode{}, true},
		{"GetName multiple returns", &GetNameMultiReturnNode{}, true},
		{"no Execute method", &NoExecuteNode{}, true},
		{"Execute too many args", &ExecuteTooManyArgsNode{}, true},
		{"Execute wrong return", &ExecuteWrongReturnNode{}, true},
		{"Execute one return", &ExecuteOneReturnNode{}, true},
		{"CanStart too many args", &CanStartTooManyArgsNode{}, true},
		{"CanStart wrong return", &CanStartWrongReturnNode{}, true},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapper, err := NewNodeWrapper(tt.node)
			if tt.expectError {
				if err == nil {
					t.Errorf("expected error for malformed node %s, got nil", tt.name)
				}
				if wrapper != nil {
					t.Errorf("expected nil wrapper for malformed node %s, got %v", tt.name, wrapper)
				}
			} else {
				if err != nil {
					t.Errorf("unexpected error for node %s: %v", tt.name, err)
				}
			}
		})
	}
}

func TestNodeWrapper_RuntimeFailures(t *testing.T) {
	ctx := context.Background()

	t.Run("panic during execution", func(t *testing.T) {
		defer func() {
			if r := recover(); r == nil {
				t.Error("expected panic from PanicNode execution")
			}
		}()

		wrapper, err := NewNodeWrapper(&PanicNode{})
		if err != nil {
			t.Fatalf("unexpected error creating wrapper: %v", err)
		}

		_, _ = wrapper.Execute(ctx)
	})

	t.Run("wrong result type", func(t *testing.T) {
		wrapper, err := NewNodeWrapper(&WrongResultTypeNode{})
		if err != nil {
			t.Fatalf("unexpected error creating wrapper: %v", err)
		}

		_, err = wrapper.Execute(ctx)
		if err == nil {
			t.Error("expected error from wrong result type, got nil")
		}
	})
}

func TestNodeWrapper_ComplexTypeConversions(t *testing.T) {
	ctx := context.Background()

	wrapper, err := NewNodeWrapper(&ComplexTypesNode{})
	if err != nil {
		t.Fatalf("unexpected error creating wrapper: %v", err)
	}

	state := &ComplexState{}
	state.Circular = state
	state.Map = map[interface{}]interface{}{
		"key": make(chan int),
		1:     func() {},
	}

	config := &ComplexConfig{
		Time: time.Now(),
	}
	config.Pointer = config

	_, err = wrapper.Execute(ctx, state, config)
	t.Logf("Complex type conversion result: %v", err)
}

func TestNodeWrapper_DeepRecursion(t *testing.T) {
	ctx := context.Background()

	wrapper, err := NewNodeWrapper(&DeepRecursionNode{})
	if err != nil {
		t.Fatalf("unexpected error creating wrapper: %v", err)
	}

	var state *RecursiveState
	current := &RecursiveState{Level: 0}
	state = current

	for i := 1; i < 10000; i++ { // Very deep nesting
		current.Next = &RecursiveState{Level: i}
		current = current.Next
	}

	_, err = wrapper.Execute(ctx, state, nil)
	t.Logf("Deep recursion result: %v", err)
}

func TestNodeWrapper_EdgeCases(t *testing.T) {
	tests := []struct {
		name string
		node interface{}
	}{
		{"empty name", &EmptyNameNode{}},
		{"very long name", &VeryLongNameNode{}},
		{"unicode name", &UnicodeNameNode{}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			wrapper, err := NewNodeWrapper(tt.node)
			if err != nil {
				t.Errorf("unexpected error for %s: %v", tt.name, err)
				return
			}

			name := wrapper.GetName()
			t.Logf("Node name length: %d, content: %q", len(name), name[:min(len(name), 100)])
		})
	}
}

func TestNodeWrapper_StressTest(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping stress test in short mode")
	}

	ctx := context.Background()
	node := &StressTestNode{}
	wrapper, err := NewNodeWrapper(node)
	if err != nil {
		t.Fatalf("unexpected error creating wrapper: %v", err)
	}

	for i := 0; i < 100; i++ {
		state := make(map[string]interface{})
		for j := 0; j < i; j++ {
			state[string(rune(j))] = make([]byte, 100)
		}

		config := make([]interface{}, i%10)

		canStart := wrapper.CanStart(ctx, state, config)
		t.Logf("Iteration %d: CanStart=%v", i, canStart)

		if canStart {
			result, err := wrapper.Execute(ctx, state, config)
			if err != nil && i < 900 {
				t.Errorf("unexpected error at iteration %d: %v", i, err)
			}
			if result != nil {
				t.Logf("Result has %d next nodes", len(result.NextNodes))
			}
		}
	}
}

func TestNodeWrapper_NilAndInvalidInputs(t *testing.T) {

	_, err := NewNodeWrapper(nil)
	if err == nil {
		t.Error("expected error for nil node")
	}

	_, err = NewNodeWrapper("not a struct")
	if err == nil {
		t.Error("expected error for non-struct node")
	}

	wrapper, err := NewNodeWrapper(&UnicodeNameNode{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Logf("Execute with nil context panicked as expected: %v", r)
		}
	}()

	_, err = wrapper.Execute(nil)
	t.Logf("Execute with nil context: %v", err)
}

func TestNodeWrapper_TypeInference(t *testing.T) {
	ctx := context.Background()
	_ = ctx // Use ctx to avoid unused variable error

	wrapper, err := NewNodeWrapper(&ComplexTypesNode{})
	if err != nil {
		t.Fatalf("unexpected error: %v", err)
	}

	t.Logf("Config type: %v", wrapper.configType)
	t.Logf("State type: %v", wrapper.stateType)
	t.Logf("Result type: %v", wrapper.resultType)

	if wrapper.configType == nil {
		t.Error("config type is nil")
	}
	if wrapper.stateType == nil {
		t.Error("state type is nil")
	}
	if wrapper.resultType == nil {
		t.Error("result type is nil")
	}
}

func min(a, b int) int {
	if a < b {
		return a
	}
	return b
}
