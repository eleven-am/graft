package core

import (
	"context"
	"fmt"
	"testing"

	"github.com/eleven-am/graft/internal/ports"
)

type SimpleNode struct{}

func (n *SimpleNode) GetName() string { return "simple" }
func (n *SimpleNode) Execute(ctx context.Context) (*ports.NodeResult, error) {
	return &ports.NodeResult{}, nil
}

type TypedNode struct{}

func (n *TypedNode) GetName() string { return "typed" }
func (n *TypedNode) Execute(ctx context.Context, state string, config int) (*ports.NodeResult, error) {
	return &ports.NodeResult{}, nil
}

func TestBug_EmptyArgsArrayPanic(t *testing.T) {
	ctx := context.Background()

	wrapper, err := NewNodeWrapper(&SimpleNode{})
	if err != nil {
		t.Fatalf("failed to create wrapper: %v", err)
	}

	defer func() {
		if r := recover(); r != nil {
			t.Errorf("Execute panicked with empty args: %v", r)
		}
	}()

	_, err = wrapper.Execute(ctx)
	t.Logf("Execute with no args result: %v", err)

	_, err = wrapper.Execute(ctx, "state")
	t.Logf("Execute with one arg result: %v", err)
}

func TestBug_NilArgsHandling(t *testing.T) {
	ctx := context.Background()

	wrapper, err := NewNodeWrapper(&SimpleNode{})
	if err != nil {
		t.Fatalf("failed to create wrapper: %v", err)
	}

	tests := [][]interface{}{
		{nil, nil},
		{"state", nil},
		{nil, "config"},
		{map[string]interface{}{}, nil},
	}

	for i, args := range tests {
		t.Run(fmt.Sprintf("args_%d", i), func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Errorf("Execute panicked with args %v: %v", args, r)
				}
			}()

			_, err := wrapper.Execute(ctx, args...)
			t.Logf("Result with args %v: %v", args, err)
		})
	}
}

func TestBug_TypeConversionEdgeCases(t *testing.T) {
	ctx := context.Background()

	wrapper, err := NewNodeWrapper(&TypedNode{})
	if err != nil {
		t.Fatalf("failed to create wrapper: %v", err)
	}

	problematicInputs := []struct {
		name   string
		state  interface{}
		config interface{}
	}{
		{"incompatible_types", 123, "not_a_number"},
		{"nil_values", nil, nil},
		{"wrong_types", []string{"array"}, map[string]interface{}{"not": "int"}},
		{"complex_objects", make(chan int), func() {}},
		{"circular_reference", createCircularRef(), createCircularRef()},
	}

	for _, test := range problematicInputs {
		t.Run(test.name, func(t *testing.T) {
			defer func() {
				if r := recover(); r != nil {
					t.Logf("Expected panic for %s: %v", test.name, r)
				}
			}()

			_, err := wrapper.Execute(ctx, test.state, test.config)
			t.Logf("Type conversion test %s result: %v", test.name, err)
		})
	}
}

func createCircularRef() map[string]interface{} {
	m := make(map[string]interface{})
	m["self"] = m
	return m
}
