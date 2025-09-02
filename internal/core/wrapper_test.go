package core

import (
	"context"
	"errors"
	"testing"
)

type TestParams struct {
	Service string `json:"service"`
	Version string `json:"version"`
	Count   int    `json:"count"`
}

type InvalidParams struct {
	Channel chan int `json:"channel"`
}

func TestWrapHandler_Success(t *testing.T) {
	var receivedParams TestParams
	var receivedFrom string
	var receivedCtx context.Context

	handler := func(ctx context.Context, from string, params TestParams) error {
		receivedCtx = ctx
		receivedFrom = from
		receivedParams = params
		return nil
	}

	wrappedHandler := WrapHandler(handler)

	ctx := context.Background()
	fromNode := "node-123"
	inputParams := map[string]interface{}{
		"service": "api",
		"version": "1.2.3",
		"count":   5,
	}

	err := wrappedHandler(ctx, fromNode, inputParams)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}

	if receivedCtx != ctx {
		t.Error("Context not passed correctly")
	}

	if receivedFrom != fromNode {
		t.Errorf("Expected from=%s, got=%s", fromNode, receivedFrom)
	}

	expected := TestParams{
		Service: "api",
		Version: "1.2.3",
		Count:   5,
	}

	if receivedParams != expected {
		t.Errorf("Expected params=%+v, got=%+v", expected, receivedParams)
	}
}

func TestWrapHandler_HandlerError(t *testing.T) {
	expectedError := errors.New("handler failed")

	handler := func(ctx context.Context, from string, params TestParams) error {
		return expectedError
	}

	wrappedHandler := WrapHandler(handler)

	ctx := context.Background()
	inputParams := map[string]interface{}{
		"service": "api",
		"version": "1.2.3",
		"count":   5,
	}

	err := wrappedHandler(ctx, "node-123", inputParams)

	if err != expectedError {
		t.Errorf("Expected error=%v, got=%v", expectedError, err)
	}
}

func TestWrapHandler_InvalidJSON(t *testing.T) {
	handler := func(ctx context.Context, from string, params TestParams) error {
		return nil
	}

	wrappedHandler := WrapHandler(handler)

	ctx := context.Background()
	invalidParams := InvalidParams{
		Channel: make(chan int),
	}

	err := wrappedHandler(ctx, "node-123", invalidParams)

	if err == nil {
		t.Fatal("Expected marshal error for channel type")
	}

	if !containsString(err.Error(), "failed to marshal params") {
		t.Errorf("Expected marshal error, got: %v", err)
	}
}

func TestWrapHandler_UnmarshalError(t *testing.T) {
	handler := func(ctx context.Context, from string, params TestParams) error {
		return nil
	}

	wrappedHandler := WrapHandler(handler)

	ctx := context.Background()
	invalidParams := map[string]interface{}{
		"service": "api",
		"version": "1.2.3",
		"count":   "not-a-number",
	}

	err := wrappedHandler(ctx, "node-123", invalidParams)

	if err == nil {
		t.Fatal("Expected unmarshal error")
	}

	if !containsString(err.Error(), "failed to unmarshal params") {
		t.Errorf("Expected unmarshal error, got: %v", err)
	}
}

func TestWrapHandler_StructToStruct(t *testing.T) {
	handler := func(ctx context.Context, from string, params TestParams) error {
		if params.Service != "direct" {
			t.Errorf("Expected service=direct, got=%s", params.Service)
		}
		return nil
	}

	wrappedHandler := WrapHandler(handler)

	ctx := context.Background()
	inputParams := TestParams{
		Service: "direct",
		Version: "2.0.0",
		Count:   3,
	}

	err := wrappedHandler(ctx, "node-456", inputParams)

	if err != nil {
		t.Fatalf("Expected no error, got: %v", err)
	}
}

func TestWrapHandler_EmptyParams(t *testing.T) {
	type EmptyParams struct{}

	handler := func(ctx context.Context, from string, params EmptyParams) error {
		return nil
	}

	wrappedHandler := WrapHandler(handler)

	ctx := context.Background()
	inputParams := map[string]interface{}{}

	err := wrappedHandler(ctx, "node-789", inputParams)

	if err != nil {
		t.Fatalf("Expected no error for empty params, got: %v", err)
	}
}

func containsString(str, substr string) bool {
	return len(str) >= len(substr) &&
		len(substr) > 0 &&
		str[len(str)-len(substr):] == substr ||
		len(str) > len(substr) &&
			str[:len(substr)] == substr ||
		len(str) > len(substr) &&
			findInString(str, substr)
}

func findInString(str, substr string) bool {
	for i := 0; i <= len(str)-len(substr); i++ {
		if str[i:i+len(substr)] == substr {
			return true
		}
	}
	return false
}
