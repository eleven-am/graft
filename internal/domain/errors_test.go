package domain

import (
	"errors"
	"strings"
	"testing"
	"time"
)

func TestDomainErrorBasics(t *testing.T) {
	cause := errors.New("underlying error")
	err := NewValidationError("invalid input provided", cause)

	if err.Category != CategoryValidation {
		t.Errorf("Expected category %v, got %v", CategoryValidation, err.Category)
	}

	if err.Severity != SeverityError {
		t.Errorf("Expected severity %v, got %v", SeverityError, err.Severity)
	}

	if err.Code != "VALIDATION_INVALID" {
		t.Errorf("Expected code VALIDATION_INVALID, got %s", err.Code)
	}

	if !err.UserFacing {
		t.Error("Expected validation error to be user facing")
	}

	if err.Retryable {
		t.Error("Expected validation error to not be retryable")
	}

	if err.Unwrap() != cause {
		t.Error("Expected cause to be unwrapped correctly")
	}
}

func TestErrorWithContext(t *testing.T) {
	err := NewNetworkError("connection failed", nil).
		WithNodeID("node-123").
		WithWorkflowID("workflow-456").
		WithOperation("connect_to_peer").
		WithContext("peer_addr", "192.168.1.100")

	if err.Context.NodeID != "node-123" {
		t.Errorf("Expected node ID node-123, got %s", err.Context.NodeID)
	}

	if err.Context.WorkflowID != "workflow-456" {
		t.Errorf("Expected workflow ID workflow-456, got %s", err.Context.WorkflowID)
	}

	if err.Context.Operation != "connect_to_peer" {
		t.Errorf("Expected operation connect_to_peer, got %s", err.Context.Operation)
	}

	if err.Context.Details["peer_addr"] != "192.168.1.100" {
		t.Error("Expected peer_addr in context details")
	}
}

func TestErrorCategorization(t *testing.T) {
	testCases := []struct {
		name               string
		constructor        func(string, error, ...ErrorOption) *DomainError
		expectedCategory   ErrorCategory
		expectedRetryable  bool
		expectedUserFacing bool
	}{
		{"validation", NewValidationError, CategoryValidation, false, true},
		{"network", NewNetworkError, CategoryNetwork, true, false},
		{"storage", NewStorageError, CategoryStorage, false, false},
		{"raft", NewRaftError, CategoryRaft, true, false},
		{"workflow", NewWorkflowError, CategoryWorkflow, false, false},
		{"timeout", NewTimeoutError, CategoryTimeout, true, false},
		{"configuration", NewConfigurationError, CategoryConfiguration, false, true},
		{"permission", NewPermissionError, CategoryPermission, false, true},
		{"resource", NewResourceError, CategoryResource, true, false},
	}

	for _, tc := range testCases {
		t.Run(tc.name, func(t *testing.T) {
			err := tc.constructor("test message", nil)

			if err.Category != tc.expectedCategory {
				t.Errorf("Expected category %v, got %v", tc.expectedCategory, err.Category)
			}

			if err.Retryable != tc.expectedRetryable {
				t.Errorf("Expected retryable %v, got %v", tc.expectedRetryable, err.Retryable)
			}

			if err.UserFacing != tc.expectedUserFacing {
				t.Errorf("Expected user facing %v, got %v", tc.expectedUserFacing, err.UserFacing)
			}
		})
	}
}

func TestErrorCodeInference(t *testing.T) {
	testCases := []struct {
		category     ErrorCategory
		message      string
		expectedCode string
	}{
		{CategoryValidation, "field is required", "VALIDATION_REQUIRED"},
		{CategoryValidation, "invalid format", "VALIDATION_INVALID"},
		{CategoryNetwork, "connection timeout", "NETWORK_TIMEOUT"},
		{CategoryNetwork, "connection refused", "NETWORK_CONNECTION"},
		{CategoryStorage, "key not found", "STORAGE_NOT_FOUND"},
		{CategoryStorage, "conflict detected", "STORAGE_CONFLICT"},
		{CategoryRaft, "no leader available", "RAFT_LEADER"},
		{CategoryRaft, "election in progress", "RAFT_ELECTION"},
		{CategoryWorkflow, "execution timeout", "WORKFLOW_TIMEOUT"},
		{CategoryWorkflow, "invalid state transition", "WORKFLOW_STATE"},
		{CategoryResource, "rate limit exceeded", "RESOURCE_LIMIT"},
		{CategoryResource, "storage full", "RESOURCE_EXHAUSTED"},
	}

	for _, tc := range testCases {
		t.Run(tc.expectedCode, func(t *testing.T) {
			err := NewDomainErrorWithCategory(tc.category, tc.message, nil)
			if err.Code != tc.expectedCode {
				t.Errorf("Expected code %s, got %s", tc.expectedCode, err.Code)
			}
		})
	}
}

func TestErrorHelpers(t *testing.T) {
	err := NewValidationError("test error", nil)

	if !IsDomainError(err) {
		t.Error("Expected IsDomainError to return true")
	}

	if GetErrorCategory(err) != CategoryValidation {
		t.Error("Expected GetErrorCategory to return CategoryValidation")
	}

	if GetErrorSeverity(err) != SeverityError {
		t.Error("Expected GetErrorSeverity to return SeverityError")
	}

	if IsRetryableError(err) {
		t.Error("Expected validation error to not be retryable")
	}

	if !IsUserFacingError(err) {
		t.Error("Expected validation error to be user facing")
	}

	ctx := GetErrorContext(err)
	if ctx == nil {
		t.Error("Expected GetErrorContext to return non-nil context")
	}
}

func TestErrorIs(t *testing.T) {
	err1 := NewValidationError("invalid input", nil)
	err2 := NewValidationError("invalid format", nil)
	err3 := NewNetworkError("connection failed", nil)

	if !err1.Is(err2) {
		t.Error("Expected validation errors with same category to be equal")
	}

	if err1.Is(err3) {
		t.Error("Expected validation and network errors to not be equal")
	}
}

func TestRetryableErrorDetection(t *testing.T) {

	retryableErr := NewNetworkError("connection timeout", nil)
	if !IsRetryableError(retryableErr) {
		t.Error("Expected network error to be retryable")
	}

	nonRetryableErr := NewValidationError("invalid input", nil)
	if IsRetryableError(nonRetryableErr) {
		t.Error("Expected validation error to not be retryable")
	}

	standardTimeoutErr := errors.New("request timeout")
	if !IsRetryableError(standardTimeoutErr) {
		t.Error("Expected timeout error to be retryable")
	}

	standardValidationErr := errors.New("validation failed")
	if IsRetryableError(standardValidationErr) {
		t.Error("Expected validation error to not be retryable")
	}
}

func TestErrorTimestamp(t *testing.T) {
	before := time.Now()
	err := NewNetworkError("test", nil)
	after := time.Now()

	if err.Timestamp.Before(before) || err.Timestamp.After(after) {
		t.Error("Expected error timestamp to be within test execution time")
	}
}

func TestCallSiteCapture(t *testing.T) {
	err := NewValidationError("test error", nil)

	if err.Context.File == "" {
		t.Error("Expected file to be captured")
	}

	if err.Context.Line == 0 {
		t.Error("Expected line number to be captured")
	}

	if !strings.Contains(err.Context.Function, "TestCallSiteCapture") {
		t.Errorf("Expected function name to contain TestCallSiteCapture, got %s", err.Context.Function)
	}
}

func TestErrorFormatting(t *testing.T) {
	err := NewValidationError("invalid input", nil)
	errStr := err.Error()

	expectedParts := []string{
		"[validation]",
		"VALIDATION_INVALID",
		"invalid input",
	}

	for _, part := range expectedParts {
		if !strings.Contains(errStr, part) {
			t.Errorf("Expected error string to contain '%s', got: %s", part, errStr)
		}
	}

	err.Context.Component = "storage"
	errStr = err.Error()
	if !strings.Contains(errStr, "[validation:storage]") {
		t.Errorf("Expected error string to contain component, got: %s", errStr)
	}
}
