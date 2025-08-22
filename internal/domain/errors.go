package domain

import (
	"errors"
	"fmt"
	"strings"
)

type ErrorType string

const (
	ErrorTypeNotFound     ErrorType = "not_found"
	ErrorTypeValidation   ErrorType = "validation"
	ErrorTypeConflict     ErrorType = "conflict"
	ErrorTypeInternal     ErrorType = "internal"
	ErrorTypeUnavailable  ErrorType = "unavailable"
	ErrorTypeUnauthorized ErrorType = "unauthorized"
	ErrorTypeTimeout      ErrorType = "timeout"
	ErrorTypeRateLimit    ErrorType = "rate_limit"
)

type Error struct {
	Type    ErrorType
	Message string
	Details map[string]interface{}
}

func (e Error) Error() string {
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

var ErrKeyNotFound = errors.New("key not found")

func IsKeyNotFound(err error) bool {
	if err == nil {
		return false
	}
	
	if errors.Is(err, ErrKeyNotFound) {
		return true
	}
	
	errStr := err.Error()
	return strings.Contains(errStr, "not found") ||
		   strings.Contains(errStr, "no such key")
}

func NewNotFoundError(resource string, id string) error {
	return Error{
		Type:    ErrorTypeNotFound,
		Message: fmt.Sprintf("%s not found", resource),
		Details: map[string]interface{}{
			"resource": resource,
			"id":       id,
		},
	}
}

func NewValidationError(field string, reason string) error {
	return Error{
		Type:    ErrorTypeValidation,
		Message: fmt.Sprintf("validation failed for field %s: %s", field, reason),
		Details: map[string]interface{}{
			"field":  field,
			"reason": reason,
		},
	}
}

func NewConflictError(resource string, reason string) error {
	return Error{
		Type:    ErrorTypeConflict,
		Message: fmt.Sprintf("conflict on %s: %s", resource, reason),
		Details: map[string]interface{}{
			"resource": resource,
			"reason":   reason,
		},
	}
}

func NewClaimConflictError(workItemID, existingNodeID, requestingNodeID string) error {
	return Error{
		Type:    ErrorTypeConflict,
		Message: fmt.Sprintf("work item %s is already claimed by node %s", workItemID, existingNodeID),
		Details: map[string]interface{}{
			"work_item_id":       workItemID,
			"existing_node_id":   existingNodeID,
			"requesting_node_id": requestingNodeID,
			"error_type":         "claim_conflict",
		},
	}
}

func NewClaimExpiredError(workItemID, nodeID string) error {
	return Error{
		Type:    ErrorTypeConflict,
		Message: fmt.Sprintf("claim for work item %s by node %s has expired", workItemID, nodeID),
		Details: map[string]interface{}{
			"work_item_id": workItemID,
			"node_id":      nodeID,
			"error_type":   "claim_expired",
		},
	}
}

func NewClaimNotFoundError(workItemID, nodeID string) error {
	return Error{
		Type:    ErrorTypeNotFound,
		Message: fmt.Sprintf("no active claim found for work item %s by node %s", workItemID, nodeID),
		Details: map[string]interface{}{
			"work_item_id": workItemID,
			"node_id":      nodeID,
			"error_type":   "claim_not_found",
		},
	}
}

type PanicError struct {
	PanicValue interface{}
	StackTrace string
}

func (pe *PanicError) Error() string {
	return fmt.Sprintf("panic occurred during execution: %v", pe.PanicValue)
}

type ActionableError struct {
	BaseError     Error
	Action        string
	Documentation string
	Examples      []string
}

func (ae ActionableError) Error() string {
	message := ae.BaseError.Error()
	if ae.Action != "" {
		message += fmt.Sprintf(" | Action: %s", ae.Action)
	}
	if ae.Documentation != "" {
		message += fmt.Sprintf(" | See: %s", ae.Documentation)
	}
	return message
}

func NewActionableError(baseError Error, action, documentation string, examples []string) error {
	return ActionableError{
		BaseError:     baseError,
		Action:        action,
		Documentation: documentation,
		Examples:      examples,
	}
}

func NewConfigurationError(field, issue, suggestion string) error {
	baseError := Error{
		Type:    ErrorTypeValidation,
		Message: fmt.Sprintf("configuration issue in field '%s': %s", field, issue),
		Details: map[string]interface{}{
			"field":      field,
			"issue":      issue,
			"error_type": "configuration",
		},
	}
	
	return NewActionableError(
		baseError,
		suggestion,
		"Check configuration documentation",
		[]string{fmt.Sprintf("Set %s to a valid value", field)},
	)
}

func NewConnectionError(target, issue string) error {
	baseError := Error{
		Type:    ErrorTypeUnavailable,
		Message: fmt.Sprintf("connection failed to %s: %s", target, issue),
		Details: map[string]interface{}{
			"target":     target,
			"issue":      issue,
			"error_type": "connection",
		},
	}
	
	actions := []string{
		"Verify network connectivity",
		"Check if target service is running",
		"Validate firewall/security group rules",
		"Confirm endpoint configuration",
	}
	
	return NewActionableError(
		baseError,
		"Check network connectivity and service availability",
		"Network troubleshooting guide",
		actions,
	)
}

func NewResourceExhaustionError(resource, limit string) error {
	baseError := Error{
		Type:    ErrorTypeRateLimit,
		Message: fmt.Sprintf("resource exhaustion: %s limit reached (%s)", resource, limit),
		Details: map[string]interface{}{
			"resource":   resource,
			"limit":      limit,
			"error_type": "resource_exhaustion",
		},
	}
	
	actions := []string{
		fmt.Sprintf("Increase %s limit in configuration", resource),
		"Scale up resources if possible",
		"Implement backpressure or queue management",
		"Review resource usage patterns",
	}
	
	return NewActionableError(
		baseError,
		fmt.Sprintf("Scale up %s resources or implement throttling", resource),
		"Resource scaling guide",
		actions,
	)
}

func NewWorkflowStateError(workflowID, expectedState, actualState string) error {
	baseError := Error{
		Type:    ErrorTypeConflict,
		Message: fmt.Sprintf("workflow %s state mismatch: expected %s, got %s", 
			workflowID, expectedState, actualState),
		Details: map[string]interface{}{
			"workflow_id":    workflowID,
			"expected_state": expectedState,
			"actual_state":   actualState,
			"error_type":     "workflow_state",
		},
	}
	
	actions := []string{
		"Check workflow definition for state transitions",
		"Verify node execution completed successfully",
		"Review workflow execution logs",
		"Consider workflow recovery or restart",
	}
	
	return NewActionableError(
		baseError,
		"Review workflow execution and state management",
		"Workflow troubleshooting documentation",
		actions,
	)
}

func NewDependencyError(component, dependency, issue string) error {
	baseError := Error{
		Type:    ErrorTypeUnavailable,
		Message: fmt.Sprintf("%s dependency unavailable: %s - %s", component, dependency, issue),
		Details: map[string]interface{}{
			"component":  component,
			"dependency": dependency,
			"issue":      issue,
			"error_type": "dependency",
		},
	}
	
	actions := []string{
		fmt.Sprintf("Verify %s service is running", dependency),
		"Check service discovery and registration",
		"Validate network connectivity",
		"Review dependency health checks",
		"Consider implementing circuit breaker pattern",
	}
	
	return NewActionableError(
		baseError,
		fmt.Sprintf("Ensure %s service is healthy and accessible", dependency),
		"Dependency management guide",
		actions,
	)
}

func NewDataConsistencyError(operation, details string) error {
	baseError := Error{
		Type:    ErrorTypeInternal,
		Message: fmt.Sprintf("data consistency issue during %s: %s", operation, details),
		Details: map[string]interface{}{
			"operation":  operation,
			"details":    details,
			"error_type": "data_consistency",
		},
	}
	
	actions := []string{
		"Review distributed system consistency settings",
		"Check for concurrent modification conflicts",
		"Verify transaction isolation levels",
		"Consider implementing optimistic locking",
		"Review data replication and synchronization",
	}
	
	return NewActionableError(
		baseError,
		"Review consistency model and conflict resolution strategy",
		"Distributed systems consistency guide",
		actions,
	)
}
