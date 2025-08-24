package domain

import (
	"errors"
	"fmt"
)

var (
	ErrAlreadyStarted  = errors.New("adapter already started")
	ErrAlreadyShutdown = errors.New("already shutdown")
	ErrNotStarted      = errors.New("adapter not started")
	ErrNotFound        = errors.New("resource not found")
	ErrInvalidConfig   = errors.New("invalid configuration")
	ErrTimeout         = errors.New("operation timeout")
	ErrConnection      = errors.New("connection error")
	ErrInvalidInput    = errors.New("invalid input")
	ErrInternalError   = errors.New("internal error")
	ErrAlreadyExists   = errors.New("resource already exists")
)

type TransportError struct {
	Op      string
	Adapter string
	Err     error
}

func (e *TransportError) Error() string {
	return fmt.Sprintf("transport[%s] %s: %v", e.Adapter, e.Op, e.Err)
}

func (e *TransportError) Unwrap() error {
	return e.Err
}

func NewTransportError(adapter, op string, err error) *TransportError {
	return &TransportError{
		Op:      op,
		Adapter: adapter,
		Err:     err,
	}
}

type DiscoveryError struct {
	Op      string
	Adapter string
	Err     error
}

func (e *DiscoveryError) Error() string {
	return fmt.Sprintf("discovery[%s] %s: %v", e.Adapter, e.Op, e.Err)
}

func (e *DiscoveryError) Unwrap() error {
	return e.Err
}

func NewDiscoveryError(adapter, op string, err error) *DiscoveryError {
	return &DiscoveryError{
		Op:      op,
		Adapter: adapter,
		Err:     err,
	}
}

type ConnectionError struct {
	Target string
	Err    error
}

func (e *ConnectionError) Error() string {
	return fmt.Sprintf("connection to %s failed: %v", e.Target, e.Err)
}

func (e *ConnectionError) Unwrap() error {
	return e.Err
}

func NewConnectionError(target string, err error) *ConnectionError {
	return &ConnectionError{
		Target: target,
		Err:    err,
	}
}

type LeaderError struct {
	Op  string
	Err error
}

func (e *LeaderError) Error() string {
	return fmt.Sprintf("leader %s: %v", e.Op, e.Err)
}

func (e *LeaderError) Unwrap() error {
	return e.Err
}

func NewLeaderError(op string, err error) *LeaderError {
	return &LeaderError{
		Op:  op,
		Err: err,
	}
}

type ConfigError struct {
	Field string
	Err   error
}

func (e *ConfigError) Error() string {
	return fmt.Sprintf("config field %s: %v", e.Field, e.Err)
}

func (e *ConfigError) Unwrap() error {
	return e.Err
}

func NewConfigError(field string, err error) *ConfigError {
	return &ConfigError{
		Field: field,
		Err:   err,
	}
}

type SemaphoreError struct {
	SemaphoreID string
	Op          string
	Err         error
}

func (e *SemaphoreError) Error() string {
	return fmt.Sprintf("semaphore[%s] %s: %v", e.SemaphoreID, e.Op, e.Err)
}

func (e *SemaphoreError) Unwrap() error {
	return e.Err
}

func NewSemaphoreError(semaphoreID, op string, err error) *SemaphoreError {
	return &SemaphoreError{
		SemaphoreID: semaphoreID,
		Op:          op,
		Err:         err,
	}
}

type ResourceError struct {
	Resource string
	Op       string
	Err      error
}

func (e *ResourceError) Error() string {
	return fmt.Sprintf("resource[%s] %s: %v", e.Resource, e.Op, e.Err)
}

func (e *ResourceError) Unwrap() error {
	return e.Err
}

func NewResourceError(resource, op string, err error) *ResourceError {
	return &ResourceError{
		Resource: resource,
		Op:       op,
		Err:      err,
	}
}

type WorkflowError struct {
	WorkflowID string
	Op         string
	Err        error
}

func (e *WorkflowError) Error() string {
	return fmt.Sprintf("workflow[%s] %s: %v", e.WorkflowID, e.Op, e.Err)
}

func (e *WorkflowError) Unwrap() error {
	return e.Err
}

func NewWorkflowError(workflowID, op string, err error) *WorkflowError {
	return &WorkflowError{
		WorkflowID: workflowID,
		Op:         op,
		Err:        err,
	}
}

type NodeError struct {
	NodeName string
	Op       string
	Err      error
}

func (e *NodeError) Error() string {
	return fmt.Sprintf("node[%s] %s: %v", e.NodeName, e.Op, e.Err)
}

func (e *NodeError) Unwrap() error {
	return e.Err
}

func NewNodeError(nodeName, op string, err error) *NodeError {
	return &NodeError{
		NodeName: nodeName,
		Op:       op,
		Err:      err,
	}
}

type StorageError struct {
	Operation string
	Key       string
	Err       error
}

func (e *StorageError) Error() string {
	if e.Key != "" {
		return fmt.Sprintf("storage[%s] key[%s]: %v", e.Operation, e.Key, e.Err)
	}
	return fmt.Sprintf("storage[%s]: %v", e.Operation, e.Err)
}

func (e *StorageError) Unwrap() error {
	return e.Err
}

func NewStorageError(operation, key string, err error) *StorageError {
	return &StorageError{
		Operation: operation,
		Key:       key,
		Err:       err,
	}
}

var (
	ErrConflict      = errors.New("resource conflict")
	ErrUnauthorized  = errors.New("unauthorized operation")
	ErrExpired       = errors.New("resource expired")
	ErrCapacityLimit = errors.New("capacity limit reached")
	ErrInvalidState  = errors.New("invalid state")
)

func IsAlreadyStarted(err error) bool {
	return errors.Is(err, ErrAlreadyStarted)
}

func IsNotStarted(err error) bool {
	return errors.Is(err, ErrNotStarted)
}

func IsConnectionError(err error) bool {
	var connErr *ConnectionError
	return errors.As(err, &connErr) || errors.Is(err, ErrConnection)
}

func IsConfigError(err error) bool {
	var configErr *ConfigError
	return errors.As(err, &configErr) || errors.Is(err, ErrInvalidConfig)
}

func IsTransportError(err error) bool {
	var transportErr *TransportError
	return errors.As(err, &transportErr)
}

func IsDiscoveryError(err error) bool {
	var discoveryErr *DiscoveryError
	return errors.As(err, &discoveryErr)
}

func IsNotFoundError(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func NewNotFoundError(resourceType, identifier string) error {
	return fmt.Errorf("%s %s: %w", resourceType, identifier, ErrNotFound)
}

func IsKeyNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

const (
	ErrorTypeValidation  = "validation"
	ErrorTypeInternal    = "internal"
	ErrorTypeConflict    = "conflict"
	ErrorTypeNotFound    = "not_found"
	ErrorTypeUnavailable = "unavailable"
	ErrorTypeRateLimit   = "rate_limit"
)

type Error struct {
	Type    string                 `json:"type"`
	Message string                 `json:"message"`
	Details map[string]interface{} `json:"details,omitempty"`
}

func (e Error) Error() string {
	if e.Details != nil {
		return fmt.Sprintf("%s: %s (details: %v)", e.Type, e.Message, e.Details)
	}
	return fmt.Sprintf("%s: %s", e.Type, e.Message)
}

func NewValidationError(field, message string) error {
	return Error{
		Type:    ErrorTypeValidation,
		Message: fmt.Sprintf("%s: %s", field, message),
	}
}
