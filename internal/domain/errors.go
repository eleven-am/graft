package domain

import (
	"errors"
	"fmt"
	"runtime"
	"strings"
	"time"
)

type StorageError struct {
	Type    ErrorType
	Key     string
	Message string
}

func (e *StorageError) Error() string {
	return e.Message
}

type ErrorType int

const (
	ErrKeyNotFound ErrorType = iota
	ErrVersionMismatch
	ErrTransactionConflict
	ErrStorageFull
	ErrCorrupted
	ErrClosed
)

type ErrorCategory int

const (
	CategoryUnknown ErrorCategory = iota
	CategoryValidation
	CategoryNetwork
	CategoryStorage
	CategoryRaft
	CategoryWorkflow
	CategoryDiscovery
	CategoryConfiguration
	CategoryTimeout
	CategoryPermission
	CategoryResource
)

func (c ErrorCategory) String() string {
	switch c {
	case CategoryValidation:
		return "validation"
	case CategoryNetwork:
		return "network"
	case CategoryStorage:
		return "storage"
	case CategoryRaft:
		return "raft"
	case CategoryWorkflow:
		return "workflow"
	case CategoryDiscovery:
		return "discovery"
	case CategoryConfiguration:
		return "configuration"
	case CategoryTimeout:
		return "timeout"
	case CategoryPermission:
		return "permission"
	case CategoryResource:
		return "resource"
	default:
		return "unknown"
	}
}

type ErrorSeverity int

const (
	SeverityInfo ErrorSeverity = iota
	SeverityWarning
	SeverityError
	SeverityCritical
)

func (s ErrorSeverity) String() string {
	switch s {
	case SeverityInfo:
		return "info"
	case SeverityWarning:
		return "warning"
	case SeverityError:
		return "error"
	case SeverityCritical:
		return "critical"
	default:
		return "unknown"
	}
}

type DomainError struct {
	Category   ErrorCategory `json:"category"`
	Severity   ErrorSeverity `json:"severity"`
	Code       string        `json:"code"`
	Message    string        `json:"message"`
	Context    ErrorContext  `json:"context"`
	Cause      error         `json:"-"`
	Timestamp  time.Time     `json:"timestamp"`
	Retryable  bool          `json:"retryable"`
	UserFacing bool          `json:"user_facing"`
}

type ErrorOption func(*DomainError)

func WithSeverity(severity ErrorSeverity) ErrorOption {
	return func(err *DomainError) {
		err.Severity = severity
	}
}

func WithCode(code string) ErrorOption {
	return func(err *DomainError) {
		err.Code = code
	}
}

func WithRetryable(retryable bool) ErrorOption {
	return func(err *DomainError) {
		err.Retryable = retryable
	}
}

func WithUserFacing(userFacing bool) ErrorOption {
	return func(err *DomainError) {
		err.UserFacing = userFacing
	}
}

func WithComponent(component string) ErrorOption {
	return func(err *DomainError) {
		err.Context.Component = component
	}
}

func WithOperation(operation string) ErrorOption {
	return func(err *DomainError) {
		err.Context.Operation = operation
	}
}

func WithNodeID(nodeID string) ErrorOption {
	return func(err *DomainError) {
		err.Context.NodeID = nodeID
	}
}

func WithWorkflowID(workflowID string) ErrorOption {
	return func(err *DomainError) {
		err.Context.WorkflowID = workflowID
	}
}

func WithRequestID(requestID string) ErrorOption {
	return func(err *DomainError) {
		err.Context.RequestID = requestID
	}
}

func WithContextDetail(key, value string) ErrorOption {
	return func(err *DomainError) {
		if err.Context.Details == nil {
			err.Context.Details = make(map[string]string)
		}
		err.Context.Details[key] = value
	}
}

type ErrorContext struct {
	NodeID     string            `json:"node_id,omitempty"`
	WorkflowID string            `json:"workflow_id,omitempty"`
	RequestID  string            `json:"request_id,omitempty"`
	Operation  string            `json:"operation,omitempty"`
	Component  string            `json:"component,omitempty"`
	File       string            `json:"file,omitempty"`
	Line       int               `json:"line,omitempty"`
	Function   string            `json:"function,omitempty"`
	Details    map[string]string `json:"details,omitempty"`
}

func (e *DomainError) Error() string {
	if e.Context.Component != "" {
		return fmt.Sprintf("[%s:%s] %s: %s", e.Category, e.Context.Component, e.Code, e.Message)
	}
	return fmt.Sprintf("[%s] %s: %s", e.Category, e.Code, e.Message)
}

func (e *DomainError) Unwrap() error {
	return e.Cause
}

func (e *DomainError) Is(target error) bool {
	if t, ok := target.(*DomainError); ok {
		return e.Code == t.Code && e.Category == t.Category
	}
	return false
}

func (e *DomainError) WithContext(key, value string) *DomainError {
	if e.Context.Details == nil {
		e.Context.Details = make(map[string]string)
	}
	e.Context.Details[key] = value
	return e
}

func (e *DomainError) WithNodeID(nodeID string) *DomainError {
	e.Context.NodeID = nodeID
	return e
}

func (e *DomainError) WithWorkflowID(workflowID string) *DomainError {
	e.Context.WorkflowID = workflowID
	return e
}

func (e *DomainError) WithRequestID(requestID string) *DomainError {
	e.Context.RequestID = requestID
	return e
}

func (e *DomainError) WithOperation(operation string) *DomainError {
	e.Context.Operation = operation
	return e
}

func NewDomainError(message string, cause error, opts ...ErrorOption) *DomainError {
	return NewDomainErrorWithCategory(CategoryUnknown, message, cause, opts...)
}

func NewDomainErrorWithCategory(category ErrorCategory, message string, cause error, opts ...ErrorOption) *DomainError {
	err := &DomainError{
		Category:   category,
		Severity:   SeverityError,
		Code:       inferErrorCode(category, message, cause),
		Message:    message,
		Context:    ErrorContext{},
		Cause:      cause,
		Timestamp:  time.Now(),
		Retryable:  isRetryable(category, cause),
		UserFacing: isUserFacing(category),
	}

	for _, opt := range opts {
		if opt != nil {
			opt(err)
		}
	}

	err.captureCallSite()
	return err
}

func (e *DomainError) captureCallSite() {
	if pc, file, line, ok := runtime.Caller(3); ok {
		e.Context.File = file
		e.Context.Line = line
		if fn := runtime.FuncForPC(pc); fn != nil {
			e.Context.Function = fn.Name()
		}
	}
}

func inferErrorCode(category ErrorCategory, message string, cause error) string {
	if cause != nil {
		if de, ok := cause.(*DomainError); ok {
			return de.Code
		}
	}

	msg := strings.ToLower(message)
	switch category {
	case CategoryValidation:
		if strings.Contains(msg, "required") {
			return "VALIDATION_REQUIRED"
		}
		if strings.Contains(msg, "invalid") {
			return "VALIDATION_INVALID"
		}
		return "VALIDATION_ERROR"
	case CategoryNetwork:
		if strings.Contains(msg, "timeout") {
			return "NETWORK_TIMEOUT"
		}
		if strings.Contains(msg, "connection") {
			return "NETWORK_CONNECTION"
		}
		return "NETWORK_ERROR"
	case CategoryStorage:
		if strings.Contains(msg, "not found") {
			return "STORAGE_NOT_FOUND"
		}
		if strings.Contains(msg, "conflict") {
			return "STORAGE_CONFLICT"
		}
		return "STORAGE_ERROR"
	case CategoryRaft:
		if strings.Contains(msg, "leader") {
			return "RAFT_LEADER"
		}
		if strings.Contains(msg, "election") {
			return "RAFT_ELECTION"
		}
		return "RAFT_ERROR"
	case CategoryWorkflow:
		if strings.Contains(msg, "timeout") {
			return "WORKFLOW_TIMEOUT"
		}
		if strings.Contains(msg, "state") {
			return "WORKFLOW_STATE"
		}
		return "WORKFLOW_ERROR"
	case CategoryTimeout:
		return "TIMEOUT_ERROR"
	case CategoryPermission:
		return "PERMISSION_DENIED"
	case CategoryResource:
		if strings.Contains(msg, "limit") {
			return "RESOURCE_LIMIT"
		}
		if strings.Contains(msg, "full") {
			return "RESOURCE_EXHAUSTED"
		}
		return "RESOURCE_ERROR"
	default:
		return "UNKNOWN_ERROR"
	}
}

func isRetryable(category ErrorCategory, cause error) bool {
	switch category {
	case CategoryNetwork, CategoryTimeout, CategoryResource:
		return true
	case CategoryRaft:
		return true
	case CategoryValidation, CategoryConfiguration, CategoryPermission:
		return false
	default:
		if cause != nil {
			if de, ok := cause.(*DomainError); ok {
				return de.Retryable
			}
		}
		return false
	}
}

func isUserFacing(category ErrorCategory) bool {
	switch category {
	case CategoryValidation, CategoryConfiguration, CategoryPermission:
		return true
	default:
		return false
	}
}

func NewKeyNotFoundError(key string) *StorageError {
	return &StorageError{
		Type:    ErrKeyNotFound,
		Key:     key,
		Message: "key not found: " + key,
	}
}

func NewVersionMismatchError(key string, expected, actual int64) *StorageError {
	return &StorageError{
		Type:    ErrVersionMismatch,
		Key:     key,
		Message: fmt.Sprintf("version mismatch for key %s: expected %d, got %d", key, expected, actual),
	}
}

var (
	ErrAlreadyStarted = NewDomainErrorWithCategory(
		CategoryConfiguration,
		"adapter already started",
		nil,
		WithCode("SYSTEM_ALREADY_STARTED"),
		WithSeverity(SeverityWarning),
	)
	ErrAlreadyShutdown = NewDomainErrorWithCategory(
		CategoryConfiguration,
		"already shutdown",
		nil,
		WithCode("SYSTEM_ALREADY_SHUTDOWN"),
		WithSeverity(SeverityInfo),
	)
	ErrNotStarted = NewDomainErrorWithCategory(
		CategoryConfiguration,
		"adapter not started",
		nil,
		WithCode("SYSTEM_NOT_STARTED"),
		WithSeverity(SeverityWarning),
	)
	ErrNotFound = NewDomainErrorWithCategory(
		CategoryStorage,
		"resource not found",
		nil,
		WithCode("RESOURCE_NOT_FOUND"),
	)
	ErrInvalidConfig = NewDomainErrorWithCategory(
		CategoryConfiguration,
		"invalid configuration",
		nil,
		WithCode("CONFIG_INVALID"),
	)
	ErrTimeout = NewDomainErrorWithCategory(
		CategoryTimeout,
		"operation timeout",
		nil,
		WithCode("OPERATION_TIMEOUT"),
	)
	ErrConnection = NewDomainErrorWithCategory(
		CategoryNetwork,
		"connection error",
		nil,
		WithCode("NETWORK_CONNECTION_ERROR"),
	)
	ErrInvalidInput = NewDomainErrorWithCategory(
		CategoryValidation,
		"invalid input",
		nil,
		WithCode("VALIDATION_INVALID_INPUT"),
	)
	ErrNodeNotReady = NewDomainErrorWithCategory(
		CategoryWorkflow,
		"node not ready to execute",
		nil,
		WithCode("WORKFLOW_NODE_NOT_READY"),
	)
	ErrLeaseOwnedByOther = NewDomainErrorWithCategory(
		CategoryResource,
		"lease owned by another owner",
		nil,
		WithCode("LEASE_OWNED_BY_OTHER"),
	)
	ErrLeaseNotFound = NewDomainErrorWithCategory(
		CategoryStorage,
		"lease not found",
		nil,
		WithCode("LEASE_NOT_FOUND"),
	)
)

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

func IsDiscoveryError(err error) bool {
	var discoveryErr *DiscoveryError
	return errors.As(err, &discoveryErr)
}

func IsAlreadyStarted(err error) bool {
	return errors.Is(err, ErrAlreadyStarted)
}

func IsNotStarted(err error) bool {
	return errors.Is(err, ErrNotStarted)
}

func IsNotFound(err error) bool {
	return errors.Is(err, ErrNotFound)
}

func IsLeaseOwnedByOther(err error) bool {
	return errors.Is(err, ErrLeaseOwnedByOther)
}

func IsLeaseNotFound(err error) bool {
	return errors.Is(err, ErrLeaseNotFound) || errors.Is(err, ErrNotFound)
}

func IsTimeout(err error) bool {
	return errors.Is(err, ErrTimeout)
}

func IsConnection(err error) bool {
	return errors.Is(err, ErrConnection)
}

func IsInvalidConfig(err error) bool {
	return errors.Is(err, ErrInvalidConfig)
}

func IsNotFoundError(err error) bool {
	if err == nil {
		return false
	}

	if de, ok := err.(*DomainError); ok {
		return de.Category == CategoryStorage && strings.Contains(de.Code, "NOT_FOUND")
	}

	errStr := err.Error()
	return errors.Is(err, ErrNotFound) ||
		strings.Contains(errStr, "log not found") ||
		strings.Contains(errStr, "no such key") ||
		strings.Contains(errStr, "key not found") ||
		strings.Contains(errStr, "not found")
}

// Enhanced error checking functions for DomainError
func IsDomainError(err error) bool {
	_, ok := err.(*DomainError)
	return ok
}

func GetErrorCategory(err error) ErrorCategory {
	if de, ok := err.(*DomainError); ok {
		return de.Category
	}
	return CategoryUnknown
}

func GetErrorSeverity(err error) ErrorSeverity {
	if de, ok := err.(*DomainError); ok {
		return de.Severity
	}
	return SeverityError
}

func IsRetryableError(err error) bool {
	if de, ok := err.(*DomainError); ok {
		return de.Retryable
	}

	errStr := strings.ToLower(err.Error())
	return strings.Contains(errStr, "timeout") ||
		strings.Contains(errStr, "connection") ||
		strings.Contains(errStr, "temporary") ||
		strings.Contains(errStr, "retry")
}

func IsUserFacingError(err error) bool {
	if de, ok := err.(*DomainError); ok {
		return de.UserFacing
	}
	return false
}

func GetErrorContext(err error) *ErrorContext {
	if de, ok := err.(*DomainError); ok {
		return &de.Context
	}
	return nil
}

// Convenience constructors for common error types
func NewValidationError(message string, cause error, opts ...ErrorOption) *DomainError {
	return NewDomainErrorWithCategory(CategoryValidation, message, cause, opts...)
}

func NewNetworkError(message string, cause error, opts ...ErrorOption) *DomainError {
	return NewDomainErrorWithCategory(CategoryNetwork, message, cause, opts...)
}

func NewStorageError(message string, cause error, opts ...ErrorOption) *DomainError {
	return NewDomainErrorWithCategory(CategoryStorage, message, cause, opts...)
}

func NewRaftError(message string, cause error, opts ...ErrorOption) *DomainError {
	return NewDomainErrorWithCategory(CategoryRaft, message, cause, opts...)
}

func NewWorkflowError(message string, cause error, opts ...ErrorOption) *DomainError {
	return NewDomainErrorWithCategory(CategoryWorkflow, message, cause, opts...)
}

func NewTimeoutError(message string, cause error, opts ...ErrorOption) *DomainError {
	return NewDomainErrorWithCategory(CategoryTimeout, message, cause, opts...)
}

func NewConfigurationError(message string, cause error, opts ...ErrorOption) *DomainError {
	return NewDomainErrorWithCategory(CategoryConfiguration, message, cause, opts...)
}

func NewPermissionError(message string, cause error, opts ...ErrorOption) *DomainError {
	return NewDomainErrorWithCategory(CategoryPermission, message, cause, opts...)
}

func NewResourceError(message string, cause error, opts ...ErrorOption) *DomainError {
	return NewDomainErrorWithCategory(CategoryResource, message, cause, opts...)
}

type ErrNotLeader struct {
	LeaderAddr string
}

func (e ErrNotLeader) Error() string {
	if e.LeaderAddr != "" {
		return fmt.Sprintf("not leader, leader is at: %s", e.LeaderAddr)
	}
	return "not leader, no known leader"
}
