package domain

import (
	"errors"
	"fmt"
	"strings"
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
	ErrAlreadyStarted  = errors.New("adapter already started")
	ErrAlreadyShutdown = errors.New("already shutdown")
	ErrNotStarted      = errors.New("adapter not started")
	ErrNotFound        = errors.New("resource not found")
	ErrInvalidConfig   = errors.New("invalid configuration")
	ErrTimeout         = errors.New("operation timeout")
	ErrConnection      = errors.New("connection error")
	ErrInvalidInput    = errors.New("invalid input")
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
	errStr := err.Error()
	return errors.Is(err, ErrNotFound) || 
		   strings.Contains(errStr, "log not found") ||
		   strings.Contains(errStr, "no such key") ||
		   strings.Contains(errStr, "key not found") ||
		   strings.Contains(errStr, "not found")
}