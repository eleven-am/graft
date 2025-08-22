package ports

import (
	"context"
	"time"
)

type NodePort interface {
	GetName() string
	CanStart(ctx context.Context, args ...interface{}) bool
	Execute(ctx context.Context, args ...interface{}) (*NodeResult, error)
}

type NodeResult struct {
	GlobalState interface{}
	NextNodes   []NextNode
}

type NextNode struct {
	NodeName       string         `json:"node_name"`
	Config         interface{}    `json:"config"`
	Priority       int            `json:"priority,omitempty"`
	Delay          *time.Duration `json:"delay,omitempty"`
	IdempotencyKey *string        `json:"idempotency_key,omitempty"`
}
