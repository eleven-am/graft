package ports

import (
	"context"
	"time"
)

type NodePort interface {
	GetName() string
	CanStart(ctx context.Context, globalState interface{}, config interface{}) bool
	Execute(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []NextNode, error)
}

type NextNode struct {
	NodeName string         `json:"node_name"`
	Config   interface{}    `json:"config"`
	Priority int            `json:"priority,omitempty"`
	Delay    *time.Duration `json:"delay,omitempty"`
}
