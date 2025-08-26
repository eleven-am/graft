package node_registry

import (
	"context"
	"encoding/json"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type DomainBridge struct {
	registry ports.NodeRegistryPort
}

func NewDomainBridge(registry ports.NodeRegistryPort) domain.NodeRegistryPort {
	return &DomainBridge{registry: registry}
}

func (b *DomainBridge) RegisterNode(node domain.NodePort) error {
	portNode := &nodePortAdapter{domainNode: node}
	return b.registry.RegisterNode(portNode)
}

func (b *DomainBridge) GetNode(nodeName string) (domain.NodePort, error) {
	portNode, err := b.registry.GetNode(nodeName)
	if err != nil {
		return nil, err
	}

	if adapter, ok := portNode.(*nodePortAdapter); ok {
		return adapter.domainNode, nil
	}

	return &domainNodeAdapter{portNode: portNode}, nil
}

func (b *DomainBridge) ListNodes() []string {
	return b.registry.ListNodes()
}

func (b *DomainBridge) HasNode(nodeName string) bool {
	return b.registry.HasNode(nodeName)
}

type nodePortAdapter struct {
	domainNode domain.NodePort
}

func (n *nodePortAdapter) GetName() string {
	return n.domainNode.GetName()
}

func (n *nodePortAdapter) CanStart(ctx context.Context, args ...interface{}) bool {
	var state, config []byte

	if len(args) > 0 {
		if rawMsg, ok := args[0].(json.RawMessage); ok {
			state = rawMsg
		} else if args[0] != nil {
			var err error
			state, err = json.Marshal(args[0])
			if err != nil {
				return false
			}
		}
	}

	if len(args) > 1 {
		if rawMsg, ok := args[1].(json.RawMessage); ok {
			config = rawMsg
		} else if args[1] != nil {
			var err error
			config, err = json.Marshal(args[1])
			if err != nil {
				return false
			}
		}
	}

	return n.domainNode.CanStart(ctx, state, config)
}

func (n *nodePortAdapter) Execute(ctx context.Context, args ...interface{}) (*ports.NodeResult, error) {
	var state, config []byte

	if len(args) > 0 {
		if rawMsg, ok := args[0].(json.RawMessage); ok {
			state = rawMsg
		} else if args[0] != nil {
			var err error
			state, err = json.Marshal(args[0])
			if err != nil {
				return nil, err
			}
		}
	}

	if len(args) > 1 {
		if rawMsg, ok := args[1].(json.RawMessage); ok {
			config = rawMsg
		} else if args[1] != nil {
			var err error
			config, err = json.Marshal(args[1])
			if err != nil {
				return nil, err
			}
		}
	}

	domainResult, err := n.domainNode.Execute(ctx, state, config)
	if err != nil {
		return nil, err
	}

	var nextNodes []ports.NextNode
	if domainResult.NextNodes != nil {
		nextNodes = make([]ports.NextNode, len(domainResult.NextNodes))
		for i, next := range domainResult.NextNodes {
			nextNodes[i] = ports.NextNode{
				NodeName:       next.NodeName,
				Config:         next.Config,
				Priority:       next.Priority,
				Delay:          next.Delay,
				IdempotencyKey: next.IdempotencyKey,
			}
		}
	}

	var globalState json.RawMessage
	if domainResult.GlobalState != nil {
		var err error
		globalState, err = json.Marshal(domainResult.GlobalState)
		if err != nil {
			return nil, err
		}
	}

	return &ports.NodeResult{
		GlobalState: globalState,
		NextNodes:   nextNodes,
	}, nil
}

type domainNodeAdapter struct {
	portNode ports.NodePort
}

func (d *domainNodeAdapter) GetName() string {
	return d.portNode.GetName()
}

func (d *domainNodeAdapter) CanStart(ctx context.Context, state []byte, config []byte) bool {
	var args []interface{}

	if len(state) > 0 {
		args = append(args, json.RawMessage(state))
	} else {
		args = append(args, nil)
	}

	if len(config) > 0 {
		args = append(args, json.RawMessage(config))
	} else {
		args = append(args, nil)
	}

	return d.portNode.CanStart(ctx, args...)
}

func (d *domainNodeAdapter) Execute(ctx context.Context, state []byte, config []byte) (*domain.NodeResult, error) {
	var args []interface{}

	if len(state) > 0 {
		args = append(args, json.RawMessage(state))
	} else {
		args = append(args, nil)
	}

	if len(config) > 0 {
		args = append(args, json.RawMessage(config))
	} else {
		args = append(args, nil)
	}

	portResult, err := d.portNode.Execute(ctx, args...)
	if err != nil {
		return nil, err
	}

	var nextNodes []domain.NextNode
	if portResult.NextNodes != nil {
		nextNodes = make([]domain.NextNode, len(portResult.NextNodes))
		for i, next := range portResult.NextNodes {
			var configBytes json.RawMessage
			if next.Config != nil {
				configBytes, err = json.Marshal(next.Config)
				if err != nil {
					return nil, err
				}
			} else {
				configBytes = json.RawMessage("null")
			}

			nextNodes[i] = domain.NextNode{
				NodeName:       next.NodeName,
				Config:         configBytes,
				Priority:       next.Priority,
				Delay:          next.Delay,
				IdempotencyKey: next.IdempotencyKey,
			}
		}
	}

	var globalState interface{}
	if portResult.GlobalState != nil {
		if err := json.Unmarshal(portResult.GlobalState, &globalState); err != nil {
			return nil, err
		}
	}

	return &domain.NodeResult{
		GlobalState: globalState,
		NextNodes:   nextNodes,
	}, nil
}
