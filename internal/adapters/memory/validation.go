package memory

import (
	"github.com/eleven-am/graft/internal/ports"
)

func validateNode(node ports.NodePort) error {
	if node == nil {
		return &ports.NodeRegistrationError{
			NodeName: "<nil>",
			Reason:   "node cannot be nil",
		}
	}

	nodeName := node.GetName()
	if nodeName == "" {
		return &ports.NodeRegistrationError{
			NodeName: nodeName,
			Reason:   "node name cannot be empty",
		}
	}

	return nil
}

func validateNodeName(nodeName string) error {
	if nodeName == "" {
		return &ports.NodeRegistrationError{
			NodeName: nodeName,
			Reason:   "node name cannot be empty",
		}
	}

	return nil
}