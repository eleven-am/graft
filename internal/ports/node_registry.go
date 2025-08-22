package ports

type NodeRegistryPort interface {
	RegisterNode(node NodePort) error
	GetNode(nodeName string) (NodePort, error)
	ListNodes() []string
	UnregisterNode(nodeName string) error
	HasNode(nodeName string) bool
	GetNodeCount() int
}

type NodeRegistrationError struct {
	NodeName string
	Reason   string
}

func (e NodeRegistrationError) Error() string {
	return "node registration failed for '" + e.NodeName + "': " + e.Reason
}