package events

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	pb "github.com/eleven-am/graft/internal/proto"
	"github.com/google/uuid"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type DistributedEventManager struct {
	pb.UnimplementedGraftEventStreamServer

	nodeID     string
	logger     *slog.Logger
	eventStore ports.EventStore
	transport  ports.TransportPort

	// Node management
	nodes   map[string]*NodeConnection
	nodesMu sync.RWMutex

	// Subscription management
	localSubscriptions  map[string]*ports.EventSubscription
	remoteSubscriptions map[string]*RemoteSubscription
	subscriptionsMu     sync.RWMutex

	// Event sequencing
	sequenceCounters map[string]int64
	sequenceMu       sync.Mutex

	// Channels and control
	eventChan    chan *ports.StateChangeEvent
	shutdownChan chan struct{}
	started      bool
	stopped      bool
	mu           sync.RWMutex
}

type NodeConnection struct {
	NodeID     string
	Address    string
	Client     pb.GraftEventStreamClient
	Connection *grpc.ClientConn
	Stream     pb.GraftEventStream_StreamEventsClient
	Connected  bool
	LastSeen   time.Time
	mu         sync.RWMutex
}

type RemoteSubscription struct {
	ID           string
	WorkflowID   string
	RemoteNodeID string
	LocalChannel chan *ports.WorkflowStatus
	CreatedAt    time.Time
}

func NewDistributedEventManager(nodeID string, eventStore ports.EventStore, transport ports.TransportPort, logger *slog.Logger) *DistributedEventManager {
	if logger == nil {
		logger = slog.Default()
	}

	return &DistributedEventManager{
		nodeID:              nodeID,
		logger:              logger.With("component", "distributed-event-manager", "node_id", nodeID),
		eventStore:          eventStore,
		transport:           transport,
		nodes:               make(map[string]*NodeConnection),
		localSubscriptions:  make(map[string]*ports.EventSubscription),
		remoteSubscriptions: make(map[string]*RemoteSubscription),
		sequenceCounters:    make(map[string]int64),
		eventChan:           make(chan *ports.StateChangeEvent, 1000),
		shutdownChan:        make(chan struct{}),
	}
}

func (dem *DistributedEventManager) Start(ctx context.Context) error {
	dem.mu.Lock()
	defer dem.mu.Unlock()

	if dem.started {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "distributed event manager already started",
		}
	}

	dem.logger.Info("starting distributed event manager")

	// Start event processing workers
	go dem.eventProcessor(ctx)
	go dem.nodeHealthChecker(ctx)

	dem.started = true
	dem.logger.Info("distributed event manager started successfully")

	return nil
}

func (dem *DistributedEventManager) Stop() error {
	dem.mu.Lock()
	defer dem.mu.Unlock()

	if dem.stopped {
		return nil
	}

	dem.logger.Info("stopping distributed event manager")

	close(dem.shutdownChan)

	// Close all node connections
	dem.nodesMu.Lock()
	for _, node := range dem.nodes {
		dem.closeNodeConnection(node)
	}
	dem.nodes = make(map[string]*NodeConnection)
	dem.nodesMu.Unlock()

	// Close all local subscriptions
	dem.subscriptionsMu.Lock()
	for _, sub := range dem.localSubscriptions {
		close(sub.Channel)
	}
	dem.localSubscriptions = make(map[string]*ports.EventSubscription)

	for _, sub := range dem.remoteSubscriptions {
		close(sub.LocalChannel)
	}
	dem.remoteSubscriptions = make(map[string]*RemoteSubscription)
	dem.subscriptionsMu.Unlock()

	dem.stopped = true
	dem.logger.Info("distributed event manager stopped")

	return nil
}

func (dem *DistributedEventManager) BroadcastEvent(ctx context.Context, event *ports.StateChangeEvent) error {
	// Generate event ID and sequence number
	if event.EventID == "" {
		event.EventID = uuid.New().String()
	}

	dem.sequenceMu.Lock()
	dem.sequenceCounters[event.WorkflowID]++
	event.SequenceNumber = dem.sequenceCounters[event.WorkflowID]
	dem.sequenceMu.Unlock()

	event.SourceNodeID = dem.nodeID
	event.Timestamp = time.Now()

	// Store event locally
	if dem.eventStore != nil {
		if err := dem.eventStore.StoreEvent(ctx, event); err != nil {
			dem.logger.Warn("failed to store event", "error", err, "event_id", event.EventID)
		}
	}

	// Send to local processing
	select {
	case dem.eventChan <- event:
	case <-ctx.Done():
		return ctx.Err()
	default:
		dem.logger.Warn("event channel full, dropping event", "event_id", event.EventID)
	}

	// Broadcast to connected nodes
	go dem.broadcastToNodes(ctx, event)

	return nil
}

func (dem *DistributedEventManager) SubscribeToWorkflow(workflowID string, subscriberNodeID string) (<-chan *ports.WorkflowStatus, func(), error) {
	dem.subscriptionsMu.Lock()
	defer dem.subscriptionsMu.Unlock()

	subscriptionID := uuid.New().String()
	channel := make(chan *ports.WorkflowStatus, 100)

	subscription := &ports.EventSubscription{
		ID:          subscriptionID,
		WorkflowID:  workflowID,
		NodeID:      subscriberNodeID,
		Channel:     channel,
		CreatedAt:   time.Now(),
		LastUpdated: time.Now(),
	}

	dem.localSubscriptions[subscriptionID] = subscription

	// Create unsubscribe function
	unsubscribe := func() {
		dem.subscriptionsMu.Lock()
		defer dem.subscriptionsMu.Unlock()

		if sub, exists := dem.localSubscriptions[subscriptionID]; exists {
			close(sub.Channel)
			delete(dem.localSubscriptions, subscriptionID)
		}
	}

	dem.logger.Info("created workflow subscription",
		"subscription_id", subscriptionID,
		"workflow_id", workflowID,
		"subscriber_node", subscriberNodeID)

	return channel, unsubscribe, nil
}

func (dem *DistributedEventManager) RegisterNode(nodeID string, address string) error {
	dem.nodesMu.Lock()
	defer dem.nodesMu.Unlock()

	if nodeID == dem.nodeID {
		return nil // Don't connect to self
	}

	if existing, exists := dem.nodes[nodeID]; exists {
		existing.Address = address
		existing.LastSeen = time.Now()
		return nil
	}

	dem.logger.Info("registering new node", "node_id", nodeID, "address", address)

	node := &NodeConnection{
		NodeID:    nodeID,
		Address:   address,
		Connected: false,
		LastSeen:  time.Now(),
	}

	dem.nodes[nodeID] = node

	// Attempt to connect asynchronously
	go dem.connectToNode(context.Background(), node)

	return nil
}

func (dem *DistributedEventManager) UnregisterNode(nodeID string) error {
	dem.nodesMu.Lock()
	defer dem.nodesMu.Unlock()

	if node, exists := dem.nodes[nodeID]; exists {
		dem.logger.Info("unregistering node", "node_id", nodeID)
		dem.closeNodeConnection(node)
		delete(dem.nodes, nodeID)
	}

	return nil
}

func (dem *DistributedEventManager) GetConnectedNodes() []ports.EventNodeInfo {
	dem.nodesMu.RLock()
	defer dem.nodesMu.RUnlock()

	nodes := make([]ports.EventNodeInfo, 0, len(dem.nodes))
	for _, node := range dem.nodes {
		nodes = append(nodes, ports.EventNodeInfo{
			NodeID:    node.NodeID,
			Address:   node.Address,
			Connected: node.Connected,
			LastSeen:  node.LastSeen,
		})
	}

	return nodes
}

// Helper methods

func (dem *DistributedEventManager) eventProcessor(ctx context.Context) {
	for {
		select {
		case <-ctx.Done():
			return
		case <-dem.shutdownChan:
			return
		case event := <-dem.eventChan:
			dem.processEvent(ctx, event)
		}
	}
}

func (dem *DistributedEventManager) processEvent(ctx context.Context, event *ports.StateChangeEvent) {
	dem.subscriptionsMu.RLock()
	defer dem.subscriptionsMu.RUnlock()

	// Convert event to workflow status
	workflowStatus := dem.eventToWorkflowStatus(event)
	if workflowStatus == nil {
		return
	}

	// Send to local subscribers
	for _, sub := range dem.localSubscriptions {
		if sub.WorkflowID == event.WorkflowID {
			select {
			case sub.Channel <- workflowStatus:
			default:
				dem.logger.Warn("subscription channel full", "subscription_id", sub.ID)
			}
		}
	}
}

func (dem *DistributedEventManager) eventToWorkflowStatus(event *ports.StateChangeEvent) *ports.WorkflowStatus {
	// Convert StateChangeEvent to WorkflowStatus
	// This is a simplified conversion - in practice, you might need
	// to fetch additional workflow information

	workflowState := ports.WorkflowStateRunning
	if event.EventType == ports.EventTypeWorkflowCompleted {
		workflowState = ports.WorkflowStateCompleted
	} else if event.EventType == ports.EventTypeWorkflowFailed {
		workflowState = ports.WorkflowStateFailed
	}

	return &ports.WorkflowStatus{
		WorkflowID:    event.WorkflowID,
		Status:        workflowState,
		CurrentState:  event.StateData,
		ExecutedNodes: []domain.ExecutedNodeData{}, // Would need to be populated from state
		PendingNodes:  []domain.PendingNodeData{},  // Would need to be populated from state
		ReadyNodes:    []domain.ReadyNodeData{},    // Would need to be populated from state
		LastError:     nil,                         // Would need to be extracted from event
	}
}

func (dem *DistributedEventManager) broadcastToNodes(ctx context.Context, event *ports.StateChangeEvent) {
	dem.nodesMu.RLock()
	nodes := make([]*NodeConnection, 0, len(dem.nodes))
	for _, node := range dem.nodes {
		if node.Connected {
			nodes = append(nodes, node)
		}
	}
	dem.nodesMu.RUnlock()

	if len(nodes) == 0 {
		return
	}

	// Convert to protobuf
	pbEvent := &pb.StateChangeEvent{
		EventId:        event.EventID,
		WorkflowId:     event.WorkflowID,
		ChangedBy:      event.ChangedBy,
		NodeName:       event.NodeName,
		EventType:      dem.convertEventType(event.EventType),
		Timestamp:      event.Timestamp.Unix(),
		SequenceNumber: event.SequenceNumber,
		SourceNodeId:   event.SourceNodeID,
	}

	// Serialize state data
	if event.StateData != nil {
		if stateBytes, err := json.Marshal(event.StateData); err == nil {
			pbEvent.StateData = stateBytes
		}
	}

	eventMsg := &pb.EventMessage{
		MessageType: &pb.EventMessage_StateChange{
			StateChange: pbEvent,
		},
	}

	// Broadcast to all connected nodes
	for _, node := range nodes {
		go dem.sendToNode(ctx, node, eventMsg)
	}
}

func (dem *DistributedEventManager) sendToNode(ctx context.Context, node *NodeConnection, msg *pb.EventMessage) {
	node.mu.RLock()
	stream := node.Stream
	node.mu.RUnlock()

	if stream == nil {
		return
	}

	if err := stream.Send(msg); err != nil {
		dem.logger.Warn("failed to send event to node",
			"node_id", node.NodeID,
			"error", err)

		// Mark node as disconnected and try to reconnect
		node.mu.Lock()
		node.Connected = false
		node.mu.Unlock()

		go dem.connectToNode(context.Background(), node)
	}
}

func (dem *DistributedEventManager) convertEventType(eventType ports.StateChangeEventType) pb.EventType {
	switch eventType {
	case ports.EventTypeNodeStarted:
		return pb.EventType_NODE_STARTED
	case ports.EventTypeNodeCompleted:
		return pb.EventType_NODE_COMPLETED
	case ports.EventTypeNodeFailed:
		return pb.EventType_NODE_FAILED
	case ports.EventTypeStateUpdated:
		return pb.EventType_STATE_UPDATED
	case ports.EventTypeWorkflowCompleted:
		return pb.EventType_WORKFLOW_COMPLETED
	case ports.EventTypeWorkflowFailed:
		return pb.EventType_WORKFLOW_FAILED
	default:
		return pb.EventType_STATE_UPDATED
	}
}

func (dem *DistributedEventManager) connectToNode(ctx context.Context, node *NodeConnection) {
	maxRetries := 3
	backoff := time.Second

	for i := 0; i < maxRetries; i++ {
		if err := dem.attemptNodeConnection(ctx, node); err == nil {
			return
		}

		time.Sleep(backoff)
		backoff *= 2
	}

	dem.logger.Warn("failed to connect to node after retries", "node_id", node.NodeID)
}

func (dem *DistributedEventManager) attemptNodeConnection(ctx context.Context, node *NodeConnection) error {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.Connected {
		return nil
	}

	conn, err := grpc.Dial(node.Address, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return fmt.Errorf("failed to dial node %s at %s: %w", node.NodeID, node.Address, err)
	}

	client := pb.NewGraftEventStreamClient(conn)
	stream, err := client.StreamEvents(ctx)
	if err != nil {
		conn.Close()
		return fmt.Errorf("failed to create stream to node %s: %w", node.NodeID, err)
	}

	node.Connection = conn
	node.Client = client
	node.Stream = stream
	node.Connected = true
	node.LastSeen = time.Now()

	dem.logger.Info("connected to node", "node_id", node.NodeID, "address", node.Address)

	// Start receiving events from this node
	go dem.receiveFromNode(ctx, node)

	return nil
}

func (dem *DistributedEventManager) receiveFromNode(ctx context.Context, node *NodeConnection) {
	defer func() {
		node.mu.Lock()
		node.Connected = false
		node.mu.Unlock()
	}()

	for {
		select {
		case <-ctx.Done():
			return
		case <-dem.shutdownChan:
			return
		default:
		}

		node.mu.RLock()
		stream := node.Stream
		node.mu.RUnlock()

		if stream == nil {
			return
		}

		msg, err := stream.Recv()
		if err != nil {
			dem.logger.Warn("error receiving from node", "node_id", node.NodeID, "error", err)
			return
		}

		dem.handleReceivedMessage(ctx, node.NodeID, msg)
	}
}

func (dem *DistributedEventManager) handleReceivedMessage(ctx context.Context, fromNodeID string, msg *pb.EventMessage) {
	switch msgType := msg.MessageType.(type) {
	case *pb.EventMessage_StateChange:
		dem.handleRemoteStateChange(ctx, fromNodeID, msgType.StateChange)
	case *pb.EventMessage_Subscription:
		dem.handleRemoteSubscription(ctx, fromNodeID, msgType.Subscription)
	case *pb.EventMessage_Unsubscription:
		dem.handleRemoteUnsubscription(ctx, fromNodeID, msgType.Unsubscription)
	}
}

func (dem *DistributedEventManager) handleRemoteStateChange(ctx context.Context, fromNodeID string, pbEvent *pb.StateChangeEvent) {
	// Convert protobuf event back to internal format
	stateData := make(map[string]interface{})
	if len(pbEvent.StateData) > 0 {
		json.Unmarshal(pbEvent.StateData, &stateData)
	}

	event := &ports.StateChangeEvent{
		EventID:        pbEvent.EventId,
		WorkflowID:     pbEvent.WorkflowId,
		ChangedBy:      pbEvent.ChangedBy,
		NodeName:       pbEvent.NodeName,
		EventType:      dem.convertFromPbEventType(pbEvent.EventType),
		Timestamp:      time.Unix(pbEvent.Timestamp, 0),
		SequenceNumber: pbEvent.SequenceNumber,
		StateData:      stateData,
		SourceNodeID:   pbEvent.SourceNodeId,
	}

	// Process the remote event locally (don't re-broadcast)
	dem.processEvent(ctx, event)
}

func (dem *DistributedEventManager) handleRemoteSubscription(ctx context.Context, fromNodeID string, req *pb.SubscriptionRequest) {
	// Handle remote subscription request - would implement subscription forwarding logic
	dem.logger.Info("received remote subscription",
		"from_node", fromNodeID,
		"workflow_id", req.WorkflowId,
		"subscription_id", req.SubscriptionId)
}

func (dem *DistributedEventManager) handleRemoteUnsubscription(ctx context.Context, fromNodeID string, req *pb.UnsubscriptionRequest) {
	// Handle remote unsubscription request
	dem.logger.Info("received remote unsubscription",
		"from_node", fromNodeID,
		"workflow_id", req.WorkflowId,
		"subscription_id", req.SubscriptionId)
}

func (dem *DistributedEventManager) convertFromPbEventType(pbType pb.EventType) ports.StateChangeEventType {
	switch pbType {
	case pb.EventType_NODE_STARTED:
		return ports.EventTypeNodeStarted
	case pb.EventType_NODE_COMPLETED:
		return ports.EventTypeNodeCompleted
	case pb.EventType_NODE_FAILED:
		return ports.EventTypeNodeFailed
	case pb.EventType_STATE_UPDATED:
		return ports.EventTypeStateUpdated
	case pb.EventType_WORKFLOW_COMPLETED:
		return ports.EventTypeWorkflowCompleted
	case pb.EventType_WORKFLOW_FAILED:
		return ports.EventTypeWorkflowFailed
	default:
		return ports.EventTypeStateUpdated
	}
}

func (dem *DistributedEventManager) closeNodeConnection(node *NodeConnection) {
	node.mu.Lock()
	defer node.mu.Unlock()

	if node.Stream != nil {
		node.Stream.CloseSend()
		node.Stream = nil
	}

	if node.Connection != nil {
		node.Connection.Close()
		node.Connection = nil
	}

	node.Connected = false
}

func (dem *DistributedEventManager) nodeHealthChecker(ctx context.Context) {
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-dem.shutdownChan:
			return
		case <-ticker.C:
			dem.checkNodeHealth(ctx)
		}
	}
}

func (dem *DistributedEventManager) checkNodeHealth(ctx context.Context) {
	dem.nodesMu.RLock()
	nodes := make([]*NodeConnection, 0, len(dem.nodes))
	for _, node := range dem.nodes {
		nodes = append(nodes, node)
	}
	dem.nodesMu.RUnlock()

	for _, node := range nodes {
		node.mu.RLock()
		connected := node.Connected
		lastSeen := node.LastSeen
		node.mu.RUnlock()

		if connected && time.Since(lastSeen) > 60*time.Second {
			dem.logger.Warn("node appears unhealthy", "node_id", node.NodeID)
			node.mu.Lock()
			node.Connected = false
			node.mu.Unlock()

			go dem.connectToNode(ctx, node)
		}
	}
}
