package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"
	"time"

	"dario.cat/mergo"
	"github.com/eleven-am/graft/internal/core/cluster"
	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type GraftCluster struct {
	internal *cluster.Cluster
}

func NewCluster(config cluster.ClusterConfig, logger *slog.Logger) (*GraftCluster, error) {
	internalCluster, err := cluster.New(config, logger)
	if err != nil {
		return nil, domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to create internal cluster",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return &GraftCluster{
		internal: internalCluster,
	}, nil
}

func (c *GraftCluster) Start(ctx context.Context) error {
	return c.internal.Start(ctx)
}

func (c *GraftCluster) Stop() error {
	return c.internal.Stop()
}

func (c *GraftCluster) RegisterNode(adapter ports.NodePort) error {
	return c.internal.RegisterNode(adapter)
}

func (c *GraftCluster) StartWorkflow(workflowID string, trigger ports.WorkflowTrigger) error {
	return c.internal.ProcessTrigger(trigger)
}

func (c *GraftCluster) GetWorkflowState(workflowID string) (*ports.WorkflowStatus, error) {
	return c.internal.GetWorkflowStatus(workflowID)
}

func (c *GraftCluster) GetClusterInfo() ports.ClusterInfo {
	return c.internal.GetClusterInfo()
}

func (c *GraftCluster) OnComplete(handler ports.CompletionHandler) {
	c.internal.OnComplete(handler)
}

func (c *GraftCluster) OnError(handler ports.ErrorHandler) {
	c.internal.OnError(handler)
}

type NodeAdapter struct {
	node         interface{}
	nodeName     string
	configType   reflect.Type
	stateType    reflect.Type
	resultType   reflect.Type
	executeFunc  reflect.Value
	canStartFunc *reflect.Value
}

func NewNodeAdapter(node interface{}) *NodeAdapter {
	configType, stateType, resultType := ExtractTypesFromExecute(node)

	adapter := &NodeAdapter{
		node:       node,
		nodeName:   ExtractNodeName(node),
		configType: configType,
		stateType:  stateType,
		resultType: resultType,
	}

	executeMethod := reflect.ValueOf(node).MethodByName("Execute")
	if !executeMethod.IsValid() {
		return nil
	}
	adapter.executeFunc = executeMethod

	canStartMethod := reflect.ValueOf(node).MethodByName("CanStart")
	if canStartMethod.IsValid() {
		adapter.canStartFunc = &canStartMethod
	}

	return adapter
}

func (n *NodeAdapter) Execute(ctx context.Context, globalState interface{}, config interface{}) (interface{}, []ports.NextNode, error) {
	methodType := n.executeFunc.Type()
	numParams := methodType.NumIn()

	args := []reflect.Value{reflect.ValueOf(ctx)}

	if numParams >= 2 {
		stateVal := reflect.New(n.stateType).Interface()
		if err := SafeTypeConversion(globalState, stateVal); err != nil {
			return nil, nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "state type conversion failed",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}
		args = append(args, reflect.ValueOf(stateVal).Elem())
	}

	if numParams >= 3 {
		configVal := reflect.New(n.configType).Interface()
		if err := SafeTypeConversion(config, configVal); err != nil {
			return nil, nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "config type conversion failed",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}
		args = append(args, reflect.ValueOf(configVal).Elem())
	}

	results := n.executeFunc.Call(args)
	if len(results) != 2 {
		return nil, nil, domain.NewValidationError("Execute method", "must return (result, error) signature")
	}

	if !results[1].IsNil() {
		return nil, nil, results[1].Interface().(error)
	}

	nodeResult := results[0].Interface()
	resultData := ExtractNodeResultData(nodeResult)
	nextNodes := ExtractNextNodes(nodeResult)

	return resultData, nextNodes, nil
}

func (n *NodeAdapter) GetName() string {
	return n.nodeName
}

func (n *NodeAdapter) CanStart(ctx context.Context, globalState interface{}, config interface{}) bool {
	if n.canStartFunc == nil {
		return true
	}

	methodType := n.canStartFunc.Type()
	numParams := methodType.NumIn()

	args := []reflect.Value{reflect.ValueOf(ctx)}

	if numParams >= 2 {
		stateVal := reflect.New(n.stateType).Interface()
		if err := SafeTypeConversion(globalState, stateVal); err != nil {
			return false
		}
		args = append(args, reflect.ValueOf(stateVal).Elem())
	}

	if numParams >= 3 {
		configVal := reflect.New(n.configType).Interface()
		if err := SafeTypeConversion(config, configVal); err != nil {
			return false
		}
		args = append(args, reflect.ValueOf(configVal).Elem())
	}

	results := n.canStartFunc.Call(args)
	if len(results) != 1 {
		return false
	}

	return results[0].Bool()
}

func ExtractNodeName(node interface{}) string {
	getNameMethod := reflect.ValueOf(node).MethodByName("GetName")
	if !getNameMethod.IsValid() {
		return reflect.TypeOf(node).Name()
	}

	results := getNameMethod.Call([]reflect.Value{})
	if len(results) != 1 {
		return reflect.TypeOf(node).Name()
	}

	return results[0].String()
}

func ExtractTypesFromExecute(node interface{}) (configType, stateType, resultType reflect.Type) {
	t := reflect.TypeOf(node)
	method, exists := t.MethodByName("Execute")
	if !exists {
		interfaceType := reflect.TypeOf((*interface{})(nil)).Elem()
		return interfaceType, interfaceType, interfaceType
	}

	if method.Type.NumIn() >= 2 && method.Type.NumOut() >= 2 {
		interfaceType := reflect.TypeOf((*interface{})(nil)).Elem()
		configType = interfaceType
		stateType = interfaceType
		resultType = interfaceType

		numParams := method.Type.NumIn()

		if numParams >= 3 {
			stateType = method.Type.In(2)
			if numParams >= 4 {
				configType = method.Type.In(3)
			}
		}

		returnType := method.Type.Out(0)
		if returnType.Kind() == reflect.Struct {
			if returnType.NumField() > 0 {
				for i := 0; i < returnType.NumField(); i++ {
					field := returnType.Field(i)
					if field.Name == "Data" {
						resultType = field.Type
						break
					}
				}
			}
		} else {
			resultType = returnType
		}
	}

	return configType, stateType, resultType
}

func ExtractNodeResultData(nodeResult interface{}) interface{} {
	resultVal := reflect.ValueOf(nodeResult)
	if resultVal.Kind() == reflect.Ptr {
		resultVal = resultVal.Elem()
	}

	dataField := resultVal.FieldByName("Data")
	if !dataField.IsValid() {
		return nil
	}

	stateUpdatesField := resultVal.FieldByName("StateUpdates")
	hasStateUpdates := stateUpdatesField.IsValid() && !stateUpdatesField.IsNil()

	if !hasStateUpdates {
		return dataField.Interface()
	}

	dataInterface := dataField.Interface()
	stateUpdatesInterface := stateUpdatesField.Interface()

	merged := dataInterface
	if err := mergo.Merge(&merged, stateUpdatesInterface, mergo.WithOverride); err == nil {
		return merged
	}

	if jsonMerged := JSONMergeWithReplace(dataInterface, stateUpdatesInterface); jsonMerged != nil {
		return jsonMerged
	}

	return map[string]interface{}{
		"data":          dataInterface,
		"state_updates": stateUpdatesInterface,
	}
}

func JSONMergeWithReplace(existing, updates interface{}) interface{} {
	if existing == nil {
		return updates
	}
	if updates == nil {
		return existing
	}

	existingJSON, err := json.Marshal(existing)
	if err != nil {
		return nil
	}

	updatesJSON, err := json.Marshal(updates)
	if err != nil {
		return nil
	}

	var existingMap, updatesMap map[string]interface{}
	if err := json.Unmarshal(existingJSON, &existingMap); err != nil {
		return nil
	}

	if err := json.Unmarshal(updatesJSON, &updatesMap); err != nil {
		return nil
	}

	for key, value := range updatesMap {
		existingMap[key] = value
	}

	mergedJSON, err := json.Marshal(existingMap)
	if err != nil {
		return existingMap
	}

	originalType := reflect.TypeOf(existing)
	if originalType.Kind() == reflect.Ptr {
		originalType = originalType.Elem()
	}

	newVal := reflect.New(originalType)
	if err := json.Unmarshal(mergedJSON, newVal.Interface()); err == nil {
		if reflect.TypeOf(existing).Kind() == reflect.Ptr {
			return newVal.Interface()
		}
		return newVal.Elem().Interface()
	}

	return existingMap
}

func ExtractNextNodes(nodeResult interface{}) []ports.NextNode {
	resultVal := reflect.ValueOf(nodeResult)
	if resultVal.Kind() == reflect.Ptr {
		resultVal = resultVal.Elem()
	}

	nextNodesField := resultVal.FieldByName("NextNodes")
	if !nextNodesField.IsValid() {
		return nil
	}

	nextNodesInterface := nextNodesField.Interface()
	if nextNodesInterface == nil {
		return nil
	}

	nextNodesSlice := reflect.ValueOf(nextNodesInterface)
	if nextNodesSlice.Kind() != reflect.Slice {
		return nil
	}

	portsNextNodes := make([]ports.NextNode, nextNodesSlice.Len())
	for i := 0; i < nextNodesSlice.Len(); i++ {
		nextNodeVal := nextNodesSlice.Index(i)
		nodeNameField := nextNodeVal.FieldByName("NodeName")
		configField := nextNodeVal.FieldByName("Config")
		priorityField := nextNodeVal.FieldByName("Priority")
		delayField := nextNodeVal.FieldByName("Delay")

		var nodeName string
		var config interface{}
		var priority int
		var delay *time.Duration

		if nodeNameField.IsValid() {
			nodeName = nodeNameField.String()
		}
		if configField.IsValid() {
			config = configField.Interface()
		}
		if priorityField.IsValid() {
			priority = int(priorityField.Int())
		}
		if delayField.IsValid() && !delayField.IsNil() {
			delayPtr := delayField.Interface().(*time.Duration)
			delay = delayPtr
		}

		portsNextNodes[i] = ports.NextNode{
			NodeName: nodeName,
			Config:   config,
			Priority: priority,
			Delay:    delay,
		}
	}

	return portsNextNodes
}

func SafeTypeConversion(src interface{}, dst interface{}) error {
	if src == nil {
		return nil
	}

	srcVal := reflect.ValueOf(src)
	dstVal := reflect.ValueOf(dst)

	if dstVal.Kind() != reflect.Ptr {
		return domain.NewValidationError("destination", "must be a pointer")
	}

	dstElem := dstVal.Elem()
	if srcVal.Type() == dstElem.Type() {
		dstElem.Set(srcVal)
		return nil
	}

	return ConvertViaJSON(src, dst)
}

func ConvertViaJSON(src interface{}, dst interface{}) error {
	if src == nil {
		return nil
	}

	jsonData, err := json.Marshal(src)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to marshal source",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	err = json.Unmarshal(jsonData, dst)
	if err != nil {
		return domain.Error{
			Type:    domain.ErrorTypeInternal,
			Message: "failed to unmarshal to destination",
			Details: map[string]interface{}{
				"error": err.Error(),
			},
		}
	}

	return nil
}