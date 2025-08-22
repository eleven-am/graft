package api

import (
	"context"
	"encoding/json"
	"log/slog"
	"reflect"

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

func (c *GraftCluster) RegisterNode(node interface{}) error {
	adapter, err := NewNodeAdapter(node)
	if err != nil {
		return err
	}
	return c.internal.RegisterNode(adapter)
}

func (c *GraftCluster) StartWorkflow(trigger ports.WorkflowTrigger) error {
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
	originalNode interface{}
	nodeName     string
	configType   reflect.Type
	stateType    reflect.Type
	resultType   reflect.Type
	executeFunc  reflect.Value
	canStartFunc *reflect.Value
}

func NewNodeAdapter(node interface{}) (*NodeAdapter, error) {
	originalNodeType := reflect.TypeOf(node)

	getNameMethod, ok := originalNodeType.MethodByName("GetName")
	if !ok {
		return nil, domain.NewValidationError("node", "must implement GetName() string")
	}
	if getNameMethod.Type.NumIn() != 1 {
		return nil, domain.NewValidationError("GetName", "must take no parameters")
	}
	if getNameMethod.Type.NumOut() != 1 {
		return nil, domain.NewValidationError("GetName", "must return string")
	}

	executeMethod, ok := originalNodeType.MethodByName("Execute")
	if !ok {
		return nil, domain.NewValidationError("node", "must implement Execute method")
	}
	if executeMethod.Type.NumIn() > 4 {
		return nil, domain.NewValidationError("Execute", "can have at most 3 parameters (ctx, state, config)")
	}
	if executeMethod.Type.NumOut() != 2 {
		return nil, domain.NewValidationError("Execute", "must return (*NodeResult, error)")
	}

	var canStartMethod *reflect.Method
	if method, ok := originalNodeType.MethodByName("CanStart"); ok {
		if method.Type.NumIn() > 4 {
			return nil, domain.NewValidationError("CanStart", "can have at most 3 parameters (ctx, state, config)")
		}
		if method.Type.NumOut() != 1 {
			return nil, domain.NewValidationError("CanStart", "must return bool")
		}
		canStartMethod = &method
	} else {
	}

	configType, stateType, resultType := ExtractTypesFromExecute(node)

	adapter := &NodeAdapter{
		originalNode: node,
		nodeName:     ExtractNodeName(node),
		configType:   configType,
		stateType:    stateType,
		resultType:   resultType,
	}

	executeMethodValue := reflect.ValueOf(node).MethodByName("Execute")
	adapter.executeFunc = executeMethodValue

	if canStartMethod != nil {
		canStartMethodValue := reflect.ValueOf(node).MethodByName("CanStart")
		adapter.canStartFunc = &canStartMethodValue
	} else {
	}

	return adapter, nil
}

func (n *NodeAdapter) Execute(ctx context.Context, args ...interface{}) (*ports.NodeResult, error) {
	globalState := args[0]
	config := args[1]

	methodType := n.executeFunc.Type()
	numParams := methodType.NumIn()

	reflectArgs := []reflect.Value{reflect.ValueOf(ctx)}

	if numParams >= 2 {
		stateVal := reflect.New(n.stateType).Interface()
		if err := SafeTypeConversion(globalState, stateVal); err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "state type conversion failed",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}
		reflectArgs = append(reflectArgs, reflect.ValueOf(stateVal).Elem())
	}

	if numParams >= 3 {
		configVal := reflect.New(n.configType).Interface()
		if err := SafeTypeConversion(config, configVal); err != nil {
			return nil, domain.Error{
				Type:    domain.ErrorTypeInternal,
				Message: "config type conversion failed",
				Details: map[string]interface{}{
					"error": err.Error(),
				},
			}
		}
		reflectArgs = append(reflectArgs, reflect.ValueOf(configVal).Elem())
	}

	results := n.executeFunc.Call(reflectArgs)
	if len(results) != 2 {
		return nil, domain.NewValidationError("Execute method", "must return (*NodeResult, error) signature")
	}

	if !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	nodeResult, ok := results[0].Interface().(*ports.NodeResult)
	if !ok {
		return nil, domain.NewValidationError("Execute method", "must return *NodeResult as first value")
	}

	return nodeResult, nil
}

func (n *NodeAdapter) GetName() string {
	return n.nodeName
}

func (n *NodeAdapter) CanStart(ctx context.Context, args ...interface{}) bool {
	if n.canStartFunc == nil {
		return true
	}

	globalState := args[0]
	config := args[1]

	methodType := n.canStartFunc.Type()
	numParams := methodType.NumIn()

	reflectArgs := []reflect.Value{reflect.ValueOf(ctx)}

	if numParams >= 2 {
		stateVal := reflect.New(n.stateType).Interface()
		if err := SafeTypeConversion(globalState, stateVal); err != nil {
			return false
		}
		reflectArgs = append(reflectArgs, reflect.ValueOf(stateVal).Elem())
	}

	if numParams >= 3 {
		configVal := reflect.New(n.configType).Interface()
		if err := SafeTypeConversion(config, configVal); err != nil {
			return false
		}
		reflectArgs = append(reflectArgs, reflect.ValueOf(configVal).Elem())
	}

	results := n.canStartFunc.Call(reflectArgs)
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
