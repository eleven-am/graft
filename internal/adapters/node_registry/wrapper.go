package node_registry

import (
	"context"
	"encoding/json"
	"reflect"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type NodeWrapper struct {
	originalNode interface{}
	nodeName     string
	configType   reflect.Type
	stateType    reflect.Type
	resultType   reflect.Type
	executeFunc  reflect.Value
	canStartFunc *reflect.Value
}

func NewNodeWrapper(node interface{}) (*NodeWrapper, error) {
	if node == nil {
		return nil, domain.ErrInvalidInput
	}

	originalNodeType := reflect.TypeOf(node)
	if originalNodeType == nil {
		return nil, domain.ErrInvalidInput
	}

	getNameMethod, ok := originalNodeType.MethodByName("GetName")
	if !ok {
		return nil, domain.ErrInvalidInput
	}
	if getNameMethod.Type.NumIn() != 1 {
		return nil, domain.ErrInvalidInput
	}
	if getNameMethod.Type.NumOut() != 1 {
		return nil, domain.ErrInvalidInput
	}

	executeMethod, ok := originalNodeType.MethodByName("Execute")
	if !ok {
		return nil, domain.ErrInvalidInput
	}
	if executeMethod.Type.NumIn() > 4 {
		return nil, domain.ErrInvalidInput
	}
	if executeMethod.Type.NumOut() != 2 {
		return nil, domain.ErrInvalidInput
	}

	var canStartMethod *reflect.Method
	if method, ok := originalNodeType.MethodByName("CanStart"); ok {
		if method.Type.NumIn() > 4 {
			return nil, domain.ErrInvalidInput
		}
		if method.Type.NumOut() != 1 {
			return nil, domain.ErrInvalidInput
		}
		canStartMethod = &method
	}

	configType, stateType, resultType := extractTypesFromExecute(node)

	wrapper := &NodeWrapper{
		originalNode: node,
		nodeName:     extractNodeName(node),
		configType:   configType,
		stateType:    stateType,
		resultType:   resultType,
	}

	executeMethodValue := reflect.ValueOf(node).MethodByName("Execute")
	wrapper.executeFunc = executeMethodValue

	if canStartMethod != nil {
		canStartMethodValue := reflect.ValueOf(node).Method(canStartMethod.Index)
		wrapper.canStartFunc = &canStartMethodValue
	}

	return wrapper, nil
}

func (nw *NodeWrapper) GetName() string {
	return nw.nodeName
}

func (nw *NodeWrapper) Execute(ctx context.Context, args ...interface{}) (*ports.NodeResult, error) {
	var globalStateRaw, configRaw json.RawMessage

	if len(args) > 0 {
		if rawMsg, ok := args[0].(json.RawMessage); ok {
			globalStateRaw = rawMsg
		} else if args[0] != nil {
			var err error
			globalStateRaw, err = json.Marshal(args[0])
			if err != nil {
				return nil, domain.NewDiscoveryError("node_wrapper", "marshal_global_state", err)
			}
		}
	}
	
	if len(args) > 1 {
		if rawMsg, ok := args[1].(json.RawMessage); ok {
			configRaw = rawMsg
		} else if args[1] != nil {
			var err error
			configRaw, err = json.Marshal(args[1])
			if err != nil {
				return nil, domain.NewDiscoveryError("node_wrapper", "marshal_config", err)
			}
		}
	}

	methodType := nw.executeFunc.Type()
	numParams := methodType.NumIn()

	callArgs := []reflect.Value{reflect.ValueOf(ctx)}

	if numParams >= 2 {
		stateVal := reflect.New(nw.stateType).Interface()
		if len(globalStateRaw) > 0 {
			if err := json.Unmarshal(globalStateRaw, stateVal); err != nil {
				return nil, domain.NewDiscoveryError("node_wrapper", "unmarshal_global_state", err)
			}
		}
		callArgs = append(callArgs, reflect.ValueOf(stateVal).Elem())
	}

	if numParams >= 3 {
		configVal := reflect.New(nw.configType).Interface()
		if len(configRaw) > 0 {
			if err := json.Unmarshal(configRaw, configVal); err != nil {
				return nil, domain.NewDiscoveryError("node_wrapper", "unmarshal_config", err)
			}
		}
		callArgs = append(callArgs, reflect.ValueOf(configVal).Elem())
	}

	results := nw.executeFunc.Call(callArgs)
	if len(results) != 2 {
		return nil, domain.ErrInvalidInput
	}

	if !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	nodeResult, ok := results[0].Interface().(*ports.NodeResult)
	if !ok {
		return nil, domain.ErrInvalidInput
	}

	return nodeResult, nil
}

func (nw *NodeWrapper) CanStart(ctx context.Context, args ...interface{}) bool {
	if nw.canStartFunc == nil {
		return true
	}

	var globalStateRaw, configRaw json.RawMessage

	if len(args) > 0 {
		if rawMsg, ok := args[0].(json.RawMessage); ok {
			globalStateRaw = rawMsg
		} else if args[0] != nil {
			var err error
			globalStateRaw, err = json.Marshal(args[0])
			if err != nil {
				return false
			}
		}
	}
	
	if len(args) > 1 {
		if rawMsg, ok := args[1].(json.RawMessage); ok {
			configRaw = rawMsg
		} else if args[1] != nil {
			var err error
			configRaw, err = json.Marshal(args[1])
			if err != nil {
				return false
			}
		}
	}

	methodType := nw.canStartFunc.Type()
	numParams := methodType.NumIn()

	callArgs := []reflect.Value{reflect.ValueOf(ctx)}

	if numParams >= 2 {
		stateVal := reflect.New(nw.stateType).Interface()
		if len(globalStateRaw) > 0 {
			if err := json.Unmarshal(globalStateRaw, stateVal); err != nil {
				return false
			}
		}
		callArgs = append(callArgs, reflect.ValueOf(stateVal).Elem())
	}

	if numParams >= 3 {
		configVal := reflect.New(nw.configType).Interface()
		if len(configRaw) > 0 {
			if err := json.Unmarshal(configRaw, configVal); err != nil {
				return false
			}
		}
		callArgs = append(callArgs, reflect.ValueOf(configVal).Elem())
	}

	results := nw.canStartFunc.Call(callArgs)
	if len(results) != 1 {
		return false
	}

	if canStart, ok := results[0].Interface().(bool); ok {
		return canStart
	}

	return false
}

func extractNodeName(node interface{}) string {
	nameMethod := reflect.ValueOf(node).MethodByName("GetName")
	if !nameMethod.IsValid() {
		return ""
	}

	results := nameMethod.Call([]reflect.Value{})
	if len(results) != 1 {
		return ""
	}

	if name, ok := results[0].Interface().(string); ok {
		return name
	}

	return ""
}

func extractTypesFromExecute(node interface{}) (configType, stateType, resultType reflect.Type) {
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

