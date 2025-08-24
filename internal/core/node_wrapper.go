package core

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
		return nil, domain.NewConfigError("node", domain.ErrInvalidInput)
	}

	originalNodeType := reflect.TypeOf(node)
	if originalNodeType == nil {
		return nil, domain.NewConfigError("node", domain.ErrInvalidInput)
	}

	getNameMethod, ok := originalNodeType.MethodByName("GetName")
	if !ok {
		return nil, domain.NewConfigError("node", domain.ErrInvalidInput)
	}
	if getNameMethod.Type.NumIn() != 1 {
		return nil, domain.NewConfigError("GetName", domain.ErrInvalidInput)
	}
	if getNameMethod.Type.NumOut() != 1 {
		return nil, domain.NewConfigError("GetName", domain.ErrInvalidInput)
	}

	executeMethod, ok := originalNodeType.MethodByName("Execute")
	if !ok {
		return nil, domain.NewConfigError("node", domain.ErrInvalidInput)
	}
	if executeMethod.Type.NumIn() > 4 {
		return nil, domain.NewConfigError("Execute", domain.ErrInvalidInput)
	}
	if executeMethod.Type.NumOut() != 2 {
		return nil, domain.NewConfigError("Execute", domain.ErrInvalidInput)
	}

	var canStartMethod *reflect.Method
	if method, ok := originalNodeType.MethodByName("CanStart"); ok {
		if method.Type.NumIn() > 4 {
			return nil, domain.NewConfigError("CanStart", domain.ErrInvalidInput)
		}
		if method.Type.NumOut() != 1 {
			return nil, domain.NewConfigError("CanStart", domain.ErrInvalidInput)
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
	var globalState, config interface{}

	if len(args) > 0 {
		globalState = args[0]
	}
	if len(args) > 1 {
		config = args[1]
	}

	methodType := nw.executeFunc.Type()
	numParams := methodType.NumIn()

	callArgs := []reflect.Value{reflect.ValueOf(ctx)}

	if numParams >= 2 {
		stateVal := reflect.New(nw.stateType).Interface()
		if err := SafeTypeConversion(globalState, stateVal); err != nil {
			return nil, domain.NewNodeError(nw.nodeName, "state_conversion", err)
		}
		callArgs = append(callArgs, reflect.ValueOf(stateVal).Elem())
	}

	if numParams >= 3 {
		configVal := reflect.New(nw.configType).Interface()
		if err := SafeTypeConversion(config, configVal); err != nil {
			return nil, domain.NewNodeError(nw.nodeName, "config_conversion", err)
		}
		callArgs = append(callArgs, reflect.ValueOf(configVal).Elem())
	}

	results := nw.executeFunc.Call(callArgs)
	if len(results) != 2 {
		return nil, domain.NewNodeError(nw.nodeName, "execute", domain.ErrInternalError)
	}

	if !results[1].IsNil() {
		return nil, results[1].Interface().(error)
	}

	nodeResult, ok := results[0].Interface().(*ports.NodeResult)
	if !ok {
		return nil, domain.NewNodeError(nw.nodeName, "execute", domain.ErrInternalError)
	}

	return nodeResult, nil
}

func (nw *NodeWrapper) CanStart(ctx context.Context, args ...interface{}) bool {
	if nw.canStartFunc == nil {
		return true
	}

	var globalState, config interface{}

	if len(args) > 0 {
		globalState = args[0]
	}
	if len(args) > 1 {
		config = args[1]
	}

	methodType := nw.canStartFunc.Type()
	numParams := methodType.NumIn()

	callArgs := []reflect.Value{reflect.ValueOf(ctx)}

	if numParams >= 2 {
		stateVal := reflect.New(nw.stateType).Interface()
		if err := SafeTypeConversion(globalState, stateVal); err != nil {
			return false
		}
		callArgs = append(callArgs, reflect.ValueOf(stateVal).Elem())
	}

	if numParams >= 3 {
		configVal := reflect.New(nw.configType).Interface()
		if err := SafeTypeConversion(config, configVal); err != nil {
			return false
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

func SafeTypeConversion(src interface{}, dst interface{}) error {
	if src == nil {
		return nil
	}

	srcVal := reflect.ValueOf(src)
	dstVal := reflect.ValueOf(dst)

	if dstVal.Kind() != reflect.Ptr {
		return domain.NewConfigError("destination", domain.ErrInvalidInput)
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
		return domain.NewResourceError("json", "marshal", err)
	}

	err = json.Unmarshal(jsonData, dst)
	if err != nil {
		return domain.NewResourceError("json", "unmarshal", err)
	}

	return nil
}
