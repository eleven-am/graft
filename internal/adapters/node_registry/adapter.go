package node_registry

import (
	"context"
	"fmt"
	"reflect"

	"github.com/eleven-am/graft/internal/ports"
	json "github.com/goccy/go-json"
)

type NodeAdapter struct {
	originalNode interface{}
	nodeName     string

	getNameMethod  reflect.Value
	executeMethod  reflect.Value
	canStartMethod reflect.Value

	executeParamTypes []reflect.Type
	executeHasCtx     bool
	executeCtxIndex   int

	canStartParamTypes []reflect.Type
	canStartHasCtx     bool
	canStartCtxIndex   int

	hasCanStart bool
}

func NewNodeAdapter(node interface{}) (*NodeAdapter, error) {
	if node == nil {
		return nil, fmt.Errorf("node cannot be nil")
	}

	nodeValue := reflect.ValueOf(node)

	getNameMethod := nodeValue.MethodByName("GetName")
	if !getNameMethod.IsValid() {
		return nil, fmt.Errorf("node must have GetName() method")
	}

	getNameType := getNameMethod.Type()
	if getNameType.NumIn() != 0 {
		return nil, fmt.Errorf("GetName() must take no parameters")
	}
	if getNameType.NumOut() != 1 || getNameType.Out(0).Kind() != reflect.String {
		return nil, fmt.Errorf("GetName() must return exactly one string")
	}

	nameResults := getNameMethod.Call(nil)
	nodeName := nameResults[0].String()

	executeMethod := nodeValue.MethodByName("Execute")
	if !executeMethod.IsValid() {
		return nil, fmt.Errorf("node must have Execute() method")
	}

	executeType := executeMethod.Type()
	if executeType.NumIn() > 3 {
		return nil, fmt.Errorf("Execute() can have at most 3 parameters")
	}
	if executeType.NumOut() != 2 {
		return nil, fmt.Errorf("Execute() must return exactly 2 values")
	}

	if err := validateExecuteReturns(executeType); err != nil {
		return nil, fmt.Errorf("Execute() return validation failed: %w", err)
	}

	executeParamTypes, executeHasCtx, executeCtxIndex := extractParamTypes(executeType)

	var canStartParamTypes []reflect.Type
	var canStartHasCtx bool
	var canStartCtxIndex int
	hasCanStart := false

	canStartMethod := nodeValue.MethodByName("CanStart")
	if canStartMethod.IsValid() {
		hasCanStart = true
		canStartType := canStartMethod.Type()

		if canStartType.NumIn() > 3 {
			return nil, fmt.Errorf("CanStart() can have at most 3 parameters")
		}
		if canStartType.NumOut() != 1 || canStartType.Out(0).Kind() != reflect.Bool {
			return nil, fmt.Errorf("CanStart() must return exactly one bool")
		}

		canStartParamTypes, canStartHasCtx, canStartCtxIndex = extractParamTypes(canStartType)
	}

	return &NodeAdapter{
		originalNode:       node,
		nodeName:           nodeName,
		getNameMethod:      getNameMethod,
		executeMethod:      executeMethod,
		canStartMethod:     canStartMethod,
		executeParamTypes:  executeParamTypes,
		executeHasCtx:      executeHasCtx,
		executeCtxIndex:    executeCtxIndex,
		canStartParamTypes: canStartParamTypes,
		canStartHasCtx:     canStartHasCtx,
		canStartCtxIndex:   canStartCtxIndex,
		hasCanStart:        hasCanStart,
	}, nil
}

func (a *NodeAdapter) GetName() string {
	return a.nodeName
}

func (a *NodeAdapter) CanStart(ctx context.Context, state json.RawMessage, config json.RawMessage) bool {
	if !a.hasCanStart {
		return true
	}

	args, err := a.buildArguments(ctx, state, config, a.canStartParamTypes, a.canStartHasCtx, a.canStartCtxIndex)
	if err != nil {
		return false
	}

	results := a.canStartMethod.Call(args)
	return results[0].Bool()
}

func (a *NodeAdapter) Execute(ctx context.Context, state json.RawMessage, config json.RawMessage) (*ports.NodeResult, error) {
	args, err := a.buildArguments(ctx, state, config, a.executeParamTypes, a.executeHasCtx, a.executeCtxIndex)
	if err != nil {
		return nil, fmt.Errorf("failed to build arguments: %w", err)
	}

	results := a.executeMethod.Call(args)

	resultValue := results[0]
	errorValue := results[1]

	if !errorValue.IsNil() {
		return nil, errorValue.Interface().(error)
	}

	if resultValue.IsNil() {
		return &ports.NodeResult{}, nil
	}

	userResult := resultValue.Interface()

	if nodeResult, ok := userResult.(*ports.NodeResult); ok {
		return nodeResult, nil
	}

	resultType := resultValue.Type()
	resultVal := resultValue

	if resultType.Kind() == reflect.Ptr {
		resultVal = resultValue.Elem()
		resultType = resultType.Elem()
	}

	portResult := &ports.NodeResult{}

	if field := resultVal.FieldByName("GlobalState"); field.IsValid() {
		portResult.GlobalState = field.Interface()
	}

	if field := resultVal.FieldByName("NextNodes"); field.IsValid() {
		if nodes, ok := field.Interface().([]ports.NextNode); ok {
			portResult.NextNodes = nodes
		}
	}

	return portResult, nil
}

func (a *NodeAdapter) buildArguments(ctx context.Context, state json.RawMessage, config json.RawMessage,
	paramTypes []reflect.Type, hasCtx bool, ctxIndex int) ([]reflect.Value, error) {

	args := make([]reflect.Value, len(paramTypes))

	dataParamIndex := 0

	for i, paramType := range paramTypes {
		if hasCtx && i == ctxIndex {
			args[i] = reflect.ValueOf(ctx)
		} else {
			var dataToUnmarshal json.RawMessage

			if dataParamIndex == 0 {
				dataToUnmarshal = state
			} else if dataParamIndex == 1 {
				dataToUnmarshal = config
			} else {
				return nil, fmt.Errorf("unexpected parameter count")
			}
			dataParamIndex++

			ptr := reflect.New(paramType)

			if len(dataToUnmarshal) > 0 {
				if err := json.Unmarshal(dataToUnmarshal, ptr.Interface()); err != nil {
					args[i] = reflect.Zero(paramType)
				} else {
					args[i] = ptr.Elem()
				}
			} else {
				args[i] = reflect.Zero(paramType)
			}
		}
	}

	return args, nil
}

func validateExecuteReturns(methodType reflect.Type) error {
	firstReturn := methodType.Out(0)
	if firstReturn.Kind() != reflect.Ptr && firstReturn.Kind() != reflect.Interface {
		return fmt.Errorf("first return must be a pointer or interface")
	}

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if !methodType.Out(1).Implements(errorType) {
		return fmt.Errorf("second return must be error")
	}

	return nil
}

func extractParamTypes(methodType reflect.Type) ([]reflect.Type, bool, int) {
	numParams := methodType.NumIn()
	paramTypes := make([]reflect.Type, numParams)

	hasCtx := false
	ctxIndex := -1

	ctxType := reflect.TypeOf((*context.Context)(nil)).Elem()

	for i := 0; i < numParams; i++ {
		paramType := methodType.In(i)
		paramTypes[i] = paramType

		if paramType == ctxType {
			hasCtx = true
			ctxIndex = i
		}
	}

	return paramTypes, hasCtx, ctxIndex
}
