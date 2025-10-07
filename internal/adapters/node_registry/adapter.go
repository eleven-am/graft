package node_registry

import (
	"context"
	"fmt"
	"reflect"
	"strconv"

	"github.com/eleven-am/graft/internal/domain"
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

const nodeAdapterComponent = "node_registry.NodeAdapter"

func newNodeAdapterConfigError(message string, cause error, opts ...domain.ErrorOption) *domain.DomainError {
	merged := []domain.ErrorOption{domain.WithComponent(nodeAdapterComponent)}
	if len(opts) > 0 {
		merged = append(merged, opts...)
	}
	return domain.NewConfigurationError(message, cause, merged...)
}

func NewNodeAdapter(node interface{}) (*NodeAdapter, error) {
	if node == nil {
		return nil, newNodeAdapterConfigError("node cannot be nil", nil)
	}

	nodeValue := reflect.ValueOf(node)
	nodeType := nodeValue.Type()
	typeDetail := domain.WithContextDetail("node_type", nodeType.String())

	getNameMethod := nodeValue.MethodByName("GetName")
	if !getNameMethod.IsValid() {
		return nil, newNodeAdapterConfigError("node must have GetName() method", nil, typeDetail)
	}

	getNameType := getNameMethod.Type()
	if getNameType.NumIn() != 0 {
		return nil, newNodeAdapterConfigError("GetName() must take no parameters", nil, typeDetail)
	}
	if getNameType.NumOut() != 1 || getNameType.Out(0).Kind() != reflect.String {
		return nil, newNodeAdapterConfigError("GetName() must return exactly one string", nil, typeDetail)
	}

	nameResults := getNameMethod.Call(nil)
	nodeName := nameResults[0].String()
	nodeNameDetail := domain.WithContextDetail("node_name", nodeName)

	executeMethod := nodeValue.MethodByName("Execute")
	if !executeMethod.IsValid() {
		return nil, newNodeAdapterConfigError("node must have Execute() method", nil, typeDetail, nodeNameDetail)
	}

	executeType := executeMethod.Type()
	if executeType.NumIn() > 3 {
		return nil, newNodeAdapterConfigError("Execute() can have at most 3 parameters", nil, typeDetail, nodeNameDetail)
	}
	if executeType.NumOut() != 2 {
		return nil, newNodeAdapterConfigError("Execute() must return exactly 2 values", nil, typeDetail, nodeNameDetail)
	}

	if err := validateExecuteReturns(executeType, nodeName); err != nil {
		return nil, err
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
			return nil, newNodeAdapterConfigError("CanStart() can have at most 3 parameters", nil, typeDetail, nodeNameDetail)
		}
		if canStartType.NumOut() != 1 || canStartType.Out(0).Kind() != reflect.Bool {
			return nil, newNodeAdapterConfigError("CanStart() must return exactly one bool", nil, typeDetail, nodeNameDetail)
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
		return nil, newNodeAdapterConfigError(
			"failed to build arguments",
			err,
			domain.WithContextDetail("node_name", a.nodeName),
		)
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
		} else if field.Kind() == reflect.Slice {
			ln := field.Len()
			if ln > 0 {
				if field.Index(0).Kind() == reflect.String {
					converted := make([]ports.NextNode, 0, ln)
					for i := 0; i < ln; i++ {
						converted = append(converted, ports.NextNode{NodeName: field.Index(i).String()})
					}
					portResult.NextNodes = converted
				} else {
					converted := make([]ports.NextNode, 0, ln)
					for i := 0; i < ln; i++ {
						elem := field.Index(i)
						if elem.Kind() == reflect.Ptr {
							elem = elem.Elem()
						}
						if elem.Kind() != reflect.Struct {
							continue
						}
						nodeNameField := elem.FieldByName("NodeName")
						configField := elem.FieldByName("Config")
						if nodeNameField.IsValid() && nodeNameField.Kind() == reflect.String {
							nn := nodeNameField.String()
							var cfg interface{}
							if configField.IsValid() {
								cfg = configField.Interface()
							}
							converted = append(converted, ports.NextNode{NodeName: nn, Config: cfg})
						}
					}
					if len(converted) > 0 {
						portResult.NextNodes = converted
					}
				}
			}
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
			var source string

			if dataParamIndex == 0 {
				dataToUnmarshal = state
				source = "state"
			} else if dataParamIndex == 1 {
				dataToUnmarshal = config
				source = "config"
			} else {
				return nil, newNodeAdapterConfigError(
					"unexpected parameter count for node adapter",
					nil,
					domain.WithContextDetail("node_name", a.nodeName),
				)
			}
			dataParamIndex++

			ptr := reflect.New(paramType)

			if len(dataToUnmarshal) > 0 {
				if err := json.Unmarshal(dataToUnmarshal, ptr.Interface()); err != nil {
					return nil, newNodeAdapterConfigError(
						fmt.Sprintf("failed to decode %s parameter", source),
						err,
						domain.WithContextDetail("node_name", a.nodeName),
						domain.WithContextDetail("parameter_index", strconv.Itoa(i)),
						domain.WithContextDetail("parameter_source", source),
					)
				}
				args[i] = ptr.Elem()
			} else {
				args[i] = reflect.Zero(paramType)
			}
		}
	}

	return args, nil
}

func validateExecuteReturns(methodType reflect.Type, nodeName string) error {
	firstReturn := methodType.Out(0)
	signatureDetail := domain.WithContextDetail("method_signature", methodType.String())
	nodeNameDetail := domain.WithContextDetail("node_name", nodeName)

	if firstReturn.Kind() != reflect.Ptr && firstReturn.Kind() != reflect.Interface {
		return newNodeAdapterConfigError("Execute() first return must be a pointer or interface", nil, signatureDetail, nodeNameDetail)
	}

	errorType := reflect.TypeOf((*error)(nil)).Elem()
	if !methodType.Out(1).Implements(errorType) {
		return newNodeAdapterConfigError("Execute() second return must implement error", nil, signatureDetail, nodeNameDetail)
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
