package connector_registry

import (
	"context"
	"fmt"
	"reflect"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
)

type ConnectorAdapter struct {
	original interface{}
	name     string

	getNameMethod reflect.Value
	startMethod   reflect.Value
	stopMethod    reflect.Value

	startHasCtx bool
	stopHasCtx  bool

	configType reflect.Type
}

const connectorAdapterComponent = "connector_registry.ConnectorAdapter"

func newConnectorAdapterError(message string, cause error, opts ...domain.ErrorOption) *domain.DomainError {
	merged := []domain.ErrorOption{domain.WithComponent(connectorAdapterComponent)}
	if len(opts) > 0 {
		merged = append(merged, opts...)
	}
	return domain.NewConfigurationError(message, cause, merged...)
}

func NewConnectorAdapter(connector interface{}) (*ConnectorAdapter, error) {
	if connector == nil {
		return nil, newConnectorAdapterError("connector cannot be nil", nil)
	}

	value := reflect.ValueOf(connector)
	typ := value.Type()
	typeDetail := domain.WithContextDetail("connector_type", typ.String())

	getNameMethod := value.MethodByName("GetName")
	if !getNameMethod.IsValid() {
		return nil, newConnectorAdapterError("connector must have GetName() method", nil, typeDetail)
	}
	if getNameMethod.Type().NumIn() != 0 {
		return nil, newConnectorAdapterError("GetName() must take no parameters", nil, typeDetail)
	}
	if getNameMethod.Type().NumOut() != 1 || getNameMethod.Type().Out(0).Kind() != reflect.String {
		return nil, newConnectorAdapterError("GetName() must return exactly one string", nil, typeDetail)
	}

	nameResults := getNameMethod.Call(nil)
	connectorName := nameResults[0].String()
	nameDetail := domain.WithContextDetail("connector_name", connectorName)

	startMethod := value.MethodByName("Start")
	if !startMethod.IsValid() {
		return nil, newConnectorAdapterError("connector must have Start() method", nil, typeDetail, nameDetail)
	}
	startHasCtx, startConfigType, err := validateLifecycleMethod(startMethod, "Start", typeDetail, nameDetail)
	if err != nil {
		return nil, err
	}

	stopMethod := value.MethodByName("Stop")
	if !stopMethod.IsValid() {
		return nil, newConnectorAdapterError("connector must have Stop() method", nil, typeDetail, nameDetail)
	}
	stopHasCtx, stopConfigType, err := validateLifecycleMethod(stopMethod, "Stop", typeDetail, nameDetail)
	if err != nil {
		return nil, err
	}

	if startConfigType != stopConfigType {
		return nil, newConnectorAdapterError(
			"Start() and Stop() must accept the same config type",
			nil,
			typeDetail,
			nameDetail,
		)
	}

	adapter := &ConnectorAdapter{
		original:      connector,
		name:          connectorName,
		getNameMethod: getNameMethod,
		startMethod:   startMethod,
		stopMethod:    stopMethod,
		startHasCtx:   startHasCtx,
		stopHasCtx:    stopHasCtx,
		configType:    startConfigType,
	}

	return adapter, nil
}

func validateLifecycleMethod(method reflect.Value, name string, opts ...domain.ErrorOption) (bool, reflect.Type, error) {
	methodType := method.Type()
	contextType := reflect.TypeOf((*context.Context)(nil)).Elem()
	if methodType.NumIn() < 1 {
		return false, nil, newConnectorAdapterError(
			fmt.Sprintf("%s() must accept at least the config parameter", name),
			nil,
			opts...,
		)
	}
	if methodType.NumIn() > 2 {
		return false, nil, newConnectorAdapterError(
			fmt.Sprintf("%s() can accept at most context and config", name),
			nil,
			opts...,
		)
	}

	if methodType.NumIn() == 1 && methodType.In(0) == contextType {
		return false, nil, newConnectorAdapterError(
			fmt.Sprintf("%s() must accept a config parameter", name),
			nil,
			opts...,
		)
	}

	if methodType.NumIn() == 2 {
		if methodType.In(0) != contextType {
			return false, nil, newConnectorAdapterError(
				fmt.Sprintf("%s() first parameter must be context.Context when two parameters are present", name),
				nil,
				opts...,
			)
		}
	}

	configIndex := methodType.NumIn() - 1
	configType := methodType.In(configIndex)
	connectorConfigType := reflect.TypeOf((*ports.ConnectorConfig)(nil)).Elem()
	if !configType.Implements(connectorConfigType) {
		return false, nil, newConnectorAdapterError(
			fmt.Sprintf("%s() config parameter must implement ports.ConnectorConfig", name),
			nil,
			opts...,
		)
	}

	if configType.Kind() != reflect.Ptr {
		return false, nil, newConnectorAdapterError(
			fmt.Sprintf("%s() config parameter must be a pointer type", name),
			nil,
			opts...,
		)
	}

	if configType.Elem().Kind() != reflect.Struct {
		return false, nil, newConnectorAdapterError(
			fmt.Sprintf("%s() config pointer must point to a struct", name),
			nil,
			opts...,
		)
	}

	if methodType.NumOut() > 1 {
		return false, nil, newConnectorAdapterError(
			fmt.Sprintf("%s() must return at most an error", name),
			nil,
			opts...,
		)
	}
	if methodType.NumOut() == 1 && methodType.Out(0) != reflect.TypeOf((*error)(nil)).Elem() {
		return false, nil, newConnectorAdapterError(
			fmt.Sprintf("%s() must return an error if it returns a value", name),
			nil,
			opts...,
		)
	}

	hasCtx := methodType.NumIn() == 2

	return hasCtx, configType, nil
}

func (a *ConnectorAdapter) GetName() string {
	return a.name
}

func (a *ConnectorAdapter) NewConfig() ports.ConnectorConfig {
	if a.configType == nil {
		return nil
	}
	instance := reflect.New(a.configType.Elem())
	if !instance.Type().Implements(reflect.TypeOf((*ports.ConnectorConfig)(nil)).Elem()) {
		return nil
	}
	return instance.Interface().(ports.ConnectorConfig)
}

func (a *ConnectorAdapter) Start(ctx context.Context, config ports.ConnectorConfig) error {
	args := a.buildArgs(ctx, config, a.startHasCtx)
	results := a.startMethod.Call(args)
	if len(results) == 0 {
		return nil
	}
	if results[0].IsNil() {
		return nil
	}
	return results[0].Interface().(error)
}

func (a *ConnectorAdapter) Stop(ctx context.Context, config ports.ConnectorConfig) error {
	args := a.buildArgs(ctx, config, a.stopHasCtx)
	results := a.stopMethod.Call(args)
	if len(results) == 0 {
		return nil
	}
	if results[0].IsNil() {
		return nil
	}
	return results[0].Interface().(error)
}

func (a *ConnectorAdapter) buildArgs(ctx context.Context, config ports.ConnectorConfig, includeCtx bool) []reflect.Value {
	args := make([]reflect.Value, 0, 2)
	if includeCtx {
		args = append(args, reflect.ValueOf(ctx))
	}
	args = append(args, reflect.ValueOf(config))
	return args
}
