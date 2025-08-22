package ports

import (
	"context"
	"log/slog"
	"time"
)

type LogLevel string

const (
	LogLevelDebug LogLevel = "debug"
	LogLevelInfo  LogLevel = "info" 
	LogLevelWarn  LogLevel = "warn"
	LogLevelError LogLevel = "error"
)

type StructuredLogger struct {
	logger    *slog.Logger
	component string
	version   string
	nodeID    string
	baseAttrs []slog.Attr
}

func NewStructuredLogger(logger *slog.Logger, component, version, nodeID string) *StructuredLogger {
	baseAttrs := []slog.Attr{
		slog.String("component", component),
		slog.String("version", version),
		slog.String("node_id", nodeID),
		slog.Time("timestamp", time.Now()),
	}
	
	return &StructuredLogger{
		logger:    logger,
		component: component,
		version:   version,
		nodeID:    nodeID,
		baseAttrs: baseAttrs,
	}
}

func (sl *StructuredLogger) WithContext(ctx context.Context) *ContextLogger {
	return &ContextLogger{
		logger: sl,
		ctx:    ctx,
	}
}

func (sl *StructuredLogger) WithWorkflow(workflowID, nodeType string) *WorkflowLogger {
	return &WorkflowLogger{
		logger:     sl,
		workflowID: workflowID,
		nodeType:   nodeType,
	}
}

func (sl *StructuredLogger) WithOperation(operation, requestID string) *OperationLogger {
	return &OperationLogger{
		logger:    sl,
		operation: operation,
		requestID: requestID,
		startTime: time.Now(),
	}
}

func (sl *StructuredLogger) Debug(msg string, args ...interface{}) {
	attrs := append(sl.baseAttrs, sl.convertArgs(args...)...)
	sl.logger.LogAttrs(context.Background(), slog.LevelDebug, msg, attrs...)
}

func (sl *StructuredLogger) Info(msg string, args ...interface{}) {
	attrs := append(sl.baseAttrs, sl.convertArgs(args...)...)
	sl.logger.LogAttrs(context.Background(), slog.LevelInfo, msg, attrs...)
}

func (sl *StructuredLogger) Warn(msg string, args ...interface{}) {
	attrs := append(sl.baseAttrs, sl.convertArgs(args...)...)
	sl.logger.LogAttrs(context.Background(), slog.LevelWarn, msg, attrs...)
}

func (sl *StructuredLogger) Error(msg string, args ...interface{}) {
	attrs := append(sl.baseAttrs, sl.convertArgs(args...)...)
	sl.logger.LogAttrs(context.Background(), slog.LevelError, msg, attrs...)
}

func (sl *StructuredLogger) convertArgs(args ...interface{}) []slog.Attr {
	attrs := make([]slog.Attr, 0, len(args)/2)
	
	for i := 0; i < len(args)-1; i += 2 {
		key, ok := args[i].(string)
		if !ok {
			continue
		}
		
		value := args[i+1]
		switch v := value.(type) {
		case string:
			attrs = append(attrs, slog.String(key, v))
		case int:
			attrs = append(attrs, slog.Int(key, v))
		case int64:
			attrs = append(attrs, slog.Int64(key, v))
		case bool:
			attrs = append(attrs, slog.Bool(key, v))
		case time.Duration:
			attrs = append(attrs, slog.Duration(key, v))
		case time.Time:
			attrs = append(attrs, slog.Time(key, v))
		case error:
			attrs = append(attrs, slog.String(key, v.Error()))
		default:
			attrs = append(attrs, slog.Any(key, v))
		}
	}
	
	return attrs
}

type ContextLogger struct {
	logger *StructuredLogger
	ctx    context.Context
}

func (cl *ContextLogger) Debug(msg string, args ...interface{}) {
	cl.logger.Debug(msg, cl.addContextFields(args...)...)
}

func (cl *ContextLogger) Info(msg string, args ...interface{}) {
	cl.logger.Info(msg, cl.addContextFields(args...)...)
}

func (cl *ContextLogger) Warn(msg string, args ...interface{}) {
	cl.logger.Warn(msg, cl.addContextFields(args...)...)
}

func (cl *ContextLogger) Error(msg string, args ...interface{}) {
	cl.logger.Error(msg, cl.addContextFields(args...)...)
}

func (cl *ContextLogger) addContextFields(args ...interface{}) []interface{} {
	contextArgs := []interface{}{}
	
	if traceID, ok := cl.ctx.Value("trace_id").(string); ok {
		contextArgs = append(contextArgs, "trace_id", traceID)
	}
	
	if requestID, ok := cl.ctx.Value("request_id").(string); ok {
		contextArgs = append(contextArgs, "request_id", requestID)
	}
	
	if userID, ok := cl.ctx.Value("user_id").(string); ok {
		contextArgs = append(contextArgs, "user_id", userID)
	}
	
	return append(contextArgs, args...)
}

type WorkflowLogger struct {
	logger     *StructuredLogger
	workflowID string
	nodeType   string
}

func (wl *WorkflowLogger) Debug(msg string, args ...interface{}) {
	wl.logger.Debug(msg, wl.addWorkflowFields(args...)...)
}

func (wl *WorkflowLogger) Info(msg string, args ...interface{}) {
	wl.logger.Info(msg, wl.addWorkflowFields(args...)...)
}

func (wl *WorkflowLogger) Warn(msg string, args ...interface{}) {
	wl.logger.Warn(msg, wl.addWorkflowFields(args...)...)
}

func (wl *WorkflowLogger) Error(msg string, args ...interface{}) {
	wl.logger.Error(msg, wl.addWorkflowFields(args...)...)
}

func (wl *WorkflowLogger) addWorkflowFields(args ...interface{}) []interface{} {
	workflowArgs := []interface{}{
		"workflow_id", wl.workflowID,
		"node_type", wl.nodeType,
	}
	
	return append(workflowArgs, args...)
}

type OperationLogger struct {
	logger    *StructuredLogger
	operation string
	requestID string
	startTime time.Time
}

func (ol *OperationLogger) Debug(msg string, args ...interface{}) {
	ol.logger.Debug(msg, ol.addOperationFields(args...)...)
}

func (ol *OperationLogger) Info(msg string, args ...interface{}) {
	ol.logger.Info(msg, ol.addOperationFields(args...)...)
}

func (ol *OperationLogger) Warn(msg string, args ...interface{}) {
	ol.logger.Warn(msg, ol.addOperationFields(args...)...)
}

func (ol *OperationLogger) Error(msg string, args ...interface{}) {
	ol.logger.Error(msg, ol.addOperationFields(args...)...)
}

func (ol *OperationLogger) Complete(msg string, args ...interface{}) {
	duration := time.Since(ol.startTime)
	operationArgs := ol.addOperationFields(args...)
	operationArgs = append(operationArgs, "duration", duration, "status", "completed")
	ol.logger.Info(msg, operationArgs...)
}

func (ol *OperationLogger) Fail(msg string, err error, args ...interface{}) {
	duration := time.Since(ol.startTime)
	operationArgs := ol.addOperationFields(args...)
	operationArgs = append(operationArgs, "duration", duration, "status", "failed", "error", err)
	ol.logger.Error(msg, operationArgs...)
}

func (ol *OperationLogger) addOperationFields(args ...interface{}) []interface{} {
	duration := time.Since(ol.startTime)
	operationArgs := []interface{}{
		"operation", ol.operation,
		"request_id", ol.requestID,
		"elapsed", duration,
	}
	
	return append(operationArgs, args...)
}

func (sl *StructuredLogger) LogMetrics(name string, value interface{}, tags map[string]string) {
	args := []interface{}{
		"metric_name", name,
		"metric_value", value,
		"metric_type", "gauge",
	}
	
	for key, val := range tags {
		args = append(args, "tag_"+key, val)
	}
	
	sl.Info("metric recorded", args...)
}

func (sl *StructuredLogger) LogPerformance(operation string, duration time.Duration, success bool, details map[string]interface{}) {
	args := []interface{}{
		"performance_operation", operation,
		"duration", duration,
		"success", success,
		"duration_ms", float64(duration.Nanoseconds())/1e6,
	}
	
	for key, val := range details {
		args = append(args, "perf_"+key, val)
	}
	
	if success {
		sl.Info("performance metric", args...)
	} else {
		sl.Warn("performance metric - operation failed", args...)
	}
}

func (sl *StructuredLogger) LogBusinessEvent(event string, entity string, entityID string, details map[string]interface{}) {
	args := []interface{}{
		"event_type", event,
		"entity", entity,
		"entity_id", entityID,
		"business_event", true,
	}
	
	for key, val := range details {
		args = append(args, "event_"+key, val)
	}
	
	sl.Info("business event", args...)
}

func (sl *StructuredLogger) LogSecurity(eventType, actor, action, resource string, success bool, details map[string]interface{}) {
	args := []interface{}{
		"security_event", eventType,
		"actor", actor,
		"action", action,
		"resource", resource,
		"success", success,
		"security_audit", true,
	}
	
	for key, val := range details {
		args = append(args, "sec_"+key, val)
	}
	
	level := slog.LevelInfo
	if !success {
		level = slog.LevelWarn
	}
	
	attrs := append(sl.baseAttrs, sl.convertArgs(args...)...)
	sl.logger.LogAttrs(context.Background(), level, "security audit", attrs...)
}

const (
	FieldWorkflowID    = "workflow_id"
	FieldNodeID        = "node_id"
	FieldNodeType      = "node_type"
	FieldOperation     = "operation"
	FieldDuration      = "duration"
	FieldRequestID     = "request_id"
	FieldTraceID       = "trace_id"
	FieldUserID        = "user_id"
	FieldError         = "error"
	FieldStatus        = "status"
	FieldComponent     = "component"
	FieldVersion       = "version"
	FieldTimestamp     = "timestamp"
	FieldMetricName    = "metric_name"
	FieldMetricValue   = "metric_value"
	FieldBusinessEvent = "business_event"
	FieldSecurityAudit = "security_audit"
)