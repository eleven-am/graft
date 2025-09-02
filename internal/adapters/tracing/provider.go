package tracing

import (
	"context"
	"fmt"
	"log/slog"
	"sync"
	"time"

	"github.com/eleven-am/graft/internal/domain"
	"github.com/eleven-am/graft/internal/ports"
	"github.com/google/uuid"
)

type TracingProvider struct {
	mu      sync.RWMutex
	config  domain.TracingConfig
	logger  *slog.Logger
	tracers map[string]ports.Tracer
	active  bool
	spans   map[string]*spanImpl
	metrics TracingMetrics
}

type TracingMetrics struct {
	SpansCreated  int64 `json:"spans_created"`
	SpansFinished int64 `json:"spans_finished"`
	TracesActive  int64 `json:"traces_active"`
	SpansDropped  int64 `json:"spans_dropped"`
}

func NewTracingProvider(config domain.TracingConfig, logger *slog.Logger) *TracingProvider {
	return &TracingProvider{
		config:  config,
		logger:  logger,
		tracers: make(map[string]ports.Tracer),
		spans:   make(map[string]*spanImpl),
		active:  config.Enabled,
		metrics: TracingMetrics{},
	}
}

func (tp *TracingProvider) GetTracer(name string) ports.Tracer {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if tracer, exists := tp.tracers[name]; exists {
		return tracer
	}

	tracer := &tracerImpl{
		name:     name,
		provider: tp,
		logger:   tp.logger,
	}

	tp.tracers[name] = tracer
	return tracer
}

func (tp *TracingProvider) Shutdown(ctx context.Context) error {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	tp.logger.Info("shutting down tracing provider")

	for _, span := range tp.spans {
		span.Finish()
	}

	tp.active = false
	tp.tracers = make(map[string]ports.Tracer)
	tp.spans = make(map[string]*spanImpl)

	return nil
}

func (tp *TracingProvider) ForceFlush(ctx context.Context) error {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	if !tp.active {
		return nil
	}

	tp.logger.Debug("force flushing traces", "active_spans", len(tp.spans))
	return nil
}

func (tp *TracingProvider) GetMetrics() TracingMetrics {
	tp.mu.RLock()
	defer tp.mu.RUnlock()

	metrics := tp.metrics
	metrics.TracesActive = int64(len(tp.spans))
	return metrics
}

func (tp *TracingProvider) shouldSample() bool {
	if !tp.active {
		return false
	}
	return tp.config.SamplingRate > 0
}

func (tp *TracingProvider) addSpan(span *spanImpl) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	if len(tp.spans) >= tp.config.MaxSpansPerTrace {
		tp.metrics.SpansDropped++
		return
	}

	tp.spans[span.spanID] = span
	tp.metrics.SpansCreated++
}

func (tp *TracingProvider) removeSpan(spanID string) {
	tp.mu.Lock()
	defer tp.mu.Unlock()

	delete(tp.spans, spanID)
	tp.metrics.SpansFinished++
}

type tracerImpl struct {
	name     string
	provider *TracingProvider
	logger   *slog.Logger
}

func (t *tracerImpl) StartSpan(operationName string) ports.Span {
	return t.StartSpanWithParent(operationName, nil)
}

func (t *tracerImpl) StartSpanWithParent(operationName string, parent ports.SpanContext) ports.Span {
	if !t.provider.shouldSample() {
		return &noopSpan{}
	}

	span := &spanImpl{
		traceID:       generateTraceID(parent),
		spanID:        generateSpanID(),
		operationName: operationName,
		startTime:     time.Now(),
		tags:          make(map[string]interface{}),
		events:        make([]SpanEvent, 0),
		provider:      t.provider,
		tracer:        t,
		logger:        t.logger,
	}

	if parent != nil {
		span.parentSpanID = parent.SpanID()
		span.traceID = parent.TraceID()
	}

	t.provider.addSpan(span)

	t.logger.Debug("started span",
		"operation", operationName,
		"trace_id", span.traceID,
		"span_id", span.spanID,
		"parent_span_id", span.parentSpanID,
	)

	return span
}

func (t *tracerImpl) InjectContext(ctx context.Context, span ports.SpanContext) context.Context {
	if span == nil {
		return ctx
	}
	return context.WithValue(ctx, "trace_context", span)
}

func (t *tracerImpl) ExtractContext(ctx context.Context) (ports.SpanContext, bool) {
	if span, ok := ctx.Value("trace_context").(ports.SpanContext); ok {
		return span, true
	}
	return nil, false
}

type SpanEvent struct {
	Name       string                 `json:"name"`
	Timestamp  time.Time              `json:"timestamp"`
	Attributes map[string]interface{} `json:"attributes"`
}

type spanImpl struct {
	mu            sync.RWMutex
	traceID       string
	spanID        string
	parentSpanID  string
	operationName string
	startTime     time.Time
	endTime       time.Time
	tags          map[string]interface{}
	events        []SpanEvent
	finished      bool
	provider      *TracingProvider
	tracer        *tracerImpl
	logger        *slog.Logger
}

func (s *spanImpl) SetTag(key string, value interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.finished {
		return
	}

	s.tags[key] = value
}

func (s *spanImpl) SetError(err error) {
	if err == nil {
		return
	}

	s.SetTag("error", true)
	s.SetTag("error.message", err.Error())
	s.AddEvent("error", map[string]interface{}{
		"error.type":    fmt.Sprintf("%T", err),
		"error.message": err.Error(),
	})
}

func (s *spanImpl) AddEvent(name string, attributes map[string]interface{}) {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.finished {
		return
	}

	event := SpanEvent{
		Name:       name,
		Timestamp:  time.Now(),
		Attributes: attributes,
	}

	s.events = append(s.events, event)
}

func (s *spanImpl) Finish() {
	s.mu.Lock()
	defer s.mu.Unlock()

	if s.finished {
		return
	}

	s.endTime = time.Now()
	s.finished = true

	duration := s.endTime.Sub(s.startTime)

	s.logger.Debug("finished span",
		"operation", s.operationName,
		"trace_id", s.traceID,
		"span_id", s.spanID,
		"duration", duration,
		"tags", len(s.tags),
		"events", len(s.events),
	)

	s.provider.removeSpan(s.spanID)
}

func (s *spanImpl) Context() ports.SpanContext {
	return &spanContextImpl{
		traceID: s.traceID,
		spanID:  s.spanID,
	}
}

type spanContextImpl struct {
	traceID string
	spanID  string
}

func (sc *spanContextImpl) TraceID() string {
	return sc.traceID
}

func (sc *spanContextImpl) SpanID() string {
	return sc.spanID
}

func (sc *spanContextImpl) TraceFlags() byte {
	return 1
}

func (sc *spanContextImpl) TraceState() string {
	return ""
}

type noopSpan struct{}

func (ns *noopSpan) SetTag(key string, value interface{})                    {}
func (ns *noopSpan) SetError(err error)                                      {}
func (ns *noopSpan) AddEvent(name string, attributes map[string]interface{}) {}
func (ns *noopSpan) Finish()                                                 {}
func (ns *noopSpan) Context() ports.SpanContext                              { return nil }

func generateTraceID(parent ports.SpanContext) string {
	if parent != nil {
		return parent.TraceID()
	}
	return uuid.New().String()
}

func generateSpanID() string {
	return uuid.New().String()[:16]
}
