package tracing

import (
	"context"
	"errors"
	"log/slog"
	"testing"

	"github.com/eleven-am/graft/internal/domain"
)

func TestTracingProviderBasic(t *testing.T) {
	config := domain.TracingConfig{
		Enabled:          true,
		ServiceName:      "test-service",
		SamplingRate:     1.0,
		MaxSpansPerTrace: 100,
	}

	logger := slog.Default()
	provider := NewTracingProvider(config, logger)

	tracer := provider.GetTracer("test-tracer")
	if tracer == nil {
		t.Fatal("Expected tracer to be created")
	}

	same := provider.GetTracer("test-tracer")
	if tracer != same {
		t.Error("Expected same tracer instance for same name")
	}
}

func TestSpanLifecycle(t *testing.T) {
	config := domain.TracingConfig{
		Enabled:          true,
		ServiceName:      "test-service",
		SamplingRate:     1.0,
		MaxSpansPerTrace: 100,
	}

	logger := slog.Default()
	provider := NewTracingProvider(config, logger)
	tracer := provider.GetTracer("test")

	span := tracer.StartSpan("test-operation")
	if span == nil {
		t.Fatal("Expected span to be created")
	}

	span.SetTag("test.tag", "value")
	span.AddEvent("test-event", map[string]interface{}{
		"key": "value",
	})

	ctx := span.Context()
	if ctx == nil {
		t.Error("Expected span context to be available")
	}

	if ctx.TraceID() == "" {
		t.Error("Expected trace ID to be set")
	}

	if ctx.SpanID() == "" {
		t.Error("Expected span ID to be set")
	}

	span.Finish()

	metrics := provider.GetMetrics()
	if metrics.SpansCreated != 1 {
		t.Errorf("Expected 1 span created, got %d", metrics.SpansCreated)
	}

	if metrics.SpansFinished != 1 {
		t.Errorf("Expected 1 span finished, got %d", metrics.SpansFinished)
	}
}

func TestSpanWithParent(t *testing.T) {
	config := domain.TracingConfig{
		Enabled:          true,
		ServiceName:      "test-service",
		SamplingRate:     1.0,
		MaxSpansPerTrace: 100,
	}

	logger := slog.Default()
	provider := NewTracingProvider(config, logger)
	tracer := provider.GetTracer("test")

	parentSpan := tracer.StartSpan("parent-operation")
	parentCtx := parentSpan.Context()

	childSpan := tracer.StartSpanWithParent("child-operation", parentCtx)
	childCtx := childSpan.Context()

	if childCtx.TraceID() != parentCtx.TraceID() {
		t.Error("Expected child span to have same trace ID as parent")
	}

	if childCtx.SpanID() == parentCtx.SpanID() {
		t.Error("Expected child span to have different span ID from parent")
	}

	childSpan.Finish()
	parentSpan.Finish()
}

func TestContextInjectionExtraction(t *testing.T) {
	config := domain.TracingConfig{
		Enabled:          true,
		ServiceName:      "test-service",
		SamplingRate:     1.0,
		MaxSpansPerTrace: 100,
	}

	logger := slog.Default()
	provider := NewTracingProvider(config, logger)
	tracer := provider.GetTracer("test")

	span := tracer.StartSpan("test-operation")
	spanCtx := span.Context()

	ctx := context.Background()
	ctxWithSpan := tracer.InjectContext(ctx, spanCtx)

	extracted, found := tracer.ExtractContext(ctxWithSpan)
	if !found {
		t.Error("Expected to extract span context")
	}

	if extracted.TraceID() != spanCtx.TraceID() {
		t.Error("Expected extracted trace ID to match original")
	}

	if extracted.SpanID() != spanCtx.SpanID() {
		t.Error("Expected extracted span ID to match original")
	}

	span.Finish()
}

func TestSpanError(t *testing.T) {
	config := domain.TracingConfig{
		Enabled:          true,
		ServiceName:      "test-service",
		SamplingRate:     1.0,
		MaxSpansPerTrace: 100,
	}

	logger := slog.Default()
	provider := NewTracingProvider(config, logger)
	tracer := provider.GetTracer("test")

	span := tracer.StartSpan("test-operation")

	testErr := errors.New("test error")
	span.SetError(testErr)

	span.Finish()
}

func TestDisabledTracing(t *testing.T) {
	config := domain.TracingConfig{
		Enabled:          false,
		ServiceName:      "test-service",
		SamplingRate:     1.0,
		MaxSpansPerTrace: 100,
	}

	logger := slog.Default()
	provider := NewTracingProvider(config, logger)
	tracer := provider.GetTracer("test")

	span := tracer.StartSpan("test-operation")

	span.SetTag("test", "value")
	span.SetError(errors.New("test error"))
	span.AddEvent("test", nil)
	span.Finish()

	metrics := provider.GetMetrics()
	if metrics.SpansCreated != 0 {
		t.Errorf("Expected 0 spans created when disabled, got %d", metrics.SpansCreated)
	}
}

func TestMaxSpansLimit(t *testing.T) {
	config := domain.TracingConfig{
		Enabled:          true,
		ServiceName:      "test-service",
		SamplingRate:     1.0,
		MaxSpansPerTrace: 2,
	}

	logger := slog.Default()
	provider := NewTracingProvider(config, logger)
	tracer := provider.GetTracer("test")

	span1 := tracer.StartSpan("operation-1")
	span2 := tracer.StartSpan("operation-2")
	span3 := tracer.StartSpan("operation-3")

	metrics := provider.GetMetrics()
	if metrics.SpansCreated != 2 {
		t.Errorf("Expected 2 spans created (at limit), got %d", metrics.SpansCreated)
	}

	if metrics.SpansDropped != 1 {
		t.Errorf("Expected 1 span dropped, got %d", metrics.SpansDropped)
	}

	span1.Finish()
	span2.Finish()
	span3.Finish()
}

func TestProviderShutdown(t *testing.T) {
	config := domain.TracingConfig{
		Enabled:          true,
		ServiceName:      "test-service",
		SamplingRate:     1.0,
		MaxSpansPerTrace: 100,
	}

	logger := slog.Default()
	provider := NewTracingProvider(config, logger)
	tracer := provider.GetTracer("test")

	span1 := tracer.StartSpan("operation-1")
	span2 := tracer.StartSpan("operation-2")

	metrics := provider.GetMetrics()
	if metrics.SpansCreated != 2 {
		t.Errorf("Expected 2 spans created, got %d", metrics.SpansCreated)
	}

	if err := provider.Shutdown(); err != nil {
		t.Errorf("Expected shutdown to succeed: %v", err)
	}

	metrics = provider.GetMetrics()
	if metrics.TracesActive != 0 {
		t.Errorf("Expected 0 active traces after shutdown, got %d", metrics.TracesActive)
	}

	newSpan := tracer.StartSpan("should-be-noop")
	newSpan.Finish()

	_, _ = span1, span2
}

func TestForceFlush(t *testing.T) {
	config := domain.TracingConfig{
		Enabled:          true,
		ServiceName:      "test-service",
		SamplingRate:     1.0,
		MaxSpansPerTrace: 100,
	}

	logger := slog.Default()
	provider := NewTracingProvider(config, logger)
	tracer := provider.GetTracer("test")

	span := tracer.StartSpan("test-operation")

	if err := provider.ForceFlush(); err != nil {
		t.Errorf("Expected force flush to succeed: %v", err)
	}

	span.Finish()
}
