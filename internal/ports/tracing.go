package ports

import "context"

type Span interface {
	SetTag(key string, value interface{})
	SetError(err error)
	AddEvent(name string, attributes map[string]interface{})
	Finish()
	Context() SpanContext
}

type SpanContext interface {
	TraceID() string
	SpanID() string
	TraceFlags() byte
	TraceState() string
}

type Tracer interface {
	StartSpan(operationName string) Span
	StartSpanWithParent(operationName string, parent SpanContext) Span
	InjectContext(ctx context.Context, span SpanContext) context.Context
	ExtractContext(ctx context.Context) (SpanContext, bool)
}

type TracingProvider interface {
	GetTracer(name string) Tracer
	Shutdown() error
	ForceFlush() error
}
