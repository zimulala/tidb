package tracing

import (
	"context"
	"fmt"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.opentelemetry.io/otel/codes"
	"go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/sdk/trace"
)

type TraceToMetricsProcessor struct {
	requestCount metric.Int64Counter
	latency      metric.Float64Histogram
	errorCount   metric.Int64Counter
}

func NewTraceToMetricsProcessor(meter metric.Meter) *TraceToMetricsProcessor {
	requestCount, _ := meter.Int64Counter("trace_request_count")
	latency, _ := meter.Float64Histogram("trace_request_latency")
	errorCount, _ := meter.Int64Counter("trace_error_count")

	return &TraceToMetricsProcessor{
		requestCount: requestCount,
		latency:      latency,
		errorCount:   errorCount,
	}
}

func (p *TraceToMetricsProcessor) OnStart(parent context.Context, s trace.ReadWriteSpan) {
}

func (p *TraceToMetricsProcessor) OnEnd(span trace.ReadOnlySpan) {
	ctx := context.Background()

	// get Span attributes
	duration := span.EndTime().Sub(span.StartTime()).Seconds()
	spanName := span.Name()

	// record metrics
	p.requestCount.Add(ctx, 1, metric.WithAttributes())
	p.latency.Record(ctx, duration, metric.WithAttributes())
	if span.Status().Code == codes.Error {
		p.errorCount.Add(ctx, 1, metric.WithAttributes())
	}

	logutil.BgLogger().Info(fmt.Sprintf("Processed Span: %s | Duration: %f | Code: %v", spanName, duration, span.Status().Code))
}

func (p *TraceToMetricsProcessor) ForceFlush(ctx context.Context) error {
	return nil
}

func (p *TraceToMetricsProcessor) Shutdown(ctx context.Context) error {
	return nil
}
