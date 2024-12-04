package tracing

import (
	"context"
	"errors"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"strconv"
	"time"

	"github.com/pingcap/tidb/pkg/util/logutil"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/baggage"
	"go.opentelemetry.io/otel/exporters/otlp/otlpmetric/otlpmetricgrpc"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracehttp"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutlog"
	"go.opentelemetry.io/otel/exporters/stdout/stdoutmetric"
	"go.opentelemetry.io/otel/exporters/stdout/stdouttrace"
	ometric "go.opentelemetry.io/otel/metric"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/log"
	"go.opentelemetry.io/otel/sdk/metric"
	"go.opentelemetry.io/otel/sdk/resource"
	"go.opentelemetry.io/otel/sdk/trace"
	otrace "go.opentelemetry.io/otel/trace"
	"go.uber.org/zap"
)

var requestHelloCounter ometric.Int64Counter

// SetupOTelSDK bootstraps the OpenTelemetry pipeline.
// If it does not return an error, make sure to call shutdown for proper cleanup.
func SetupOTelSDK(ctx context.Context) (shutdown func(context.Context) error, err error) {
	var shutdownFuncs []func(context.Context) error

	// shutdown calls cleanup functions registered via shutdownFuncs.
	// The errors from the calls are joined.
	// Each registered cleanup will be invoked once.
	shutdown = func(ctx context.Context) error {
		var err error
		for _, fn := range shutdownFuncs {
			err = errors.Join(err, fn(ctx))
		}
		shutdownFuncs = nil
		return err
	}

	// handleErr calls shutdown for cleanup and makes sure that all errors are returned.
	handleErr := func(inErr error) {
		err = errors.Join(inErr, shutdown(ctx))
	}

	// Set up propagator.
	prop := newPropagator()
	otel.SetTextMapPropagator(prop)

	// Set up trace provider.
	// tracerProvider, err := newTraceProvider()
	// tracerProvider, err := newJaegerTraceProvider(ctx)
	// if err != nil {
	// 	handleErr(err)
	// 	return
	// }
	// shutdownFuncs = append(shutdownFuncs, tracerProvider.Shutdown)
	// otel.SetTracerProvider(tracerProvider)

	// Set up meter provider.
	// meterProvider, err := newMeterProvider()
	meterProvider, err := newPrometheusMeterProvider()
	if err != nil {
		handleErr(err)
		return
	}
	shutdownFuncs = append(shutdownFuncs, meterProvider.Shutdown)
	otel.SetMeterProvider(meterProvider)

	meterName := "prometheus_testing"
	meter := otel.Meter(meterName)
	requestHelloCounter, err = meter.Int64Counter("requests_hello_total")
	if err != nil {
		handleErr(err)
		return
	}

	/*
		// Set up logger provider.
		loggerProvider, err := newLoggerProvider()
		if err != nil {
			handleErr(err)
			return
		}
		shutdownFuncs = append(shutdownFuncs, loggerProvider.Shutdown)
		global.SetLoggerProvider(loggerProvider)
	*/

	return
}

func newPropagator() propagation.TextMapPropagator {
	return propagation.NewCompositeTextMapPropagator(
		propagation.TraceContext{},
		propagation.Baggage{},
	)
}

func newMeterProvider() (*metric.MeterProvider, error) {
	metricExporter, err := stdoutmetric.New()
	if err != nil {
		return nil, err
	}

	meterProvider := metric.NewMeterProvider(
		metric.WithReader(metric.NewPeriodicReader(metricExporter,
			// Default is 1m. Set to 3s for demonstrative purposes.
			metric.WithInterval(3*time.Second))),
	)
	return meterProvider, nil
}

func newPrometheusMeterProvider() (*metric.MeterProvider, error) {
	metricExporter, err := otlpmetricgrpc.New(
		context.Background(),
		otlpmetricgrpc.WithInsecure(),
		otlpmetricgrpc.WithEndpoint("0.0.0.0:4317"))
	// metricExporter, err := prometheus.New()
	if err != nil {
		return nil, err
	}

	// return metric.NewMeterProvider(metric.WithReader(metricExporter)), nil

	return metric.NewMeterProvider(
		metric.WithReader(
			metric.NewPeriodicReader(
				metricExporter,
				// Default is 1m. Set to 3s for demonstrative purposes.
				metric.WithInterval(3*time.Second))),
	), nil
}

func newLoggerProvider() (*log.LoggerProvider, error) {
	logExporter, err := stdoutlog.New()
	if err != nil {
		return nil, err
	}

	loggerProvider := log.NewLoggerProvider(
		log.WithProcessor(log.NewBatchProcessor(logExporter)),
	)
	return loggerProvider, nil
}

func newTraceProvider() (*trace.TracerProvider, error) {
	traceExporter, err := stdouttrace.New(
		stdouttrace.WithPrettyPrint())
	if err != nil {
		return nil, err
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithBatcher(traceExporter,
			// Default is 5s. Set to 1s for demonstrative purposes.
			trace.WithBatchTimeout(time.Second)),
	)
	return traceProvider, nil
}

func newJaegerTraceProvider(ctx context.Context) (*trace.TracerProvider, error) {
	// Create an Exporter that connects to the local Jaeger using the HTTP protocol
	traceExporter, err := otlptracehttp.New(ctx,
		otlptracehttp.WithEndpoint("127.0.0.1:4318"),
		otlptracehttp.WithInsecure())
	if err != nil {
		return nil, err
	}

	// process info
	res, err := resource.New(ctx,
		resource.WithFromEnv(),
		resource.WithProcess(),
		resource.WithTelemetrySDK(),
		resource.WithHost(),
		// resource.WithAttributes(
		//	// 在可观测链路 OpenTelemetry 版后端显示的服务名称。
		//	semconv.ServiceNameKey.String(common.ClientServiceName),
		//	semconv.HostNameKey.String(common.ClientServiceHostName),
		// ),
	)
	if err != nil {
		return nil, err
	}

	traceProvider := trace.NewTracerProvider(
		trace.WithResource(res),
		trace.WithBatcher(traceExporter,
			trace.WithBatchTimeout(time.Second)),
	)
	return traceProvider, nil
}

func Rolldice(w http.ResponseWriter, r *http.Request) {
	name := "go.opentelemetry.io/otel/example/dice"
	tracer := otel.Tracer(name)

	// metric
	requestHelloCounter.Add(context.Background(), 1)

	method, _ := baggage.NewMember("method", "repl")
	client, _ := baggage.NewMember("client", "cli")
	bag, _ := baggage.New(method, client)
	defaultCtx := baggage.ContextWithBaggage(r.Context(), bag)
	_, span := tracer.Start(defaultCtx, "roll")
	defer span.End()

	roll := 1 + rand.Intn(6)

	var msg string
	if player := r.PathValue("player"); player != "" {
		msg = fmt.Sprintf("%s is rolling the dice", player)
	} else {
		msg = "Anonymous player is rolling the dice"
	}
	logutil.BgLogger().Warn("xxx ", zap.String("msg", msg))

	// logs info
	span.AddEvent("event msg",
		otrace.WithAttributes(
			attribute.String("event.value", "event val"),
			attribute.Int64Slice("event.values", []int64{1, 2, 3})))

	// tags info
	rollValueAttr := attribute.Int("roll.value", roll)
	testValueAttr := attribute.String("roll.test", "print string")
	span.SetAttributes(rollValueAttr, testValueAttr)

	resp := strconv.Itoa(roll) + "\n"
	if _, err := io.WriteString(w, resp); err != nil {
		logutil.BgLogger().Warn("Write failed ", zap.Error(err))
	}
}
