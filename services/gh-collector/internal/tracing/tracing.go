// Package tracing централизует установку OpenTelemetry TracerProvider для gh-collector.
// Обоснование выбора OTLP/gRPC + Jaeger, W3C traceparent в заголовках Kafka и того, почему это
// НЕ единое дерево span'ов от приёма события до ответа API, — docs/adr/0009-opentelemetry-tracing-jaeger.md.
package tracing

import (
	"context"
	"fmt"

	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/exporters/otlp/otlptrace/otlptracegrpc"
	"go.opentelemetry.io/otel/propagation"
	"go.opentelemetry.io/otel/sdk/resource"
	sdktrace "go.opentelemetry.io/otel/sdk/trace"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Setup строит TracerProvider с экспортом по OTLP/gRPC в endpoint (например "jaeger:4317" внутри
// docker-сети или "localhost:4317" для CLI, запущенного с хоста) и регистрирует его глобально
// вместе с W3C-пропагатором (propagation.TraceContext) — именно его читает pulse-consumer из
// заголовков Kafka-сообщения (ADR 0009), поэтому пропагатор здесь не опция, а часть контракта
// топика gh.events.
//
// Возвращает shutdown-функцию: вызывающий код (cmd/gh-collector) обязан вызвать её перед выходом
// (после Producer.Flush, а не вместо него) — иначе span'ы, ещё не сброшенные батчевым
// BatchSpanProcessor, потеряются вместе с процессом.
func Setup(ctx context.Context, serviceName, endpoint string) (shutdown func(context.Context) error, err error) {
	// grpc.NewClient (не устаревший grpc.Dial) резолвит цель лениво при первом вызове — на этом
	// шаге сеть ещё не используется, поэтому insecure-креды (Jaeger живёт только внутри docker-сети
	// ghpulse или на loopback хоста, TLS здесь не нужен) не означают, что соединение уже небезопасно
	// открыто наружу.
	conn, err := grpc.NewClient(endpoint, grpc.WithTransportCredentials(insecure.NewCredentials()))
	if err != nil {
		return nil, fmt.Errorf("tracing: dial otlp endpoint %q: %w", endpoint, err)
	}

	exporter, err := otlptracegrpc.New(ctx, otlptracegrpc.WithGRPCConn(conn))
	if err != nil {
		return nil, fmt.Errorf("tracing: build otlp exporter: %w", err)
	}

	res, err := resource.New(ctx, resource.WithAttributes(attribute.String("service.name", serviceName)))
	if err != nil {
		return nil, fmt.Errorf("tracing: build resource: %w", err)
	}

	tp := sdktrace.NewTracerProvider(sdktrace.WithBatcher(exporter), sdktrace.WithResource(res))
	otel.SetTracerProvider(tp)
	otel.SetTextMapPropagator(propagation.TraceContext{})

	return tp.Shutdown, nil
}
