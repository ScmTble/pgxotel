package pgxotel

import (
	"context"
	"time"

	"github.com/jackc/pgx/v5"
	"go.opentelemetry.io/otel"
	"go.opentelemetry.io/otel/attribute"
	"go.opentelemetry.io/otel/metric"
)

type queryMeterKey struct{}

var (
	_ pgx.QueryTracer   = (*QueryMeter)(nil)
	_ pgx.BatchTracer   = (*QueryMeter)(nil)
	_ pgx.ConnectTracer = (*QueryMeter)(nil)
	_ pgx.PrepareTracer = (*QueryMeter)(nil)
)

type QueryMeter struct {
	latency metric.Int64Histogram
	mp      metric.MeterProvider
	next    Tracer
}

func NewQueryMeter(options ...queryMeterOption) (*QueryMeter, error) {
	q := &QueryMeter{}

	for _, option := range options {
		option(q)
	}

	if q.mp == nil {
		q.mp = otel.GetMeterProvider()
	}

	latency, err := q.mp.Meter("pgx-otel").Int64Histogram(
		"pgx.latency",
		metric.WithDescription("The latency of calls in milliseconds"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, err
	}
	q.latency = latency

	return q, nil
}

func (q *QueryMeter) TracePrepareStart(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareStartData) context.Context {
	startTime := time.Now()

	if q.next != nil {
		ctx = q.next.TracePrepareStart(ctx, conn, data)
	}

	return context.WithValue(ctx, queryMeterKey{}, startTime)
}

func (q *QueryMeter) TracePrepareEnd(ctx context.Context, conn *pgx.Conn, data pgx.TracePrepareEndData) {
	if q.next != nil {
		q.next.TracePrepareEnd(ctx, conn, data)
	}

	startTime, ok := ctx.Value(queryMeterKey{}).(time.Time)
	if !ok {
		return
	}

	latency := time.Since(startTime)
	q.latency.Record(ctx, latency.Milliseconds(), metric.WithAttributes(
		attribute.String("method", "prepare"),
	))
}

func (q *QueryMeter) TraceConnectStart(ctx context.Context, data pgx.TraceConnectStartData) context.Context {
	startTime := time.Now()

	if q.next != nil {
		ctx = q.next.TraceConnectStart(ctx, data)
	}

	return context.WithValue(ctx, queryMeterKey{}, startTime)
}

func (q *QueryMeter) TraceConnectEnd(ctx context.Context, data pgx.TraceConnectEndData) {
	if q.next != nil {
		q.next.TraceConnectEnd(ctx, data)
	}

	startTime, ok := ctx.Value(queryMeterKey{}).(time.Time)
	if !ok {
		return
	}

	latency := time.Since(startTime)
	q.latency.Record(ctx, latency.Milliseconds(), metric.WithAttributes(
		attribute.String("method", "connect"),
	))
}

func (q *QueryMeter) TraceBatchStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchStartData) context.Context {
	startTime := time.Now()

	if q.next != nil {
		ctx = q.next.TraceBatchStart(ctx, conn, data)
	}

	return context.WithValue(ctx, queryMeterKey{}, startTime)
}

func (q *QueryMeter) TraceBatchQuery(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchQueryData) {
	if q.next != nil {
		q.next.TraceBatchQuery(ctx, conn, data)
	}
}

func (q *QueryMeter) TraceBatchEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceBatchEndData) {
	if q.next != nil {
		q.next.TraceBatchEnd(ctx, conn, data)
	}

	startTime, ok := ctx.Value(queryMeterKey{}).(time.Time)
	if !ok {
		return
	}

	latency := time.Since(startTime)
	q.latency.Record(ctx, latency.Milliseconds(), metric.WithAttributes(
		attribute.String("method", "batch"),
	))
}

func (q *QueryMeter) TraceQueryStart(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryStartData) context.Context {
	startTime := time.Now()

	if q.next != nil {
		ctx = q.next.TraceQueryStart(ctx, conn, data)
	}

	return context.WithValue(ctx, queryMeterKey{}, startTime)
}

func (q *QueryMeter) TraceQueryEnd(ctx context.Context, conn *pgx.Conn, data pgx.TraceQueryEndData) {
	if q.next != nil {
		q.next.TraceQueryEnd(ctx, conn, data)
	}

	startTime, ok := ctx.Value(queryMeterKey{}).(time.Time)
	if !ok {
		return
	}

	latency := time.Since(startTime)
	q.latency.Record(ctx, latency.Milliseconds(), metric.WithAttributes(
		attribute.String("method", "query"),
	))
}

type Tracer interface {
	pgx.QueryTracer
	pgx.BatchTracer
	pgx.ConnectTracer
	pgx.PrepareTracer
}
