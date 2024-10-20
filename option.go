package pgxotel

import "go.opentelemetry.io/otel/metric"

type queryMeterOption func(*QueryMeter)

func WithQueryMeter(mp metric.MeterProvider) queryMeterOption {
	return func(m *QueryMeter) {
		m.mp = mp
	}
}
