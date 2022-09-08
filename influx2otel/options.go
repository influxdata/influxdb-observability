package influx2otel

import "github.com/influxdata/influxdb-observability/common"

type Option func(*LineProtocolToOtelMetrics)

func WithLogger(l common.Logger) Option {
	return func(lo *LineProtocolToOtelMetrics) {
		lo.logger = l
	}
}

func WithSeparator(s string) Option {
	return func(lo *LineProtocolToOtelMetrics) {
		lo.separator = s
	}
}
