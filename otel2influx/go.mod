module github.com/influxdata/influxdb-observability/otel2influx

go 1.15

require (
	github.com/stretchr/testify v1.5.1
	go.opentelemetry.io/proto/otlp v0.0.0-00010101000000-000000000000
)

replace go.opentelemetry.io/proto/otlp => github.com/open-telemetry/opentelemetry-proto-go/otlp v0.7.0
