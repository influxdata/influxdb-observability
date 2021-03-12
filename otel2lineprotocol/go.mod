module github.com/influxdata/influxdb-observability/otel2lineprotocol

go 1.15

require (
	github.com/influxdata/line-protocol/v2 v2.0.0-20210312163703-69fb9462cb3c
	go.opentelemetry.io/proto/otlp v0.0.0-00010101000000-000000000000
)

replace go.opentelemetry.io/proto/otlp => github.com/open-telemetry/opentelemetry-proto-go/otlp v0.7.0
