module github.com/influxdata/influxdb-observability/influx2otel

go 1.16

require (
	github.com/influxdata/influxdb-observability/common v0.2.2
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector/model v0.0.0-20210723184018-3b7d6ce4830c
)

replace github.com/influxdata/influxdb-observability/common => ../common
