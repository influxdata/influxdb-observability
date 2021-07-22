module github.com/influxdata/influxdb-observability/influx2otel

go 1.16

require (
	github.com/influxdata/influxdb-observability/common v0.2.2
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector/model v0.0.0-20210722215046-85b2ac1326b4
)

replace github.com/influxdata/influxdb-observability/common => ../common
