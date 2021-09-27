module github.com/influxdata/influxdb-observability/influx2otel

go 1.16

require (
	github.com/influxdata/influxdb-observability/common v0.2.7
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector/model v0.36.1-0.20210927193005-ebb0fbd6f23e
)

replace github.com/influxdata/influxdb-observability/common => ../common
