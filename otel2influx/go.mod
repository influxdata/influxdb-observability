module github.com/influxdata/influxdb-observability/otel2influx

go 1.16

require (
	github.com/influxdata/influxdb-observability/common v0.2.8
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector/model v0.38.1-0.20211103215828-cffbecb2ac9e
)

replace github.com/influxdata/influxdb-observability/common => ../common
