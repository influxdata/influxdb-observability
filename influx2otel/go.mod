module github.com/influxdata/influxdb-observability/influx2otel

go 1.16

require (
	github.com/influxdata/influxdb-observability/common v0.2.0
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector/model v0.30.2-0.20210719230137-809cae954ed3
)

replace github.com/influxdata/influxdb-observability/common => ../common
