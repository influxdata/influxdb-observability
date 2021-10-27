module tests

go 1.16

require (
	cloud.google.com/go/monitoring v1.1.0 // indirect
	github.com/influxdata/influxdb-observability/common v0.2.7
	github.com/influxdata/line-protocol/v2 v2.2.0
	github.com/influxdata/telegraf v1.20.0
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbexporter v0.37.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension v0.37.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver v0.37.0
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector v0.37.1-0.20211026180946-46c8e2290e45
	go.opentelemetry.io/collector/model v0.37.1-0.20211026180946-46c8e2290e45
	go.uber.org/zap v1.19.1
	google.golang.org/grpc v1.41.0
)

replace (
	github.com/influxdata/influxdb-observability/common => ../common
	github.com/influxdata/influxdb-observability/influx2otel => ../influx2otel
	github.com/influxdata/influxdb-observability/otel2influx => ../otel2influx
	github.com/influxdata/telegraf => ../../telegraf
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbexporter => ../../opentelemetry-collector-contrib/exporter/influxdbexporter
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver => ../../opentelemetry-collector-contrib/receiver/influxdbreceiver
)
