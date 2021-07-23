module tests

go 1.16

require (
	github.com/containerd/containerd v1.5.2 // indirect
	github.com/influxdata/influxdb-observability/common v0.2.4
	github.com/influxdata/line-protocol/v2 v2.0.0-20210520103755-6551a972d603
	github.com/influxdata/telegraf v1.19.1
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbexporter v0.0.0-20210722210311-7d7c32c02db1
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver v0.0.0-20210722210311-7d7c32c02db1
	github.com/stretchr/testify v1.7.0
	go.opentelemetry.io/collector v0.30.2-0.20210722014926-f6364581235d
	go.opentelemetry.io/collector/model v0.30.2-0.20210722014926-f6364581235d
	go.uber.org/zap v1.18.1
	google.golang.org/grpc v1.39.0
)

replace (
	github.com/influxdata/influxdb-observability/common => ../common
	github.com/influxdata/influxdb-observability/influx2otel => ../influx2otel
	github.com/influxdata/influxdb-observability/otel2influx => ../otel2influx
	//github.com/influxdata/telegraf => ../../telegraf
	github.com/influxdata/telegraf => github.com/jacobmarble/telegraf v0.0.0-20210720181419-f4311457ad7e
	//github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbexporter => ../../opentelemetry-collector-contrib/exporter/influxdbexporter
	//github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver => ../../opentelemetry-collector-contrib/receiver/influxdbreceiver
	//go.opentelemetry.io/collector => ../../opentelemetry-collector
	//go.opentelemetry.io/collector/model => ../../opentelemetry-collector/model
	go.opentelemetry.io/collector => go.opentelemetry.io/collector v0.0.0-20210723184018-3b7d6ce4830c
	go.opentelemetry.io/collector/model => go.opentelemetry.io/collector/model v0.0.0-20210723184018-3b7d6ce4830c
)
