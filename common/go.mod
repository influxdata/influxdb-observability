module github.com/influxdata/influxdb-observability/common

go 1.15

require github.com/influxdata/influxdb-observability/otlp v0.0.0-20210605003714-a868e4b21ba8

replace (
	github.com/influxdata/influxdb-observability/otlp => ../otlp
)
