module github.com/influxdata/influxdb-observability/otel2influx

go 1.15

require (
	github.com/influxdata/influxdb-observability/common v0.0.0-20210428032116-36c28f2d40c7
	github.com/influxdata/influxdb-observability/otlp v0.0.0-20210428032116-36c28f2d40c7
	github.com/stretchr/testify v1.7.0
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/protobuf v1.26.0
)

replace (
	github.com/influxdata/influxdb-observability/common => ../common
	github.com/influxdata/influxdb-observability/otlp => ../otlp
)
