module github.com/influxdata/influxdb-observability/influx2otel

go 1.15

require (
	github.com/influxdata/influxdb-observability/common v0.0.0-20210503043157-6ea7daf489f3
	github.com/influxdata/influxdb-observability/otlp v0.0.0-20210503043157-6ea7daf489f3
	github.com/stretchr/testify v1.7.0
	golang.org/x/net v0.0.0-20201021035429-f5854403a974 // indirect
	golang.org/x/xerrors v0.0.0-20200804184101-5ec99f83aff1 // indirect
	google.golang.org/protobuf v1.26.0
)

replace (
	github.com/influxdata/influxdb-observability/common => ../common
	github.com/influxdata/influxdb-observability/otlp => ../otlp
)
