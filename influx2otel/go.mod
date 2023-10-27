module github.com/influxdata/influxdb-observability/influx2otel

go 1.19

require (
	github.com/influxdata/influxdb-observability/common v0.5.8
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest v0.87.0
	github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil v0.87.0
	github.com/stretchr/testify v1.8.4
	go.opentelemetry.io/collector/pdata v1.0.0-rcv0016
	go.opentelemetry.io/collector/semconv v0.87.0
)

require (
	github.com/cespare/xxhash/v2 v2.2.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	go.uber.org/multierr v1.11.0 // indirect
	golang.org/x/net v0.17.0 // indirect
	golang.org/x/sys v0.13.0 // indirect
	golang.org/x/text v0.13.0 // indirect
	google.golang.org/genproto/googleapis/rpc v0.0.0-20230711160842-782d3b101e98 // indirect
	google.golang.org/grpc v1.58.3 // indirect
	google.golang.org/protobuf v1.31.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/influxdata/influxdb-observability/common => ../common
