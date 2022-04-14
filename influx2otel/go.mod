module github.com/influxdata/influxdb-observability/influx2otel

go 1.17

require (
	github.com/influxdata/influxdb-observability/common v0.2.18
	github.com/stretchr/testify v1.7.1
	go.opentelemetry.io/collector/model v0.49.0
	go.opentelemetry.io/collector/pdata v0.49.0
)

require (
	github.com/davecgh/go-spew v1.1.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20200313102051-9f266ea9e77c // indirect
)

replace github.com/influxdata/influxdb-observability/common => ../common
