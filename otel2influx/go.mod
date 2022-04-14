module github.com/influxdata/influxdb-observability/otel2influx

go 1.17

require (
	github.com/influxdata/influxdb-observability/common v0.2.18
	github.com/stretchr/testify v1.7.1
	go.opentelemetry.io/collector/model v0.49.0
	go.opentelemetry.io/collector/pdata v0.49.0
)

require (
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/kr/text v0.2.0 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	gopkg.in/check.v1 v1.0.0-20201130134442-10cb98267c6c // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace github.com/influxdata/influxdb-observability/common => ../common
