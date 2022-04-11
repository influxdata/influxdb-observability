module tests

go 1.17

require (
	github.com/influxdata/influxdb-observability/common v0.2.16
	github.com/influxdata/line-protocol/v2 v2.2.1
	github.com/influxdata/telegraf v1.21.4
	github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbexporter v0.48.0
	github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension v0.48.0
	github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver v0.48.0
	github.com/stretchr/testify v1.7.1
	go.opentelemetry.io/collector v0.48.0
	go.opentelemetry.io/collector/model v0.48.0
	go.uber.org/zap v1.21.0
	google.golang.org/grpc v1.45.0
)

require (
	collectd.org v0.5.0 // indirect
	contrib.go.opencensus.io/exporter/prometheus v0.4.0 // indirect
	github.com/alecthomas/participle v0.4.1 // indirect
	github.com/alecthomas/units v0.0.0-20210208195552-ff826a37aa15 // indirect
	github.com/antchfx/jsonquery v1.1.5 // indirect
	github.com/antchfx/xmlquery v1.3.9 // indirect
	github.com/antchfx/xpath v1.2.0 // indirect
	github.com/armon/go-metrics v0.3.10 // indirect
	github.com/benbjohnson/clock v1.3.0 // indirect
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/caio/go-tdigest v3.1.0+incompatible // indirect
	github.com/cenkalti/backoff/v4 v4.1.2 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/coreos/go-semver v0.3.0 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/doclambda/protobufquery v0.0.0-20210317203640-88ffabe06a60 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/felixge/httpsnoop v1.0.2 // indirect
	github.com/frankban/quicktest v1.14.2 // indirect
	github.com/go-kit/log v0.1.0 // indirect
	github.com/go-logfmt/logfmt v0.5.0 // indirect
	github.com/go-logr/logr v1.2.3 // indirect
	github.com/go-logr/stdr v1.2.2 // indirect
	github.com/go-ole/go-ole v1.2.6 // indirect
	github.com/gobwas/glob v0.2.3 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/uuid v1.3.0 // indirect
	github.com/gosnmp/gosnmp v1.34.0 // indirect
	github.com/hashicorp/go-cleanhttp v0.5.2 // indirect
	github.com/hashicorp/go-immutable-radix v1.3.1 // indirect
	github.com/inconshreveable/mousetrap v1.0.0 // indirect
	github.com/influxdata/influxdb-observability/influx2otel v0.2.16 // indirect
	github.com/influxdata/influxdb-observability/otel2influx v0.2.16 // indirect
	github.com/influxdata/toml v0.0.0-20190415235208-270119a8ce65 // indirect
	github.com/jaegertracing/jaeger v1.32.0 // indirect
	github.com/jhump/protoreflect v1.8.3-0.20210616212123-6cc1efa697ca // indirect
	github.com/klauspost/compress v1.15.1 // indirect
	github.com/knadh/koanf v1.4.0 // indirect
	github.com/lufia/plan9stats v0.0.0-20211012122336-39d0f177ccd0 // indirect
	github.com/magiconair/properties v1.8.6 // indirect
	github.com/mattn/go-colorable v0.1.12 // indirect
	github.com/mattn/go-isatty v0.0.14 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.2-0.20181231171920-c182affec369 // indirect
	github.com/mitchellh/copystructure v1.2.0 // indirect
	github.com/mitchellh/mapstructure v1.4.3 // indirect
	github.com/mitchellh/reflectwalk v1.0.2 // indirect
	github.com/naoina/go-stringutil v0.1.0 // indirect
	github.com/open-telemetry/opentelemetry-collector-contrib/internal/common v0.48.0 // indirect
	github.com/philhofer/fwd v1.1.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/power-devops/perfstat v0.0.0-20210106213030-5aafc221ea8c // indirect
	github.com/prometheus/client_golang v1.12.1 // indirect
	github.com/prometheus/client_model v0.2.0 // indirect
	github.com/prometheus/common v0.32.1 // indirect
	github.com/prometheus/procfs v0.7.3 // indirect
	github.com/prometheus/prometheus v1.8.2-0.20210430082741-2a4b8e12bbf2 // indirect
	github.com/prometheus/statsd_exporter v0.21.0 // indirect
	github.com/rs/cors v1.8.2 // indirect
	github.com/shirou/gopsutil/v3 v3.22.3 // indirect
	github.com/sleepinggenius2/gosmi v0.4.4 // indirect
	github.com/spf13/cast v1.4.1 // indirect
	github.com/spf13/cobra v1.4.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/streadway/amqp v1.0.0 // indirect
	github.com/tidwall/gjson v1.10.2 // indirect
	github.com/tidwall/match v1.1.1 // indirect
	github.com/tidwall/pretty v1.2.0 // indirect
	github.com/tinylib/msgp v1.1.6 // indirect
	github.com/tklauser/go-sysconf v0.3.10 // indirect
	github.com/tklauser/numcpus v0.4.0 // indirect
	github.com/vjeantet/grok v1.0.1 // indirect
	github.com/wavefronthq/wavefront-sdk-go v0.9.10 // indirect
	github.com/yusufpapurcu/wmi v1.2.2 // indirect
	go.opencensus.io v0.23.0 // indirect
	go.opentelemetry.io/contrib/instrumentation/net/http/otelhttp v0.31.0 // indirect
	go.opentelemetry.io/contrib/zpages v0.31.0 // indirect
	go.opentelemetry.io/otel v1.6.1 // indirect
	go.opentelemetry.io/otel/exporters/prometheus v0.28.0 // indirect
	go.opentelemetry.io/otel/metric v0.28.0 // indirect
	go.opentelemetry.io/otel/sdk v1.6.1 // indirect
	go.opentelemetry.io/otel/sdk/metric v0.28.0 // indirect
	go.opentelemetry.io/otel/trace v1.6.1 // indirect
	go.uber.org/atomic v1.9.0 // indirect
	go.uber.org/multierr v1.8.0 // indirect
	golang.org/x/net v0.0.0-20220127200216-cd36cc0744dd // indirect
	golang.org/x/sys v0.0.0-20220128215802-99c3d69c2c27 // indirect
	golang.org/x/text v0.3.7 // indirect
	google.golang.org/genproto v0.0.0-20220207164111-0872dc986b00 // indirect
	google.golang.org/protobuf v1.28.0 // indirect
	gopkg.in/yaml.v2 v2.4.0 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)

replace (
	github.com/influxdata/influxdb-observability/common => ../common
	github.com/influxdata/influxdb-observability/influx2otel => ../influx2otel
	github.com/influxdata/influxdb-observability/otel2influx => ../otel2influx
	github.com/influxdata/telegraf => ../../telegraf
//github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbexporter => ../../opentelemetry-collector-contrib/exporter/influxdbexporter
//github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver => ../../opentelemetry-collector-contrib/receiver/influxdbreceiver
)
