module github.com/influxdata/influxdb-observability/jaeger-influxdb

go 1.20

require (
	github.com/apache/arrow-adbc/go/adbc v0.3.0
	github.com/golang/groupcache v0.0.0-20210331224755-41bb18bfe9da
	github.com/influxdata/influxdb-observability/common v0.3.15
	github.com/influxdata/line-protocol/v2 v2.2.1
	github.com/jaegertracing/jaeger v1.45.0
	github.com/mattn/go-isatty v0.0.18
	github.com/opentracing/opentracing-go v1.2.0
	github.com/spf13/cobra v1.7.0
	github.com/spf13/viper v1.15.0
	github.com/stretchr/testify v1.8.2
	go.opentelemetry.io/collector/pdata v1.0.0-rcv0012
	go.opentelemetry.io/collector/semconv v0.78.2
	go.uber.org/multierr v1.11.0
	go.uber.org/zap v1.24.0
	google.golang.org/grpc v1.55.0
)

require (
	github.com/andybalholm/brotli v1.0.4 // indirect
	github.com/apache/arrow/go/v12 v12.0.0-20230307201612-6fdf1e520a76 // indirect
	github.com/apache/thrift v0.18.1 // indirect
	github.com/bluele/gcache v0.0.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/fatih/color v1.13.0 // indirect
	github.com/fsnotify/fsnotify v1.6.0 // indirect
	github.com/goccy/go-json v0.10.0 // indirect
	github.com/gogo/protobuf v1.3.2 // indirect
	github.com/golang/protobuf v1.5.3 // indirect
	github.com/golang/snappy v0.0.4 // indirect
	github.com/google/flatbuffers v23.1.21+incompatible // indirect
	github.com/hashicorp/go-hclog v1.4.0 // indirect
	github.com/hashicorp/go-plugin v1.4.8 // indirect
	github.com/hashicorp/hcl v1.0.0 // indirect
	github.com/hashicorp/yamux v0.0.0-20190923154419-df201c70410d // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/json-iterator/go v1.1.12 // indirect
	github.com/klauspost/asmfmt v1.3.2 // indirect
	github.com/klauspost/compress v1.16.5 // indirect
	github.com/klauspost/cpuid/v2 v2.2.3 // indirect
	github.com/magiconair/properties v1.8.7 // indirect
	github.com/mattn/go-colorable v0.1.13 // indirect
	github.com/minio/asm2plan9s v0.0.0-20200509001527-cdd76441f9d8 // indirect
	github.com/minio/c2goasm v0.0.0-20190812172519-36a3d3bbc4f3 // indirect
	github.com/mitchellh/go-testing-interface v1.0.0 // indirect
	github.com/mitchellh/mapstructure v1.5.0 // indirect
	github.com/modern-go/concurrent v0.0.0-20180306012644-bacd9c7ef1dd // indirect
	github.com/modern-go/reflect2 v1.0.2 // indirect
	github.com/oklog/run v1.1.0 // indirect
	github.com/pelletier/go-toml/v2 v2.0.6 // indirect
	github.com/pierrec/lz4/v4 v4.1.17 // indirect
	github.com/pmezard/go-difflib v1.0.1-0.20181226105442-5d4384ee4fb2 // indirect
	github.com/spf13/afero v1.9.3 // indirect
	github.com/spf13/cast v1.5.0 // indirect
	github.com/spf13/jwalterweatherman v1.1.0 // indirect
	github.com/spf13/pflag v1.0.5 // indirect
	github.com/subosito/gotenv v1.4.2 // indirect
	github.com/uber/jaeger-client-go v2.30.0+incompatible // indirect
	github.com/uber/jaeger-lib v2.4.1+incompatible // indirect
	github.com/zeebo/xxh3 v1.0.2 // indirect
	go.uber.org/atomic v1.10.0 // indirect
	go.uber.org/goleak v1.2.0 // indirect
	golang.org/x/exp v0.0.0-20230206171751-46f607a40771 // indirect
	golang.org/x/mod v0.8.0 // indirect
	golang.org/x/net v0.10.0 // indirect
	golang.org/x/sync v0.1.0 // indirect
	golang.org/x/sys v0.8.0 // indirect
	golang.org/x/text v0.9.0 // indirect
	golang.org/x/tools v0.6.0 // indirect
	golang.org/x/xerrors v0.0.0-20220907171357-04be3eba64a2 // indirect
	google.golang.org/genproto v0.0.0-20230306155012-7f2fa6fef1f4 // indirect
	google.golang.org/protobuf v1.30.0 // indirect
	gopkg.in/ini.v1 v1.67.0 // indirect
	gopkg.in/yaml.v3 v3.0.1 // indirect
)

replace github.com/influxdata/influxdb-observability/common => ../common
