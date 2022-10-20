# OpenTelemetry Collector Distribution

This module provides a custom otel distribution config.
To build:
```console
$ go install go.opentelemetry.io/collector/cmd/builder@v0.62.1
...
$ cd otelcol-influxdb
$ builder --config otelcol-builder.yml
2022-10-20T15:52:51.983-0700    INFO    internal/command.go:121 OpenTelemetry Collector Builder {"version": "dev", "date": "unknown"}
2022-10-20T15:52:51.984-0700    INFO    internal/command.go:154 Using config file       {"path": "otelcol-builder.yml"}
2022-10-20T15:52:51.984-0700    INFO    builder/config.go:103   Using go        {"go-executable": "/Users/jacobmarble/.gvm/gos/go1.19.1/bin/go"}
2022-10-20T15:52:51.985-0700    INFO    builder/main.go:76      Sources created {"path": "./build"}
2022-10-20T15:52:52.417-0700    INFO    builder/main.go:118     Getting go modules
2022-10-20T15:52:52.450-0700    INFO    builder/main.go:87      Compiling
2022-10-20T15:53:06.183-0700    INFO    builder/main.go:99      Compiled        {"binary": "./build/otelcol-influxdb"}
$ ./build/otelcol-influxdb 
Error: at least one config flag must be provided
2022/10/20 15:53:25 collector server run finished with error: at least one config flag must be provided
$
```
