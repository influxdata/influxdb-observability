# OpenTelemetry Collector Distribution

This module provides a custom otel distribution config.
To build:
```console
$ go install go.opentelemetry.io/collector/cmd/builder@v0.63.1
...
$ cd otelcol-influxdb
$ builder --config build.yml
builder --config build.yml
2022-10-30T23:10:50.992-0700    INFO    internal/command.go:125 OpenTelemetry Collector Builder {"version": "dev", "date": "unknown"}
2022-10-30T23:10:50.993-0700    INFO    internal/command.go:158 Using config file       {"path": "build.yml"}
2022-10-30T23:10:50.993-0700    INFO    builder/config.go:107   Using go        {"go-executable": "/Users/jacobmarble/.gvm/gos/go1.19.1/bin/go"}
2022-10-30T23:10:50.994-0700    INFO    builder/main.go:76      Sources created {"path": "./build"}
2022-10-30T23:10:53.209-0700    INFO    builder/main.go:118     Getting go modules
2022-10-30T23:10:53.986-0700    INFO    builder/main.go:87      Compiling
2022-10-30T23:11:00.744-0700    INFO    builder/main.go:99      Compiled        {"binary": "./build/otelcol-influxdb"}
$ ./build/otelcol-influxdb 
Error: at least one config flag must be provided
2022/10/30 23:11:23 collector server run finished with error: at least one config flag must be provided
$
```
