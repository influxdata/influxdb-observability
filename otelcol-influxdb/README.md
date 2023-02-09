# OpenTelemetry Collector, InfluxDB Distribution

**This is experimental software**

This directory contains tools to build an [OpenTelemetry Collector Distribution](https://opentelemetry.io/docs/concepts/distributions/) with the InfluxDB exporter, and little else.
Its purpose is to be a lightweight alternative to the [OpenTelemetry Collector-Contrib](https://github.com/open-telemetry/opentelemetry-collector-contrib/) Distribution, which includes the InfluxDB plugins, as well as many others.

## Docker
Docker images exist at [jacobmarble/otelcol-influxdb](https://hub.docker.com/r/jacobmarble/otelcol-influxdb).
For an example configuration, see [docker-compose.yml](../demo/docker-compose.yml).

## Build

```console
$ go install go.opentelemetry.io/collector/cmd/builder@latest
...
$ cd otelcol-influxdb
$ builder --config build.yml
...
$ ./build/otelcol-influxdb
...
```
