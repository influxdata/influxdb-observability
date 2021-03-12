# InfluxDB Plugin for Jaeger

**This is experimental software**

> If you are looking for the legacy Jaeger plugin for InfluxDB v1/v2, see the [legacy repository/branch](https://github.com/influxdata/jaeger-influxdb/tree/legacy).

This storage plugin supports [InfluxDB/IOx](https://github.com/influxdata/influxdb_iox), a high-performance, scalable, time-series storage engine.
The plugin enables querying InfluxDB via the Jaeger UI.
The plugin does not support writes via the Jaeger Collector.
To write traces to InfluxDB/IOx, use [OpenTelemetry](https://github.com/influxdata/opentelemetry-collector-contrib/tree/influxdb).

## Docker
A Docker image exists at [jacobmarble/jaeger-query-influxdb:latest](https://hub.docker.com/r/jacobmarble/jaeger-query-influxdb).

## Build
Build the plugin with `go install`:

```
go install ./cmd/jaeger-influxdb/
```

## How it works

Jaeger supports plugins via a gRPC interface.
More at [Jaeger: Running with a plugin](https://github.com/jaegertracing/jaeger/tree/master/plugin/storage/grpc#running-with-a-plugin).

Using the env var `SPAN_STORAGE_TYPE=grpc-plugin` you can specify the storage type, in this case the `grpc-plugin`.

`jaeger-query` now respects two important flags:
- `--grpc-storage-plugin.binary` points to a gRPC plugin
- `--grpc-storage-plugin.configuration-file` points to a configuration file for the plugin.
  An example config file is [`config-example.yaml`](config-example.yaml).
