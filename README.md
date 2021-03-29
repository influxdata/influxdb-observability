# InfluxDB Observability

> This is experimental software

This repository is a reference for converting observability signals (traces, metrics, logs) to/from a common InfluxDB/IOx schema.

The [InfluxDB/IOx storage engine](https://github.com/influxdata/influxdb_iox) is a new time series storage engine, currently under active development.
Its design objectives include critical features for storing and querying observability signals at scale:
- high cardinality
- high capacity
- high performance

## Schema Reference

[Schema reference with conversion tables](docs/index.md).

## `otel2influx`

The golang package [`otel2influx`](otel2influx/README.md) converts OpenTelemetry protocol buffer objects to (measurement, tags, fields, timestamp) tuples.
It is imported by [a WIP fork of OpenTelemetry Collector Contrib](https://github.com/influxdata/opentelemetry-collector-contrib/tree/influxdb) and by [a WIP fork of Telegraf](https://github.com/jacobmarble/telegraf/tree/jgm-opentelemetry).

## `jaeger-query-plugin`

The [Jaeger Query Plugin for InfluxDB](jaeger-query-plugin) enables querying traces stored in InfluxDB/IOx via the Jaeger UI.

## `otlp`

The golang package `otlp` is the generated form of the [OpenTelemetry protobuf types](https://github.com/open-telemetry/opentelemetry-proto).
To regenerate:
```console
$ ./gen-proto.sh
```
