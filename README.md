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

## `otel2influx` and `influx2otel`

The golang package [`otel2influx`](otel2influx/README.md) converts OpenTelemetry protocol buffer objects to (measurement, tags, fields, timestamp) tuples.
It is imported by [a proposed OpenTelemetry Collector Contrib exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/pull/2952)
and by [a proposed Telegraf input plugin](https://github.com/influxdata/telegraf/pull/9077).

The golang package [`influx2otel`](influx2otel/README.md) converts (measurement, tags, fields, timestamp) tuples to OpenTelemetry protocol buffer objects.
It could be imported by an OpenTelemtry Collector Contrib receiver
or by a Telegraf output plugin.

## `jaeger-query-plugin`

The [Jaeger Query Plugin for InfluxDB](jaeger-query-plugin) enables querying traces stored in InfluxDB/IOx via the Jaeger UI.

## `otlp`

The golang package `otlp` is the generated form of the [OpenTelemetry protobuf types](https://github.com/open-telemetry/opentelemetry-proto).
To regenerate:
```console
$ ./gen-proto.sh
```

This package is only intended for use by packages within this repository.

## `common`

The golang package `common` contains simple utilities and common string values,
used in at least two of the above-mentioned packages.

This package is only intended for use by packages within this repository.
