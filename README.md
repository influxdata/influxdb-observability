# InfluxDB Observability

This repository is a reference for converting observability signals (traces, metrics, logs) to/from a common InfluxDB schema.

## Schema Reference

[Schema reference with conversion tables](docs/index.md).

## `otel2influx` and `influx2otel`

The golang package [`otel2influx`](otel2influx/README.md) converts OpenTelemetry protocol buffer objects to (measurement, tags, fields, timestamp) tuples.
It is imported by [the OpenTelemetry Collector InfluxDB exporter](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.27.0/exporter/influxdbexporter)
and by [the Telegraf OpenTelemetry input plugin](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/opentelemetry).

The golang package [`influx2otel`](influx2otel/README.md) converts (measurement, tags, fields, timestamp) tuples to OpenTelemetry protocol buffer objects.
It is imported by [the OpenTelemtry Collector InfluxDB receiver](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.27.0/receiver/influxdbreceiver)
and by [the (WIP) Telegraf OpenTelemetry output plugin](https://github.com/influxdata/telegraf/pull/9228).

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

## `tests`

The golang package `tests` contains integration tests.
These tests exercise the above packages against OpenTelemetry Collector and Telegraf.
