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

## `jaeger-influxdb`

The [Jaeger Query Plugin for InfluxDB](jaeger-influxdb) enables querying traces stored in InfluxDB/IOx via the Jaeger UI.

## `common`

The golang package `common` contains simple utilities and common string values,
used in at least two of the above-mentioned packages.

This package is only intended for use by packages within this repository.

## `tests-integration`

The golang package `tests-integration` contains integration tests.
These tests exercise the above packages against OpenTelemetry Collector and Telegraf.

To run these tests:
```console
$ cd tests-integration
$ go test
```

## Contributing

Changes can be tested on a local branch using the `run-checks.sh` tool.
`run-checks.sh` verifies `go mod tidy` using `git diff`,
so any changes must be staged for commit in order for `run-checks.sh` to pass.

To update all OpenTelemetry dependencies in the various modules of this repository:
- run `update-otel.sh`
- stage the changed `go.mod` and `go.sum` files
- run `run-checks.sh`
