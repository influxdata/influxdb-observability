# InfluxDB Observability

This repository is a reference for converting observability signals (traces, metrics, logs) to/from a common InfluxDB schema.

## TODO
Fork this demo:
https://github.com/open-telemetry/opentelemetry-demo

Double-check auth with this doc:
https://opentelemetry.io/docs/collector/custom-auth/

## WIP

This branch is a work-in-progress, with the goal to refine tracing.
Steps to run the current demo follow.

In an InfluxDB Cloud 2 org backed by IOx, create a bucket named `otel` and a token with permission to read and write to that bucket.

In docker-compose.yml, set values for these keys:
```yaml
INFLUXDB_ADDR: <region specific hostname - no https prefix>
INFLUXDB_BUCKETNAME: otel
INFLUXDB_TOKEN: <the API token you just created>
```

In otelcol-influxdb/config.yml, set the similar values for these keys:
```yaml
endpoint: https://<region specific URL - https://hostname>
bucket: otel
token: <the API token you just created>
```

Build the needed docker images:
```console
$ docker compose build
```

Run the docker compose:
```console
$ docker compose up --abort-on-container-exit --remove-orphans
```

Create some traces.
Browse to HotROD at http://localhost:8080 and click a few customer names.

Query those traces.
Browse to Jaeger at http://localhost:16686 and click "Find Traces" near the bottom left.

Click any trace.

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

## `tests-synthetic`

This is a Docker Compose that generates synthetic signals, for testing.
(It is not a pass-fail test.)
For more information see [tests-synthetic/README.md](tests-synthetic/README.md).
