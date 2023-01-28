# Jaeger UI Storage backend, InfluxDB Service

**This is experimental software**

This service enables querying traces stored in InfluxDB, via the Jaeger UI.
To write traces to InfluxDB, use the [OpenTelemetry Collector, InfluxDB Distribution](https://github.com/influxdata/influxdb-observability/tree/main/otelcol-influxdb).

## Docker
Docker images exist at [jacobmarble/jaeger-influxdb](https://hub.docker.com/r/jacobmarble/jaeger-influxdb) and [jacobmarble/jaeger-influxdb-all-in-one](https://hub.docker.com/r/jacobmarble/jaeger-influxdb-all-in-one).
In particular, the all-in-one image is great for testing,
but for production use, consider running `jaegertracing/jaeger-query` and `jacobmarble/jaeger-influxdb` in separate containers.
For an example configuration using separate containers, see [docker-compose.yml](../demo/docker-compose.yml).

## Build
Build the `jaeger-influxdb` service with `go install`:

```console
$ cd jaeger-influxdb
$ go install ./cmd/jaeger-influxdb/
```
