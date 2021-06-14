# Synthetic Load Tests

Generate signals and convert them between OTLP and line protocol,
from within otelcol and Telegraf.
Other load tests will be added later.

## Try It

These synthetic load tests exist:
- `docker-compose.otel2influx-telegraf.yml`
- `docker-compose.influx2otel-telegraf.yml`
- `docker-compose.otel2influx-otelcol.yml`
- `docker-compose.influx2otel-otelcol.yml`

For any of those:
```console
$ docker compose --file docker-compose.otel2influx-telegraf.yml build
$ docker compose --file docker-compose.otel2influx-telegraf.yml up
$ docker compose --file docker-compose.otel2influx-telegraf.yml down
```

Visit http://localhost:8000 to generate signals with the Jaeger HotRod app.

The console will print signals received by the Telegraf OpenTelemetry gRPC service, to stdout.

## Configure

To focus on a particular signal, simply comment services named `generate-*` in `docker-compose.*.yml`.
