# Synthetic Load Tests

This is a more-or-less black box test suite.
For now, it is simply a Docker Compose which emits generated signals to Telegraf via OTLP.
Other load tests will be added later.

## Try It

```console
$ docker compose build
$ docker compose up
```

Visit http://localhost:8000 to generate signals with the Jaeger HotRod app.

The console will print signals received by the Telegraf OpenTelemetry gRPC service, to stdout.

## Configure

To focus on a particular signal, simply comment services named `generate-*` in docker-compose.yml.
