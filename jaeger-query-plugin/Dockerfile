#syntax=docker/dockerfile:1.2
FROM golang:1.17-alpine AS builder

COPY . /go/src/influxdb-observability
WORKDIR /go/src/influxdb-observability/jaeger-query-plugin

RUN go install ./cmd/jaeger-query-influxdb

FROM alpine:3.14

COPY --from=jaegertracing/jaeger-query:1.28 /go/bin/query-linux /usr/bin/jaeger-query
COPY --from=builder /go/bin/jaeger-query-influxdb /usr/bin/jaeger-query-influxdb

ENV SPAN_STORAGE_TYPE grpc-plugin
ENV GRPC_STORAGE_PLUGIN_BINARY /usr/bin/jaeger-query-influxdb

ENTRYPOINT /usr/bin/jaeger-query
