#syntax=docker/dockerfile:1.2
FROM golang:1.15-alpine AS builder

COPY . /go/src/jaeger-influxdb
WORKDIR /go/src/jaeger-influxdb

RUN \
  --mount=type=cache,id=jaeger-influxdb-gocache,sharing=locked,target=/root/.cache/go-build \
  --mount=type=cache,id=jaeger-influxdb-gomodcache,sharing=locked,target=/go/pkg/mod \
  du -cshx /root/.cache/go-build /go/pkg/mod && \
  go install ./cmd/jaeger-influxdb && \
  du -cshx /root/.cache/go-build /go/pkg/mod

FROM alpine:3.13

COPY --from=jaegertracing/jaeger-query:1.22 /go/bin/query-linux /usr/bin/jaeger-query
COPY --from=builder /go/bin/jaeger-influxdb /usr/bin/jaeger-influxdb

ENV SPAN_STORAGE_TYPE grpc-plugin
ENV GRPC_STORAGE_PLUGIN_BINARY /usr/bin/jaeger-influxdb

ENTRYPOINT /usr/bin/jaeger-query
