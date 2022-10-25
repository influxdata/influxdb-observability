#syntax=docker/dockerfile:1.2
FROM golang:1.19-alpine AS builder
RUN apk --update --no-cache add ca-certificates
COPY . /project
WORKDIR /project/jaeger-query-plugin
ENV CGO_ENABLED 0

RUN \
  --mount=type=cache,id=influxdb-observability-gocache,sharing=locked,target=/root/.cache/go-build \
  --mount=type=cache,id=influxdb-observability-gomodcache,sharing=locked,target=/go/pkg/mod \
  du -cshx /root/.cache/go-build /go/pkg/mod && \
  go install ./cmd/jaeger-influxdb && \
  du -cshx /root/.cache/go-build /go/pkg/mod

FROM alpine:3.16
#FROM scratch

USER 10001
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/ca-certificates.crt
COPY --from=builder --chmod=0755 /go/bin/jaeger-influxdb /jaeger-influxdb

ENV LOG_LEVEL info
ENV LISTEN_ADDR :17271
EXPOSE 17271
ENV INFLUXDB_ADDR ""
ENV INFLUXDB_TIMEOUT "30s"
ENV INFLUXDB_BUCKETID ""
ENV INFLUXDB_BUCKETNAME ""
ENV INFLUXDB_TOKEN ""

ENTRYPOINT ["/jaeger-influxdb"]
