#syntax=docker/dockerfile:1.2
FROM golang:1.19-alpine AS builder
RUN apk --update --no-cache add ca-certificates

RUN \
  --mount=type=cache,id=influxdb-observability-gocache,sharing=locked,target=/root/.cache/go-build \
  --mount=type=cache,id=influxdb-observability-gomodcache,sharing=locked,target=/go/pkg/mod \
  du -cshx /root/.cache/go-build /go/pkg/mod && \
  go install go.opentelemetry.io/collector/cmd/builder@latest && \
  du -cshx /root/.cache/go-build /go/pkg/mod

COPY . /project
WORKDIR /project/otelcol-influxdb
ENV CGO_ENABLED 0

RUN \
  --mount=type=cache,id=influxdb-observability-gocache,sharing=locked,target=/root/.cache/go-build \
  --mount=type=cache,id=influxdb-observability-gomodcache,sharing=locked,target=/go/pkg/mod \
  du -cshx /root/.cache/go-build /go/pkg/mod && \
  builder --config build.yml && \
  du -cshx /root/.cache/go-build /go/pkg/mod

FROM scratch
USER 10001
COPY --from=builder /etc/ssl/certs/ca-certificates.crt /etc/ssl/certs/
COPY --from=builder --chmod=0755 /project/otelcol-influxdb/build/otelcol-influxdb /
ENTRYPOINT ["/otelcol-influxdb"]