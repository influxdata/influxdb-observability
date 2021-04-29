#!/bin/sh

set -e

# This version matches the current version used by otelcol
OTEL_PROTO_VERSION="8ab21e9da6246e465cd9d50d405561aedef31a1e"

cd "$(dirname "$0")"
BASEDIR=$(pwd)

cleanup() {
  echo "cleaning up"
  rm -rf "$BASEDIR"/opentelemetry-proto
}

trap cleanup EXIT

rm -rf "$BASEDIR"/opentelemetry-proto "$BASEDIR"/otlp
git clone --quiet https://github.com/open-telemetry/opentelemetry-proto "$BASEDIR"/opentelemetry-proto
cd "$BASEDIR"/opentelemetry-proto
git checkout ${OTEL_PROTO_VERSION}
find . -type f -name '*.proto' -exec sed -i '' 's+github.com/open-telemetry/opentelemetry-proto/gen/go/+github.com/influxdata/influxdb-observability/otlp/+g' {} +
mkdir gen
find . -type f -name '*.proto' -exec protoc --proto_path=. --go_out=gen --go-grpc_out=gen {} +
mv gen/github.com/influxdata/influxdb-observability/otlp "$BASEDIR"/
cd "$BASEDIR"/otlp
go mod init github.com/influxdata/influxdb-observability/otlp
go mod tidy

