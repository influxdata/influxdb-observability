#!/bin/sh

set -e

OTEL_PROTO_VERSION="v0.8.0"

cd "$(dirname "$0")"
BASEDIR=$(pwd)

cleanup() {
  echo "cleaning up"
  rm -rf "$BASEDIR"/opentelemetry-proto
}

trap cleanup EXIT

rm -rf "$BASEDIR"/opentelemetry-proto "$BASEDIR"/otlp
git clone --depth 1 --branch ${OTEL_PROTO_VERSION} --quiet https://github.com/open-telemetry/opentelemetry-proto "$BASEDIR"/opentelemetry-proto
cd "$BASEDIR"/opentelemetry-proto
find . -type f -name '*.proto' -exec sed -i '' 's+github.com/open-telemetry/opentelemetry-proto/gen/go/+github.com/influxdata/influxdb-observability/otlp/+g' {} +
make gen-go
mv gen/go/github.com/influxdata/influxdb-observability/otlp "$BASEDIR"/
cd "$BASEDIR"/otlp
go mod init github.com/influxdata/influxdb-observability/otlp
