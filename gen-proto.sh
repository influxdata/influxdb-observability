#!/bin/sh

set -e

OTEL_PROTO_VERSION="v0.8.0"

cd $(dirname "$0")
BASEDIR=$(pwd)

function cleanup {
  echo "cleaning up"
  rm -rf "$BASEDIR"/opentelemetry-proto
}

trap cleanup EXIT

rm -rf "$BASEDIR"/opentelemetry-proto "$BASEDIR"/otlp
git clone --depth 1 --branch ${OTEL_PROTO_VERSION} --quiet https://github.com/open-telemetry/opentelemetry-proto "$BASEDIR"/opentelemetry-proto
cd "$BASEDIR"/opentelemetry-proto
make gen-go
mv gen/go/github.com/open-telemetry/opentelemetry-proto/gen/go "$BASEDIR"/otlp/
ls "$BASEDIR"
cd "$BASEDIR"/otlp
go mod init github.com/open-telemetry/opentelemetry-proto/gen/go
