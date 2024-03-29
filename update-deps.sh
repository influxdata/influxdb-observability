#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"
BASEDIR=$(pwd)

for module in common influx2otel otel2influx jaeger-influxdb tests-integration; do
  cd ${BASEDIR}/${module}
  go mod tidy
  go list -f '{{range .Imports}}{{.}}
{{end}}
{{range .TestImports}}{{.}}
{{end}}
{{range .XTestImports}}{{.}}
{{end}}' ./... | sort | uniq | grep 'github.com/open-telemetry\|go.opentelemetry.io\|github.com/jaegertracing/jaeger\|github.com/influxdata/influxdb-observability' | xargs go get -t
  go mod tidy

done

cd ${BASEDIR}
