#!/usr/bin/env bash

set -e

cd "$(dirname "$0")"
BASEDIR=$(pwd)

for module in common influx2otel jaeger-influxdb otel2influx tests-integration; do
  cd ${BASEDIR}/${module}
  go list -f '{{range .Imports}}{{.}}
{{end}}
{{range .TestImports}}{{.}}
{{end}}' ./... | sort | uniq | grep 'github.com/open-telemetry\|go.opentelemetry.io' | xargs go get -t

done

cd ${BASEDIR}
