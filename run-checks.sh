#!/bin/sh

set -e

cd "$(dirname "$0")"
BASEDIR=$(pwd)

for package in common influx2otel otel2influx tests-integration; do
  cd ${BASEDIR}/${package}
  if ! go build; then
    fail=1
  fi
  if ! go test; then
    fail=1
  fi
  if [[ -z $(gofmt -s -l . | head -n 1) ]]; then
    fail=1
    gofmt -s -d .
  fi
  if ! go vet; then
    fail=1
  fi
  staticcheck -f stylish
done

if [ -n "$fail" ]; then
  exit 1
fi
