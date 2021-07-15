#!/bin/sh

set -e

cd "$(dirname "$0")"
BASEDIR=$(pwd)

cd "${BASEDIR}/common"
if ! go test ; then
  fail=1
fi

cd "${BASEDIR}/influx2otel"
if ! go test ; then
  fail=1
fi

cd "${BASEDIR}/otel2influx"
if ! go test ; then
  fail=1
fi

if [ -n "$fail" ] ; then
  exit 1
fi
