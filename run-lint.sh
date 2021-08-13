#!/bin/sh

set -e

cd "$(dirname "$0")"
BASEDIR=$(pwd)

cd "${BASEDIR}/common"
if ! go vet ; then
  fail=1
fi

cd "${BASEDIR}/influx2otel"
if ! go vet ; then
  fail=1
fi

cd "${BASEDIR}/otel2influx"
if ! go vet ; then
  fail=1
fi

unformatted=$(gofmt -s -l "${BASEDIR}/common" "${BASEDIR}/influx2otel" "${BASEDIR}/otel2influx")
if [ ! -z "$unformatted" ] ; then
  for filename in $unformatted ; do
    gofmt -s -d "$filename"
  done
  fail=1
fi

if [ -n "$fail" ] ; then
  exit 1
fi
