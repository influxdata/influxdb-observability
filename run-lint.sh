#!/bin/sh

set -e

cd "$(dirname "$0")"
BASEDIR=$(pwd)

unformatted=$(gofmt -s -l "${BASEDIR}/common" "${BASEDIR}/influx2otel" "${BASEDIR}/otel2influx")
if [ -z "$unformatted" ] ; then
  exit 0
else
  for filename in $unformatted ; do
    gofmt -s -d "$filename"
  done
  exit 1
fi
