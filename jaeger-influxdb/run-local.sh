#!/bin/bash

set -e

cd "$(dirname "$0")"
BASEDIR=$(pwd)

export LOG_LEVEL=debug
export LISTEN_ADDR=:17271
export INFLUXDB_TIMEOUT=5s
export INFLUXDB_ADDR=stag-us-east-1-4.aws.cloud2.influxdata.com
export INFLUXDB_BUCKET=otel
export INFLUXDB_BUCKET_ARCHIVE=otel-archive
export INFLUXDB_TOKEN=slDCMuNMVxJtWwl5a8tm0J9mW_Q4cKULY5V91Dz2NsR7KLRnY_je3WrWDGzD6djh3-G-XZTg6jvkBpgtteferw==

go run ./cmd/jaeger-influxdb
