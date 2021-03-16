#!/bin/bash

set -e

influxdb_iox database create myorg_mybucket --host http://${HOST}:${PORT} -m 100
