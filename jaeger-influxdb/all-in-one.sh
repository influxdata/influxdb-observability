#!/usr/bin/env bash

trap 'kill $(jobs -p)' SIGINT SIGTERM

./jaeger-influxdb &
./jaeger-query &

wait -n
kill -s SIGINT $(jobs -p)
wait
