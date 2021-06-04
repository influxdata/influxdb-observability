# Metrics

A metric is a measurement about a particular component within a service.
Metrics are composed of:

- timestamp
- specific attributes (mostly optional)
- zero-to-many free-form attributes
- one-to-many varying values

This package emits two metrics schemas, based on Telegraf's Prometheus input plugin schemas.
- Schema `metric_version = 1`:
  - Telegraf/InfluxDB measurement per Prometheus metric
  - Fields `count`/`sum`/`gauge`/etc contain metric values
  - Implemented here as `MetricsSchemaTelegrafPrometheusV1`
- Schema `metric_version = 2`:
  - One measurement `prometheus`
  - Fields (Prometheus metric name) + `_` + `count`/`sum`/`gauge`/etc contain metric values
  - Implemented here as `MetricsSchemaTelegrafPrometheusV2`

#### References

- [OpenTelemetry Metrics Specification](https://github.com/open-telemetry/opentelemetry-specification/tree/v1.1.0/specification/metrics)
- [OpenTelemetry Metrics Data Model](https://github.com/open-telemetry/opentelemetry-specification/blob/ea7ec75fa8376cd6ad937fe9d130835c397c414f/specification/metrics/datamodel.md)
- [OpenTelemetry Metric protocol buffer message](https://github.com/open-telemetry/opentelemetry-proto/blob/v0.8.0/opentelemetry/proto/metrics/v1/metrics.proto#L48-L148)
- [Telegraf Prometheus Input Plugin](https://github.com/influxdata/telegraf/tree/master/plugins/inputs/prometheus)
- [Prometheus Data Model](https://prometheus.io/docs/concepts/data_model/)
- [Prometheus golang structs](https://github.com/prometheus/client_golang/tree/v1.10.0/prometheus)
- [OpenMetrics spec](https://github.com/OpenObservability/OpenMetrics/blob/v1.0.0/specification/OpenMetrics.md)
- [Vector metric event](https://vector.dev/docs/about/under-the-hood/architecture/data-model/metric/)
- [StatsD](https://github.com/statsd/statsd)

## Schema `MetricsSchemaTelegrafPrometheusV1`

### Gauge Metric

Influx measurement/tag/field | OpenTelemetry Metric field                         | Prometheus attribute
--- | --- | ---
measurement                  | `name` string                                      | metric name
.                            | `description` string                               | `HELP` (string)
.                            | `unit` string                                      |
.                            | `resource` Resource                                |
(free-form tags)             | `Resource.attributes` repeated KeyValue            | (labels)
.                            | `Resource.dropped_attributes_count` uint32         |
.                            | `instrumentation_library` InstrumentationLibrary   |
`otel.library.name` tag      | `InstrumentationLibrary.name` string               | (labels)
`otel.library.version` tag   | `InstrumentationLibrary.version` string            | (labels)
.                            | `gauge` Gauge                                      | `TYPE` = `gauge`
.                            | `gauge.data_points.start_time_unix_nano` fixed64   |
timestamp                    | `gauge.data_points.time_unix_nano` fixed64         | timestamp (Unix millis)
(free-form tags)             | `gauge.data_points.labels` repeated StringKeyValue | labels (string:string map)
`gauge` field float          | `gauge.data_points.value` double or sfixed64       | value (float)
.                            | `gauge.data_points.exemplars` repeated Exemplars   |


### Sum Metric

Influx measurement/tag/field | OpenTelemetry Metric field                                | Prometheus attribute
--- | --- | ---
measurement                  | `name` string                                             | metric name
.                            | `description` string                                      | `HELP` (string)
.                            | `unit` string                                             |
.                            | `resource` Resource                                       |
(free-form tags)             | `Resource.attributes` repeated KeyValue                   | (labels)
.                            | `Resource.dropped_attributes_count` uint32                |
.                            | `instrumentation_library` InstrumentationLibrary          |
`otel.library.name` tag      | `InstrumentationLibrary.name` string                      | (labels)
`otel.library.version` tag   | `InstrumentationLibrary.version` string                   | (labels)
.                            | `sum` Sum                                                 | `TYPE` = `counter`
(only CUMULATIVE)            | `sum.aggregation_temporality` enum AggregationTemporality |
(only TRUE)                  | `sum.is_monotonic` bool                                   |
.                            | `sum.data_points.start_time_unix_nano` fixed64            |
timestamp                    | `sum.data_points.time_unix_nano` fixed64                  | timestamp (Unix millis)
(free-form tags)             | `sum.data_points.labels` repeated StringKeyValue          | labels (string:string map)
`counter` field float        | `sum.data_points.value` double or sfixed64                | value (float)
.                            | `sum.data_points.exemplars` repeated Exemplars            |


### Histogram Metric

Influx measurement/tag/field         | OpenTelemetry Metric field                                      | Prometheus attribute
--- | --- | ---
measurement                          | `name` string                                                   | metric name
.                                    | `description` string                                            | `HELP` (string)
.                                    | `unit` string                                                   |
.                                    | `resource` Resource                                             |
(free-form tags)                     | `Resource.attributes` repeated KeyValue                         | (labels)
.                                    | `Resource.dropped_attributes_count` uint32                      |
.                                    | `instrumentation_library` InstrumentationLibrary                |
`otel.library.name` tag              | `InstrumentationLibrary.name` string                            | (labels)
`otel.library.version` tag           | `InstrumentationLibrary.version` string                         | (labels)
.                                    | `histogram` Histogram                                           | `TYPE` = `histogram`
(only CUMULATIVE)                    | `histogram.aggregation_temporality` enum AggregationTemporality |
.                                    | `histogram.data_points.start_time_unix_nano` fixed64            |
timestamp                            | `histogram.data_points.time_unix_nano` fixed64                  | timestamp (Unix millis)
(free-form tags)                     | `histogram.data_points.labels` repeated StringKeyValue          | labels (string:string map)
`count` field float                  | `histogram.data_points.count` fixed64                           | metric name + `_count` (float)
`sum` field float                    | `histogram.data_points.sum` double                              | metric name + `_sum` (float)
(bucket count as string) field key   | `histogram.data_points.bucket_counts` repeated fixed64          | metric name + `_bucket` (float)
(bucket count as string) field float | `histogram.data_points.explicit_bounds` repeated double         | label `le`
.                                    | `histogram.data_points.exemplars` repeated Exemplars            |


### Summary Metric

Influx measurement/tag/field     | OpenTelemetry Metric field                                     | Prometheus attribute
--- | --- | ---
measurement                      | `name` string                                                  | metric name
.                                | `description` string                                           | `HELP` (string)
.                                | `unit` string                                                  |
.                                | `resource` Resource                                            |
(free-form tags)                 | `Resource.attributes` repeated KeyValue                        | (labels)
.                                | `Resource.dropped_attributes_count` uint32                     |
.                                | `instrumentation_library` InstrumentationLibrary               |
`otel.library.name` tag          | `InstrumentationLibrary.name` string                           | (labels)
`otel.library.version` tag       | `InstrumentationLibrary.version` string                        | (labels)
.                                | `summary` Summary                                              | `TYPE` = `summary`
.                                | `summary.data_points.start_time_unix_nano` fixed64             |
timestamp                        | `summary.data_points.time_unix_nano` fixed64                   | timestamp (Unix millis)
(free-form tags)                 | `summary.data_points.labels` repeated StringKeyValue           | labels (string:string map)
`count` field float              | `summary.data_points.count` fixed64                            | metric name + `_count` (float)
`sum` field float                | `summary.data_points.sum` double                               | metric name + `_sum` (float)
.                                | `summary.data_points.quantile_values` repeated ValueAtQuantile |
(quantile as string) field key   | `summary.data_points.quantile_values.quantile` double          | value (float)
(quantile as string) field float | `summary.data_points.quantile_values.value` double             | label `quantile`


## Schema `MetricsSchemaTelegrafPrometheusV2`

In this schema, the Influx measurement name is always `prometheus`.

### Gauge Metric

Influx tag/field           | OpenTelemetry Metric field                         | Prometheus attribute
--- | --- | ---
.                          | `name` string                                      | metric name
.                          | `description` string                               | `HELP` (string)
.                          | `unit` string                                      |
.                          | `resource` Resource                                |
(free-form tags)           | `Resource.attributes` repeated KeyValue            | (labels)
.                          | `Resource.dropped_attributes_count` uint32         |
.                          | `instrumentation_library` InstrumentationLibrary   |
`otel.library.name` tag    | `InstrumentationLibrary.name` string               | (labels)
`otel.library.version` tag | `InstrumentationLibrary.version` string            | (labels)
.                          | `gauge` Gauge                                      | `TYPE` = `gauge`
.                          | `gauge.data_points.start_time_unix_nano` fixed64   |
timestamp                  | `gauge.data_points.time_unix_nano` fixed64         | timestamp (Unix millis)
(free-form tags)           | `gauge.data_points.labels` repeated StringKeyValue | labels (string:string map)
(Metric name) field float  | `gauge.data_points.value` double or sfixed64       | value (float)
.                          | `gauge.data_points.exemplars` repeated Exemplars   |


### Sum Metric

Influx tag/field           | OpenTelemetry Metric field                                | Prometheus attribute
--- | --- | ---
.                          | `name` string                                             | metric name
.                          | `description` string                                      | `HELP` (string)
.                          | `unit` string                                             |
.                          | `resource` Resource                                       |
(free-form tags)           | `Resource.attributes` repeated KeyValue                   | (labels)
.                          | `Resource.dropped_attributes_count` uint32                |
.                          | `instrumentation_library` InstrumentationLibrary          |
`otel.library.name` tag    | `InstrumentationLibrary.name` string                      | (labels)
`otel.library.version` tag | `InstrumentationLibrary.version` string                   | (labels)
.                          | `sum` Sum                                                 | `TYPE` = `counter`
(only CUMULATIVE)          | `sum.aggregation_temporality` enum AggregationTemporality |
(only TRUE)                | `sum.is_monotonic` bool                                   |
.                          | `sum.data_points.start_time_unix_nano` fixed64            |
timestamp                  | `sum.data_points.time_unix_nano` fixed64                  | timestamp (Unix millis)
(free-form tags)           | `sum.data_points.labels` repeated StringKeyValue          | labels (string:string map)
(Metric name) field float  | `sum.data_points.value` double or sfixed64                | value (float)
.                          | `sum.data_points.exemplars` repeated Exemplars            |


### Histogram Metric

Influx tag/field                     | OpenTelemetry Metric field                                      | Prometheus attribute
--- | --- | ---
.                                    | `name` string                                                   | metric name
.                                    | `description` string                                            | `HELP` (string)
.                                    | `unit` string                                                   |
.                                    | `resource` Resource                                             |
(free-form tags)                     | `Resource.attributes` repeated KeyValue                         | (labels)
.                                    | `Resource.dropped_attributes_count` uint32                      |
.                                    | `instrumentation_library` InstrumentationLibrary                |
`otel.library.name` tag              | `InstrumentationLibrary.name` string                            | (labels)
`otel.library.version` tag           | `InstrumentationLibrary.version` string                         | (labels)
.                                    | `histogram` Histogram                                           | `TYPE` = `histogram`
(only CUMULATIVE)                    | `histogram.aggregation_temporality` enum AggregationTemporality |
.                                    | `histogram.data_points.start_time_unix_nano` fixed64            |
timestamp                            | `histogram.data_points.time_unix_nano` fixed64                  | timestamp (Unix millis)
(free-form tags)                     | `histogram.data_points.labels` repeated StringKeyValue          | labels (string:string map)
(Metric name) +`_count` field float  | `histogram.data_points.count` fixed64                           | metric name + `_count` (float)
(Metric name) +`_sum` field float    | `histogram.data_points.sum` double                              | metric name + `_sum` (float)
(Metric name) +`_bucket` field float | `histogram.data_points.bucket_counts` repeated fixed64          | metric name + `_bucket` (float)
`le` tag                             | `histogram.data_points.explicit_bounds` repeated double         | label `le`
.                                    | `histogram.data_points.exemplars` repeated Exemplars            |


### Summary Metric

Influx tag/field                     | OpenTelemetry Metric field                                     | Prometheus attribute
--- | --- | ---
.                                    | `name` string                                                  | metric name
.                                    | `description` string                                           | `HELP` (string)
.                                    | `unit` string                                                  |
.                                    | `resource` Resource                                            |
(free-form tags)                     | `Resource.attributes` repeated KeyValue                        | (labels)
.                                    | `Resource.dropped_attributes_count` uint32                     |
.                                    | `instrumentation_library` InstrumentationLibrary               |
`otel.library.name` tag              | `InstrumentationLibrary.name` string                           | (labels)
`otel.library.version` tag           | `InstrumentationLibrary.version` string                        | (labels)
.                                    | `summary` Summary                                              | `TYPE` = `summary`
.                                    | `summary.data_points.start_time_unix_nano` fixed64             |
timestamp                            | `summary.data_points.time_unix_nano` fixed64                   | timestamp (Unix millis)
(free-form tags)                     | `summary.data_points.labels` repeated StringKeyValue           | labels (string:string map)
(metric name) +`_count` field float  | `summary.data_points.count` fixed64                            | metric name + `_count` (float)
(metric name) +`_sum` field float    | `summary.data_points.sum` double                               | metric name + `_sum` (float)
.                                    | `summary.data_points.quantile_values` repeated ValueAtQuantile |
`quantile` tag                       | `summary.data_points.quantile_values.quantile` double          | value (float)
(metric name) field float            | `summary.data_points.quantile_values.value` double             | label `quantile`
