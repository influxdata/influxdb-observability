# Logs

A log record is a timestamped event record, containing textual structured and/or unstructured information.
Log records are composed of:

- timestamp
- specific attributes (mostly optional)
- zero-to-many free-form attributes

#### References

- [OpenTelemetry Logs Specification](https://github.com/open-telemetry/opentelemetry-specification/tree/v1.1.0/specification/logs)
- [OpenTelemetry LogRecord protocol buffer message](https://github.com/open-telemetry/opentelemetry-proto/blob/v0.8.0/opentelemetry/proto/logs/v1/logs.proto#L86-L132)
- [Fluentd Event structure](https://docs.fluentd.org/v/1.0/quickstart/life-of-a-fluentd-event#event-structure)
- [Greylog GELF payload spec](https://docs.graylog.org/en/4.0/pages/gelf.html#gelf-payload-specification)
- [Syslog message parts - RFC 3164 (obsoleted)](https://tools.ietf.org/html/rfc3164#section-4)
- [Syslog message parts - RFC 5424](https://tools.ietf.org/html/rfc5424#section-6)
- [Vector log event](https://vector.dev/docs/about/under-the-hood/architecture/data-model/log/)
- [Logfmt description](https://brandur.org/logfmt)
- [Elastic Common Schema: Log Fields](https://www.elastic.co/guide/en/ecs/current/ecs-log.html)

## Log Records

Influx measurement/tag/field                        | OpenTelemetry LogRecord field                   | Fluentd                                                   | Greylog GELF                                         | Syslog 3164                | Syslog 5424
--- | --- | --- | --- | --- | ---
measurement =`logs`                                 | .                                                                                                                                                             
timestamp                                           | `time_unix_nano` fixed64                        | `time` float                                              | `timestamp` number                                   | `HEADER timestamp` string  | `TIMESTAMP` string
`body` field string                                 | `body` AnyValue                                 | `record["message"]` string or<br />`record["log"]` string | `full_message` string or<br />`short_message` string | `MSG content` string       | `MSG` string
`name` tag                                          | `name` string                                   |                                                           |                                                      | `MSG tag` string           | `APP-NAME` string
`trace_id` tag                                      | `trace_id` bytes
`span_id` tag                                       | `span_id` bytes
.                                                   | `instrumentation_library` InstrumentationLibrary
`otel.library.name` tag                             | `InstrumentationLibrary.name` string
`otel.library.version` tag                          | `InstrumentationLibrary.version` string
.                                                   | `resource` Resource
(free-form fields)\*                                | `Resource.attributes` repeated KeyValue
`otel.resource.dropped_attributes_count` field uint | `Resource.dropped_attributes_count` uint32
(free-form fields)\*                                | `attributes` repeated KeyValue                  | `record` JSON map                                         | `_[additional field]` string or number               |                            | `STRUCTURED-DATA` string
`otel.log.dropped_attributes_count` field uint      | `dropped_attributes_count` uint32
`severity_number` tag uint                          | `severity_number` enum SeverityNumber           |                                                           | `level` number                                       | `PRI severity` integer     | `PRI severity` integer
`severity_text` field string                        | `severity_text` string
`otel.log.flags` field uint                         | `flags` fixed32
.                                                   | `attributes["fluent.tag"]` string               | `tag` string
.                                                   | `Resource.attributes["net.host.name"]` string   |                                                           | `host` string                                        | `HEADER hostname` string   | `HOSTNAME` string
.                                                   | `Resource.attributes["net.host.ip"]` string     |                                                           |                                                      | `HEADER IP address` string | `HOSTNAME` string
.                                                   | `Resource.attributes["greylog.version"]` string |                                                           | `version` string =`1.1`
.                                                   | `Resource.attributes["syslog.version"]` string  |                                                           |                                                      |                            | `VERSION` integer =`1`
.                                                   | TODO                                            |                                                           |                                                      |                            | `PROCID` varying
.                                                   | TODO                                            |                                                           |                                                      |                            | `MSGID` string



\* To convert from Influx to OTel, use common OTel attribute key prefixes to distinguish resource attributes from log record attributes.
This regex matches resource attribute keys:

```
^(service\.|telemetry\.|container\.|process\.|host\.|os\.|cloud\.|deployment\.|k8s\.|aws\.|gcp\.|azure\.|faas\.name|faas\.id|faas\.version|faas\.instance|faas\.max_memory)
```
