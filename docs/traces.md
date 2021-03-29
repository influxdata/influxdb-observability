# Traces

A trace is a list of spans.
A span is composed of:

- start and end timestamps
- some specific attributes
- zero-to-many free-form attributes
- logs
- links to other spans

#### References

- [OpenTelemetry Tracing Specification](https://github.com/open-telemetry/opentelemetry-specification/tree/v1.1.0/specification/trace)
- [OpenTelemetry Span protocol buffer message](https://github.com/open-telemetry/opentelemetry-proto/blob/v0.8.0/opentelemetry/proto/trace/v1/trace.proto#L48-L227)
- [OpenTracing Specification](https://github.com/opentracing/specification)
- [Jaeger protocol buffers](https://github.com/jaegertracing/jaeger-idl/tree/34396033ff11c60fced342ab2858ace278fedaa8/proto)
- [OpenTelemetry -> Jaeger](https://github.com/open-telemetry/opentelemetry-specification/blob/v1.1.0/specification/trace/sdk_exporters/jaeger.md) TODO link to code and documentation
- [Zipkin protocol buffers](https://github.com/openzipkin/zipkin-api/blob/1.0.0/zipkin.proto)
- [OpenTelemetry -> Zipkin](https://github.com/open-telemetry/opentelemetry-specification/blob/v1.1.0/specification/trace/sdk_exporters/zipkin.md) TODO link to code and documentation

## Trace Spans

Influx measurement/tag/field                        | OpenTelemetry Span field                    | Jaeger Span field                                                                | Zipkin Span field
--- | --- | --- | ---
`spans` measurement                                 | -
timestamp                                           | `start_time_unix_nano` fixed64              | `start_time` Timestamp                                                           | `timestamp` fixed64 (µs)
`trace_id` tag                                      | `trace_id` bytes                            | `trace_id` bytes                                                                 | `trace_id` bytes
`span_id` tag                                       | `span_id` bytes                             | `span_id` bytes                                                                  | `id` bytes
`parent_span_id` tag                                | `parent_span_id` bytes                      | (included in `references`)<br />type `CHILD_OF`                                  | `parent_id` bytes
`trace_state` tag                                   | `trace_state` string
`name` tag                                          | `name` string                               | `operation_name` string                                                          | `name` string
`kind` tag<br />(OTel stringified)                  | `kind` enum SpanKind                        | `tags["span.kind"]`                                                              | `kind` enum Kind
`end_time_unix_nano` field int                      | `end_time_unix_nano` fixed64
`duration_nano` field int                           |                                             | `duration` Duration                                                              | `duration` uint64 (µs)
-                                                   | `status` Status
`otel.status_code` tag; `OK` or `ERROR`             | `status.code` enum StatusCode               | `tags["otel.status_code"]`<br />if `ERROR` then add:<br />`tags["error"] = true` | `tags["otel.status_code"]`<br />if `ERROR` then add:<br />`tags["error"] = true`
`otel.status_description` field string              | `status.message` string                     | `tags["otel.status_description"]`                                                | `tags["error"]`<br />iff `otel.status_code` == ERROR
-                                                   | `instrumentation_library` InstrumentationLibrary
`otel.library.name` tag                             | `InstrumentationLibrary.name` string        | `tags["otel.library.name"]`                                                      | `tags["otel.library.name"]`
`otel.library.version` tag                          | `InstrumentationLibrary.version` string     | `tags["otel.library.version"]`                                                   | `tags["otel.library.version"]`
-                                                   | `resource` Resource                         | `process` Process
-                                                   | `attributes["service.name"]`                | `process.service_name` string
(free-form fields)\*                                | `Resource.attributes` repeated KeyValue.    | `process.tags` repeated KeyValue
`otel.resource.dropped_attributes_count` field uint | `Resource.dropped_attributes_count` uint32
(free-form fields)\*                                | `attributes` repeated KeyValue              | `tags` repeated KeyValue                                                         | `tags` map<string, string>
`otel.span.dropped_attributes_count` field uint     | `dropped_attributes_count` uint32
(see "Influx measurement `logs`")                   | `events` repeated Event                     | `logs` repeated Log                                                              | `annotations` repeated Annotation
`otel.span.dropped_events_count` field uint.        | `dropped_events_count` uint32
(see "Influx measurement `span-links`")             | `links` repeated Link                       | `references` repeated SpanRef
`otel.span.dropped_links_count` field uint          | `dropped_links_count` uint32
-                                                   |                                             | `flags` uint32
-                                                   |                                             | `warnings` string
-                                                   | `attributes["zipkin.local_endpoint"]`       |                                                                                  | `local_endpoint` Endpoint
-                                                   | \*\*                                        |                                                                                  | `remote_endpoint` Endpoint
-                                                   | `attributes["zipkin.debug"]`                |                                                                                  | `debug` bool
-                                                   | `attributes["zipkin.shared"]`               |                                                                                  | `shared` bool

\* To convert from Influx to OTel, use common OTel attribute key prefixes to distinguish resource attributes from span attributes.
This regex matches resource attribute keys:

```
^(service\.|telemetry\.|container\.|process\.|host\.|os\.|cloud\.|deployment\.|k8s\.|aws\.|gcp\.|azure\.|faas\.name|faas\.id|faas\.version|faas\.instance|faas\.max_memory)
```

\*\* Zipkin's `remote_endpoint` [must be created from several OTel attributes](https://github.com/open-telemetry/opentelemetry-specification/blob/main/specification/trace/sdk_exporters/zipkin.md#remote-endpoint)

## Span Logs

Influx measurement/tag/field                     | OpenTelemetry Span.Event field    | Jaeger Log field      | Zipkin Annotation field
--- | --- | --- | ---
`logs` measurement                               | -
timestamp                                        | `time_unix_nano` fixed64          | `timestamp` Timestamp | `timestamp` fixed64 (µs)
`trace_id` tag                                   | `trace_id` bytes
`span_id` tag                                    | `span_id` bytes
`name` tag                                       | `name` string                     | `fields["event"]`     | `value` string\*\*
`body` field string\*                            |                                   | `fields["message"]`
?                                                | ?                                 | `fields["stack"]`
(free-form fields)                               | `attributes` repeated KeyValue    | `fields` repeated KeyValue
`otel.event.dropped_attributes_count` field uint | `dropped_attributes_count` uint32 | `fields["otel.event.dropped_attributes_count"]`
(free-form fields)                               | span resource.attributes

\* `body` does not exist in the OpenTelemetry Span.Event, but does in OpenTelemetry LogRecord; InfluxDB explicitly names it in the `logs` measurement.

\*\* `value` is composed as:

```
"<name>": {"<attribute key>": "<attribute value", ...}
```

## Span Links

Influx measurement/tag/field                     | OpenTelemetry Span.Link field     | Jaeger SpanRef field
--- | --- | ---
`span-links` measurement                         | -
timestamp                                        | (copied from linking span)
`trace_id` tag                                   | (copied from linking span)
`span_id` tag                                    | (copied from linking span)
`linked_trace_id` tag                            | `trace_id` bytes                  | `trace_id` bytes
`linked_span_id` tag                             | `span_id` bytes                   | `span_id` bytes
`trace_state` tag                                | `trace_state` string
(free-form fields)                               | `attributes` repeated KeyValue
`otel.link.dropped_attributes_count` field uint  | `dropped_attributes_count` uint32
-                                                |                                   | `ref_type` SpanRefType<br />always `FOLLOWS_FROM`
