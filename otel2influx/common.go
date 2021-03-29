package otel2influx

const (
	measurementSpans     = "spans"
	measurementSpanLinks = "span-links"
	measurementLogs      = "logs"

	metricGaugeFieldKey          = "gauge"
	metricCounterFieldKey        = "counter"
	metricHistogramCountFieldKey = "count"
	metricHistogramSumFieldKey   = "sum"
	metricHistogramInfFieldKey   = "inf"
	metricSummaryCountFieldKey   = "count"
	metricSummarySumFieldKey     = "sum"

	// These attribute key names are influenced by the proto message keys.
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/trace/v1/trace.proto
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/metrics/v1/metrics.proto
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/logs/v1/logs.proto
	attributeTraceID                        = "trace_id"
	attributeSpanID                         = "span_id"
	attributeTraceState                     = "trace_state"
	attributeParentSpanID                   = "parent_span_id"
	attributeName                           = "name"
	attributeSpanKind                       = "kind"
	attributeEndTimeUnixNano                = "end_time_unix_nano"
	attributeDurationNano                   = "duration_nano"
	attributeDroppedResourceAttributesCount = "otel.resource.dropped_attributes_count"
	attributeDroppedSpanAttributesCount     = "otel.span.dropped_attributes_count"
	attributeDroppedEventAttributesCount    = "otel.event.dropped_attributes_count"
	attributeDroppedEventsCount             = "otel.span.dropped_events_count"
	attributeDroppedLinkAttributesCount     = "otel.link.dropped_attributes_count"
	attributeDroppedLinksCount              = "otel.span.dropped_links_count"
	attributeLinkedTraceID                  = "linked_trace_id"
	attributeLinkedSpanID                   = "linked_span_id"
	attributeStatusCode                     = "otel.status_code"
	attributeStatusCodeOK                   = "OK"
	attributeStatusCodeError                = "ERROR"
	attributeStatusMessage                  = "otel.status_description"
	attributeInstrumentationLibraryName     = "otel.library.name"
	attributeInstrumentationLibraryVersion  = "otel.library.version"
	attributeSeverityNumber                 = "severity_number"
	attributeSeverityText                   = "severity_text"
	attributeBody                           = "body"
)
