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
	attributeDroppedResourceAttributesCount = "dropped_resource_attributes_count"
	attributeDroppedAttributesCount         = "dropped_attributes_count"
	attributeDroppedEventsCount             = "dropped_events_count"
	attributeLinkedTraceID                  = "linked_trace_id"
	attributeLinkedSpanID                   = "linked_span_id"
	attributeDroppedLinksCount              = "dropped_links_count"
	attributeStatusCode                     = "status_code"
	attributeStatusMessage                  = "status_message"
	attributeInstrumentationLibraryName     = "instrumentation_library_name"
	attributeInstrumentationLibraryVersion  = "instrumentation_library_version"
	attributeSeverityNumber                 = "severity_number"
	attributeSeverityText                   = "severity_text"
	attributeBody                           = "body"
)
