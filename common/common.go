package common

import (
	"regexp"
	"strings"

	"go.opentelemetry.io/collector/model/pdata"
)

// https://github.com/open-telemetry/opentelemetry-specification/tree/v1.0.1/specification/resource/semantic_conventions
var ResourceNamespace = regexp.MustCompile(`^(service\.|telemetry\.|container\.|process\.|host\.|os\.|cloud\.|deployment\.|k8s\.|aws\.|gcp\.|azure\.|faas\.name|faas\.id|faas\.version|faas\.instance|faas\.max_memory)`)

const (
	MeasurementSpans      = "spans"
	MeasurementSpanLinks  = "span-links"
	MeasurementLogs       = "logs"
	MeasurementPrometheus = "prometheus"

	MetricGaugeFieldKey          = "gauge"
	MetricCounterFieldKey        = "counter"
	MetricHistogramCountFieldKey = "count"
	MetricHistogramSumFieldKey   = "sum"
	MetricHistogramInfFieldKey   = "inf"
	MetricHistogramBoundKeyV2    = "le"
	MetricHistogramCountSuffix   = "_count"
	MetricHistogramSumSuffix     = "_sum"
	MetricHistogramBucketSuffix  = "_bucket"
	MetricSummaryCountFieldKey   = "count"
	MetricSummarySumFieldKey     = "sum"
	MetricSummaryQuantileKeyV2   = "quantile"
	MetricSummaryCountSuffix     = "_count"
	MetricSummarySumSuffix       = "_sum"

	// These attribute key names are influenced by the proto message keys.
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/trace/v1/trace.proto
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/metrics/v1/metrics.proto
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/logs/v1/logs.proto
	AttributeTraceID                        = "trace_id"
	AttributeSpanID                         = "span_id"
	AttributeTraceState                     = "trace_state"
	AttributeParentSpanID                   = "parent_span_id"
	AttributeName                           = "name"
	AttributeSpanKind                       = "kind"
	AttributeEndTimeUnixNano                = "end_time_unix_nano"
	AttributeDurationNano                   = "duration_nano"
	AttributeDroppedResourceAttributesCount = "otel.resource.dropped_attributes_count"
	AttributeDroppedSpanAttributesCount     = "otel.span.dropped_attributes_count"
	AttributeDroppedEventAttributesCount    = "otel.event.dropped_attributes_count"
	AttributeDroppedEventsCount             = "otel.span.dropped_events_count"
	AttributeDroppedLinkAttributesCount     = "otel.link.dropped_attributes_count"
	AttributeDroppedLinksCount              = "otel.span.dropped_links_count"
	AttributeLinkedTraceID                  = "linked_trace_id"
	AttributeLinkedSpanID                   = "linked_span_id"
	AttributeStatusCode                     = "otel.status_code"
	AttributeStatusCodeOK                   = "OK"
	AttributeStatusCodeError                = "ERROR"
	AttributeStatusMessage                  = "otel.status_description"
	AttributeInstrumentationLibraryName     = "otel.library.name"
	AttributeInstrumentationLibraryVersion  = "otel.library.version"
	AttributeSeverityNumber                 = "severity_number"
	AttributeSeverityText                   = "severity_text"
	AttributeBody                           = "body"
)

func ResourceAttributesToKey(rAttributes pdata.AttributeMap) string {
	var key strings.Builder
	rAttributes.Range(func(k string, v pdata.AttributeValue) bool {
		key.WriteString(k)
		key.WriteByte(':')
		return true
	})
	return key.String()
}
