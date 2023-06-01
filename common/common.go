package common

import (
	"regexp"
	"strings"

	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"
)

// https://github.com/open-telemetry/opentelemetry-specification/tree/v1.16.0/specification/resource/semantic_conventions
var ResourceNamespace = regexp.MustCompile(generateResourceNamespaceRegexp())

func generateResourceNamespaceRegexp() string {
	semconvResourceAttributeNames := semconv.GetResourceSemanticConventionAttributeNames()
	components := make([]string, len(semconvResourceAttributeNames))
	for i, attributeName := range semconvResourceAttributeNames {
		components[i] = strings.ReplaceAll(attributeName, `.`, `\.`)
	}
	return `^(?:` + strings.Join(components, `|`) + `)(?:\.[a-z0-9]+)*$`
}

const (
	MeasurementSpans      = "spans"
	MeasurementSpanLinks  = "span-links"
	MeasurementLogs       = "logs"
	MeasurementPrometheus = "prometheus"

	MetricGaugeFieldKey          = "gauge"
	MetricCounterFieldKey        = "counter"
	MetricHistogramCountFieldKey = "count"
	MetricHistogramSumFieldKey   = "sum"
	MetricHistogramMinFieldKey   = "min"
	MetricHistogramMaxFieldKey   = "max"
	MetricHistogramInfFieldKey   = "inf"
	MetricHistogramBoundKeyV2    = "le"
	MetricHistogramCountSuffix   = "_count"
	MetricHistogramSumSuffix     = "_sum"
	MetricHistogramBucketSuffix  = "_bucket"
	MetricHistogramMinSuffix     = "_min"
	MetricHistogramMaxSuffix     = "_max"
	MetricSummaryCountFieldKey   = "count"
	MetricSummarySumFieldKey     = "sum"
	MetricSummaryQuantileKeyV2   = "quantile"
	MetricSummaryCountSuffix     = "_count"
	MetricSummarySumSuffix       = "_sum"

	// These attribute key names are influenced by the proto message keys.
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/trace/v1/trace.proto
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/metrics/v1/metrics.proto
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/logs/v1/logs.proto
	AttributeTime                   = "time"
	AttributeStartTimeUnixNano      = "start_time_unix_nano"
	AttributeTraceID                = "trace_id"
	AttributeSpanID                 = "span_id"
	AttributeTraceState             = "trace_state"
	AttributeParentSpanID           = "parent_span_id"
	AttributeSpanName               = "span.name" // https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.78.0/connector/spanmetricsconnector/connector.go#L30
	AttributeSpanKind               = "span.kind" // https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.78.0/connector/spanmetricsconnector/connector.go#L31
	AttributeEndTimeUnixNano        = "end_time_unix_nano"
	AttributeDurationNano           = "duration_nano"
	AttributeDroppedAttributesCount = "dropped_attributes_count"
	AttributeDroppedEventsCount     = "dropped_events_count"
	AttributeDroppedLinksCount      = "dropped_links_count"
	AttributeAttributes             = "attributes"
	AttributeLinkedTraceID          = "linked_trace_id"
	AttributeLinkedSpanID           = "linked_span_id"
	AttributeSeverityNumber         = "severity_number"
	AttributeSeverityText           = "severity_text"
	AttributeBody                   = "body"
)
