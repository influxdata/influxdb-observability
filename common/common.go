package common

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

	"go.opentelemetry.io/collector/pdata/pcommon"
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

func ResourceToTags(logger Logger, resource pcommon.Resource, tags map[string]string) (tagsAgain map[string]string) {
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			logger.Debug("resource attribute key is empty")
		} else if v, err := AttributeValueToInfluxTagValue(v); err != nil {
			logger.Debug("invalid resource attribute value", "key", k, err)
		} else {
			tags[k] = v
		}
		return true
	})
	return tags
}

func InstrumentationLibraryToTags(instrumentationLibrary pcommon.InstrumentationScope, tags map[string]string) (tagsAgain map[string]string) {
	if instrumentationLibrary.Name() != "" {
		tags[AttributeInstrumentationLibraryName] = instrumentationLibrary.Name()
	}
	if instrumentationLibrary.Version() != "" {
		tags[AttributeInstrumentationLibraryVersion] = instrumentationLibrary.Version()
	}
	return tags
}

func AttributeValueToInfluxTagValue(value pcommon.Value) (string, error) {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		return value.Str(), nil
	case pcommon.ValueTypeInt:
		return strconv.FormatInt(value.Int(), 10), nil
	case pcommon.ValueTypeDouble:
		return strconv.FormatFloat(value.Double(), 'f', -1, 64), nil
	case pcommon.ValueTypeBool:
		return strconv.FormatBool(value.Bool()), nil
	case pcommon.ValueTypeMap:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.Map())); err != nil {
			return "", err
		} else {
			return string(jsonBytes), nil
		}
	case pcommon.ValueTypeSlice:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.Slice())); err != nil {
			return "", err
		} else {
			return string(jsonBytes), nil
		}
	case pcommon.ValueTypeEmpty:
		return "", nil
	default:
		return "", fmt.Errorf("unknown value type %d", value.Type())
	}
}

func AttributeValueToInfluxFieldValue(value pcommon.Value) (interface{}, error) {
	switch value.Type() {
	case pcommon.ValueTypeStr:
		return value.Str(), nil
	case pcommon.ValueTypeInt:
		return value.Int(), nil
	case pcommon.ValueTypeDouble:
		return value.Double(), nil
	case pcommon.ValueTypeBool:
		return value.Bool(), nil
	case pcommon.ValueTypeMap:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.Map())); err != nil {
			return nil, err
		} else {
			return string(jsonBytes), nil
		}
	case pcommon.ValueTypeSlice:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.Slice())); err != nil {
			return nil, err
		} else {
			return string(jsonBytes), nil
		}
	case pcommon.ValueTypeEmpty:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown value type %v", value)
	}
}

func otlpKeyValueListToMap(kvList pcommon.Map) map[string]interface{} {
	m := make(map[string]interface{}, kvList.Len())
	kvList.Range(func(k string, v pcommon.Value) bool {
		switch v.Type() {
		case pcommon.ValueTypeStr:
			m[k] = v.Str()
		case pcommon.ValueTypeInt:
			m[k] = v.Int()
		case pcommon.ValueTypeDouble:
			m[k] = v.Double()
		case pcommon.ValueTypeBool:
			m[k] = v.Bool()
		case pcommon.ValueTypeMap:
			m[k] = otlpKeyValueListToMap(v.Map())
		case pcommon.ValueTypeSlice:
			m[k] = otlpArrayToSlice(v.Slice())
		case pcommon.ValueTypeEmpty:
			m[k] = nil
		default:
			m[k] = fmt.Sprintf("<invalid map value> %v", v)
		}
		return true
	})
	return m
}

func otlpArrayToSlice(arr pcommon.Slice) []interface{} {
	s := make([]interface{}, 0, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		v := arr.At(i)
		switch v.Type() {
		case pcommon.ValueTypeStr:
			s = append(s, v.Str())
		case pcommon.ValueTypeInt:
			s = append(s, v.Int())
		case pcommon.ValueTypeDouble:
			s = append(s, v.Double())
		case pcommon.ValueTypeBool:
			s = append(s, v.Bool())
		case pcommon.ValueTypeEmpty:
			s = append(s, nil)
		default:
			s = append(s, fmt.Sprintf("<invalid array value> %v", v))
		}
	}
	return s
}
