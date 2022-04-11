package common

import (
	"encoding/json"
	"fmt"
	"regexp"
	"strconv"

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

func ResourceToTags(logger Logger, resource pdata.Resource, tags map[string]string) (tagsAgain map[string]string) {
	resource.Attributes().Range(func(k string, v pdata.Value) bool {
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

func InstrumentationLibraryToTags(instrumentationLibrary pdata.InstrumentationScope, tags map[string]string) (tagsAgain map[string]string) {
	if instrumentationLibrary.Name() != "" {
		tags[AttributeInstrumentationLibraryName] = instrumentationLibrary.Name()
	}
	if instrumentationLibrary.Version() != "" {
		tags[AttributeInstrumentationLibraryVersion] = instrumentationLibrary.Version()
	}
	return tags
}

func AttributeValueToInfluxTagValue(value pdata.Value) (string, error) {
	switch value.Type() {
	case pdata.ValueTypeString:
		return value.StringVal(), nil
	case pdata.ValueTypeInt:
		return strconv.FormatInt(value.IntVal(), 10), nil
	case pdata.ValueTypeDouble:
		return strconv.FormatFloat(value.DoubleVal(), 'f', -1, 64), nil
	case pdata.ValueTypeBool:
		return strconv.FormatBool(value.BoolVal()), nil
	case pdata.ValueTypeMap:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.MapVal())); err != nil {
			return "", err
		} else {
			return string(jsonBytes), nil
		}
	case pdata.ValueTypeSlice:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.SliceVal())); err != nil {
			return "", err
		} else {
			return string(jsonBytes), nil
		}
	case pdata.ValueTypeEmpty:
		return "", nil
	default:
		return "", fmt.Errorf("unknown value type %d", value.Type())
	}
}

func AttributeValueToInfluxFieldValue(value pdata.Value) (interface{}, error) {
	switch value.Type() {
	case pdata.ValueTypeString:
		return value.StringVal(), nil
	case pdata.ValueTypeInt:
		return value.IntVal(), nil
	case pdata.ValueTypeDouble:
		return value.DoubleVal(), nil
	case pdata.ValueTypeBool:
		return value.BoolVal(), nil
	case pdata.ValueTypeMap:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.MapVal())); err != nil {
			return nil, err
		} else {
			return string(jsonBytes), nil
		}
	case pdata.ValueTypeSlice:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.SliceVal())); err != nil {
			return nil, err
		} else {
			return string(jsonBytes), nil
		}
	case pdata.ValueTypeEmpty:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown value type %v", value)
	}
}

func otlpKeyValueListToMap(kvList pdata.Map) map[string]interface{} {
	m := make(map[string]interface{}, kvList.Len())
	kvList.Range(func(k string, v pdata.Value) bool {
		switch v.Type() {
		case pdata.ValueTypeString:
			m[k] = v.StringVal()
		case pdata.ValueTypeInt:
			m[k] = v.IntVal()
		case pdata.ValueTypeDouble:
			m[k] = v.DoubleVal()
		case pdata.ValueTypeBool:
			m[k] = v.BoolVal()
		case pdata.ValueTypeMap:
			m[k] = otlpKeyValueListToMap(v.MapVal())
		case pdata.ValueTypeSlice:
			m[k] = otlpArrayToSlice(v.SliceVal())
		case pdata.ValueTypeEmpty:
			m[k] = nil
		default:
			m[k] = fmt.Sprintf("<invalid map value> %v", v)
		}
		return true
	})
	return m
}

func otlpArrayToSlice(arr pdata.Slice) []interface{} {
	s := make([]interface{}, 0, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		v := arr.At(i)
		switch v.Type() {
		case pdata.ValueTypeString:
			s = append(s, v.StringVal())
		case pdata.ValueTypeInt:
			s = append(s, v.IntVal())
		case pdata.ValueTypeDouble:
			s = append(s, v.DoubleVal())
		case pdata.ValueTypeBool:
			s = append(s, v.BoolVal())
		case pdata.ValueTypeEmpty:
			s = append(s, nil)
		default:
			s = append(s, fmt.Sprintf("<invalid array value> %v", v))
		}
	}
	return s
}
