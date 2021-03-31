package otel2influx

import (
	"encoding/json"
	"fmt"
	"strconv"

	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
)

type OpenTelemetryToInfluxConverter struct {
	logger Logger
}

func NewOpenTelemetryToInfluxConverter(logger Logger) *OpenTelemetryToInfluxConverter {
	return &OpenTelemetryToInfluxConverter{
		&errorLogger{logger},
	}
}

func (c *OpenTelemetryToInfluxConverter) resourceToTags(resource *otlpresource.Resource, tags map[string]string) (tagsAgain map[string]string, droppedAttributesCount uint64) {
	droppedAttributesCount = uint64(resource.DroppedAttributesCount)
	for _, attribute := range resource.Attributes {
		if k := attribute.Key; k == "" {
			droppedAttributesCount++
			c.logger.Debug("resource attribute key is empty")
		} else if v, err := otlpValueToInfluxTagValue(attribute.Value); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid resource attribute value", "key", k, err)
		} else {
			tags[k] = v
		}
	}
	return tags, droppedAttributesCount
}

func (c *OpenTelemetryToInfluxConverter) instrumentationLibraryToTags(instrumentationLibrary *otlpcommon.InstrumentationLibrary, tags map[string]string) (tagsAgain map[string]string) {
	if instrumentationLibrary.Name != "" {
		tags[attributeInstrumentationLibraryName] = instrumentationLibrary.Name
	}
	if instrumentationLibrary.Version != "" {
		tags[attributeInstrumentationLibraryVersion] = instrumentationLibrary.Version
	}
	return tags
}

func otlpValueToInfluxTagValue(value *otlpcommon.AnyValue) (string, error) {
	if value == nil {
		return "", nil
	}
	switch value.Value.(type) {
	case *otlpcommon.AnyValue_StringValue:
		return value.GetStringValue(), nil
	case *otlpcommon.AnyValue_IntValue:
		return strconv.FormatInt(value.GetIntValue(), 10), nil
	case *otlpcommon.AnyValue_DoubleValue:
		return strconv.FormatFloat(value.GetDoubleValue(), 'f', -1, 64), nil
	case *otlpcommon.AnyValue_BoolValue:
		return strconv.FormatBool(value.GetBoolValue()), nil
	case *otlpcommon.AnyValue_KvlistValue:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.GetKvlistValue())); err != nil {
			return "", err
		} else {
			return string(jsonBytes), nil
		}
	case *otlpcommon.AnyValue_ArrayValue:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.GetArrayValue())); err != nil {
			return "", err
		} else {
			return string(jsonBytes), nil
		}
	case nil:
		return "", nil
	default:
		return "", fmt.Errorf("unknown value type %T", value.Value)
	}
}

func otlpValueToInfluxFieldValue(value *otlpcommon.AnyValue) (interface{}, error) {
	if value == nil {
		return nil, nil
	}
	switch value.Value.(type) {
	case *otlpcommon.AnyValue_StringValue:
		return value.GetStringValue(), nil
	case *otlpcommon.AnyValue_IntValue:
		return value.GetIntValue(), nil
	case *otlpcommon.AnyValue_DoubleValue:
		return value.GetDoubleValue(), nil
	case *otlpcommon.AnyValue_BoolValue:
		return value.GetBoolValue(), nil
	case *otlpcommon.AnyValue_KvlistValue:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.GetKvlistValue())); err != nil {
			return nil, err
		} else {
			return string(jsonBytes), nil
		}
	case *otlpcommon.AnyValue_ArrayValue:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.GetArrayValue())); err != nil {
			return nil, err
		} else {
			return string(jsonBytes), nil
		}
	case nil:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown value type %T", value.Value)
	}
}

func otlpKeyValueListToMap(kvList *otlpcommon.KeyValueList) map[string]interface{} {
	if kvList == nil {
		return map[string]interface{}{}
	}
	m := make(map[string]interface{}, len(kvList.Values))
	for _, kv := range kvList.Values {
		switch kv.Value.Value.(type) {
		case *otlpcommon.AnyValue_StringValue:
			m[kv.Key] = kv.Value.GetStringValue()
		case *otlpcommon.AnyValue_IntValue:
			m[kv.Key] = kv.Value.GetIntValue()
		case *otlpcommon.AnyValue_DoubleValue:
			m[kv.Key] = kv.Value.GetDoubleValue()
		case *otlpcommon.AnyValue_BoolValue:
			m[kv.Key] = kv.Value.GetBoolValue()
		case *otlpcommon.AnyValue_KvlistValue:
			m[kv.Key] = otlpKeyValueListToMap(kv.Value.GetKvlistValue())
		case *otlpcommon.AnyValue_ArrayValue:
			m[kv.Key] = otlpArrayToSlice(kv.Value.GetArrayValue())
		case nil:
			m[kv.Key] = nil
		default:
			m[kv.Key] = fmt.Sprintf("<invalid map value> %q", kv.Value.String())
		}
	}
	return m
}

func otlpArrayToSlice(arr *otlpcommon.ArrayValue) []interface{} {
	if arr == nil {
		return nil
	}
	s := make([]interface{}, 0, len(arr.Values))
	for _, value := range arr.Values {
		switch value.Value.(type) {
		case *otlpcommon.AnyValue_StringValue:
			s = append(s, value.GetStringValue())
		case *otlpcommon.AnyValue_IntValue:
			s = append(s, value.GetIntValue())
		case *otlpcommon.AnyValue_DoubleValue:
			s = append(s, value.GetDoubleValue())
		case *otlpcommon.AnyValue_BoolValue:
			s = append(s, value.GetBoolValue())
		case nil:
			s = append(s, nil)
		default:
			s = append(s, fmt.Sprintf("<invalid array value> %q", value.String()))
		}
	}
	return s
}
