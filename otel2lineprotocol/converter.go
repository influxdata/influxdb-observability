package otel2lineprotocol

import (
	"encoding/json"
	"errors"
	"fmt"
	"strconv"
	"sync"

	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlpresource "go.opentelemetry.io/proto/otlp/resource/v1"
)

type OpenTelemetryToLineProtocolConverter struct {
	encoderPool sync.Pool
	logger      Logger
}

func NewOpenTelemetryToLineProtocolConverter(logger Logger) *OpenTelemetryToLineProtocolConverter {
	converter := &OpenTelemetryToLineProtocolConverter{
		encoderPool: sync.Pool{
			New: func() interface{} {
				e := new(lineprotocol.Encoder)
				e.SetLax(true)
				e.SetPrecision(lineprotocol.Nanosecond)
				return e
			},
		},
		logger: &errorLogger{logger},
	}

	return converter
}

func (c *OpenTelemetryToLineProtocolConverter) resourceToTags(resource *otlpresource.Resource, encoder *lineprotocol.Encoder) {
	droppedAttributesCount := uint64(resource.DroppedAttributesCount)
	for _, attribute := range resource.Attributes {
		if k := attribute.Key; k == "" {
			droppedAttributesCount++
			c.logger.Debug("resource attribute key is empty")
		} else if v, err := otlpValueToString(attribute.Value); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid resource attribute value", "key", k, err)
		} else {
			encoder.AddTag(k, v)
		}
	}
	if droppedAttributesCount > 0 {
		encoder.AddField(attributeDroppedResourceAttributesCount, lineprotocol.UintValue(droppedAttributesCount))
	}
	return
}

func instrumentationLibraryToTags(instrumentationLibrary *otlpcommon.InstrumentationLibrary, encoder *lineprotocol.Encoder) {
	if instrumentationLibrary.Name != "" {
		encoder.AddTag(attributeInstrumentationLibraryName, instrumentationLibrary.Name)
	}
	if instrumentationLibrary.Version != "" {
		encoder.AddTag(attributeInstrumentationLibraryVersion, instrumentationLibrary.Version)
	}
}

func otlpValueToString(value *otlpcommon.AnyValue) (string, error) {
	if value == nil {
		return "", errors.New("value is nil")
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
		return "", errors.New("value is nil")
	default:
		return "", fmt.Errorf("unknown value type %T", value.Value)
	}
}

func otlpValueToLPV(value *otlpcommon.AnyValue) (lineprotocol.Value, error) {
	if value == nil {
		return lineprotocol.Value{}, errors.New("value is nil")
	}
	switch value.Value.(type) {
	case *otlpcommon.AnyValue_StringValue:
		if v, ok := lineprotocol.StringValue(value.GetStringValue()); !ok {
			return lineprotocol.Value{}, fmt.Errorf("string value invalid %q", value.String())
		} else {
			return v, nil
		}
	case *otlpcommon.AnyValue_IntValue:
		return lineprotocol.IntValue(value.GetIntValue()), nil
	case *otlpcommon.AnyValue_DoubleValue:
		if v, ok := lineprotocol.FloatValue(value.GetDoubleValue()); !ok {
			return lineprotocol.Value{}, nil
		} else {
			return v, nil
		}
	case *otlpcommon.AnyValue_BoolValue:
		return lineprotocol.BoolValue(value.GetBoolValue()), nil
	case *otlpcommon.AnyValue_KvlistValue:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.GetKvlistValue())); err != nil {
			return lineprotocol.Value{}, err
		} else if v, ok := lineprotocol.StringValueFromBytes(jsonBytes); !ok {
			return lineprotocol.Value{}, fmt.Errorf("map value invalid as serialized JSON bytes %q", value.String())
		} else {
			return v, nil
		}
	case *otlpcommon.AnyValue_ArrayValue:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.GetArrayValue())); err != nil {
			return lineprotocol.Value{}, err
		} else if v, ok := lineprotocol.StringValueFromBytes(jsonBytes); !ok {
			return lineprotocol.Value{}, fmt.Errorf("array value invalid as serialized JSON bytes %q", value.String())
		} else {
			return v, nil
		}
	case nil:
		return lineprotocol.Value{}, errors.New("value is nil")
	default:
		return lineprotocol.Value{}, fmt.Errorf("unknown value type %T", value.Value)
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
