package otel2influx

import (
	"encoding/json"
	"fmt"
	"strconv"

	"github.com/influxdata/influxdb-observability/common"
	"go.opentelemetry.io/collector/model/pdata"
)

func ResourceToTags(logger common.Logger, resource pdata.Resource, tags map[string]string) (tagsAgain map[string]string) {
	resource.Attributes().Range(func(k string, v pdata.AttributeValue) bool {
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

func InstrumentationLibraryToTags(instrumentationLibrary pdata.InstrumentationLibrary, tags map[string]string) (tagsAgain map[string]string) {
	if instrumentationLibrary.Name() != "" {
		tags[common.AttributeInstrumentationLibraryName] = instrumentationLibrary.Name()
	}
	if instrumentationLibrary.Version() != "" {
		tags[common.AttributeInstrumentationLibraryVersion] = instrumentationLibrary.Version()
	}
	return tags
}

func AttributeValueToInfluxTagValue(value pdata.AttributeValue) (string, error) {
	switch value.Type() {
	case pdata.AttributeValueTypeString:
		return value.StringVal(), nil
	case pdata.AttributeValueTypeInt:
		return strconv.FormatInt(value.IntVal(), 10), nil
	case pdata.AttributeValueTypeDouble:
		return strconv.FormatFloat(value.DoubleVal(), 'f', -1, 64), nil
	case pdata.AttributeValueTypeBool:
		return strconv.FormatBool(value.BoolVal()), nil
	case pdata.AttributeValueTypeMap:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.MapVal())); err != nil {
			return "", err
		} else {
			return string(jsonBytes), nil
		}
	case pdata.AttributeValueTypeArray:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.SliceVal())); err != nil {
			return "", err
		} else {
			return string(jsonBytes), nil
		}
	case pdata.AttributeValueTypeEmpty:
		return "", nil
	default:
		return "", fmt.Errorf("unknown value type %d", value.Type())
	}
}

func AttributeValueToInfluxFieldValue(value pdata.AttributeValue) (interface{}, error) {
	switch value.Type() {
	case pdata.AttributeValueTypeString:
		return value.StringVal(), nil
	case pdata.AttributeValueTypeInt:
		return value.IntVal(), nil
	case pdata.AttributeValueTypeDouble:
		return value.DoubleVal(), nil
	case pdata.AttributeValueTypeBool:
		return value.BoolVal(), nil
	case pdata.AttributeValueTypeMap:
		if jsonBytes, err := json.Marshal(otlpKeyValueListToMap(value.MapVal())); err != nil {
			return nil, err
		} else {
			return string(jsonBytes), nil
		}
	case pdata.AttributeValueTypeArray:
		if jsonBytes, err := json.Marshal(otlpArrayToSlice(value.SliceVal())); err != nil {
			return nil, err
		} else {
			return string(jsonBytes), nil
		}
	case pdata.AttributeValueTypeEmpty:
		return nil, nil
	default:
		return nil, fmt.Errorf("unknown value type %v", value)
	}
}

func otlpKeyValueListToMap(kvList pdata.AttributeMap) map[string]interface{} {
	m := make(map[string]interface{}, kvList.Len())
	kvList.Range(func(k string, v pdata.AttributeValue) bool {
		switch v.Type() {
		case pdata.AttributeValueTypeString:
			m[k] = v.StringVal()
		case pdata.AttributeValueTypeInt:
			m[k] = v.IntVal()
		case pdata.AttributeValueTypeDouble:
			m[k] = v.DoubleVal()
		case pdata.AttributeValueTypeBool:
			m[k] = v.BoolVal()
		case pdata.AttributeValueTypeMap:
			m[k] = otlpKeyValueListToMap(v.MapVal())
		case pdata.AttributeValueTypeArray:
			m[k] = otlpArrayToSlice(v.SliceVal())
		case pdata.AttributeValueTypeEmpty:
			m[k] = nil
		default:
			m[k] = fmt.Sprintf("<invalid map value> %v", v)
		}
		return true
	})
	return m
}

func otlpArrayToSlice(arr pdata.AttributeValueSlice) []interface{} {
	s := make([]interface{}, 0, arr.Len())
	for i := 0; i < arr.Len(); i++ {
		v := arr.At(i)
		switch v.Type() {
		case pdata.AttributeValueTypeString:
			s = append(s, v.StringVal())
		case pdata.AttributeValueTypeInt:
			s = append(s, v.IntVal())
		case pdata.AttributeValueTypeDouble:
			s = append(s, v.DoubleVal())
		case pdata.AttributeValueTypeBool:
			s = append(s, v.BoolVal())
		case pdata.AttributeValueTypeEmpty:
			s = append(s, nil)
		default:
			s = append(s, fmt.Sprintf("<invalid array value> %v", v))
		}
	}
	return s
}
