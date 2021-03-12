package otel2lineprotocol

import (
	"encoding/json"
	"fmt"
	"sync"

	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
	"go.opentelemetry.io/collector/consumer/pdata"
	tracetranslator "go.opentelemetry.io/collector/translator/trace"
	"go.uber.org/zap"
)

type OpenTelemetryToLineProtocolConverter struct {
	encoderPool sync.Pool

	logger *zap.Logger
}

func NewOpenTelemetryToLineProtocolConverter(logger *zap.Logger) *OpenTelemetryToLineProtocolConverter {
	converter := &OpenTelemetryToLineProtocolConverter{
		encoderPool: sync.Pool{
			New: func() interface{} {
				e := new(lineprotocol.Encoder)
				e.SetLax(true)
				e.SetPrecision(lineprotocol.Nanosecond)
				return e
			},
		},
		logger: logger,
	}

	return converter
}

func (c *OpenTelemetryToLineProtocolConverter) resourceToTags(resource pdata.Resource, encoder *lineprotocol.Encoder) {
	attributes := resource.Attributes()
	attributes.ForEach(func(k string, v pdata.AttributeValue) {
		encoder.AddTag(k, tracetranslator.AttributeValueToString(v, false))
	})
}

func (c *OpenTelemetryToLineProtocolConverter) instrumentationLibraryToTags(instrumentationLibrary pdata.InstrumentationLibrary, encoder *lineprotocol.Encoder) {
	if instrumentationLibrary.Name() != "" {
		if instrumentationLibrary.Version() != "" {
			encoder.AddTag(attributeInstrumentationLibraryName, instrumentationLibrary.Name())
			encoder.AddTag(attributeInstrumentationLibraryVersion, instrumentationLibrary.Version())

		} else {
			encoder.AddTag(attributeInstrumentationLibraryName, instrumentationLibrary.Name())
		}

	} else if instrumentationLibrary.Version() != "" {
		encoder.AddTag(attributeInstrumentationLibraryVersion, instrumentationLibrary.Version())
	}
}

func (c *OpenTelemetryToLineProtocolConverter) attributeValueToLPV(value pdata.AttributeValue) (lineprotocol.Value, error) {
	switch value.Type() {
	case pdata.AttributeValueNULL:
		return lineprotocol.Value{}, nil
	case pdata.AttributeValueSTRING:
		return lineprotocol.MustNewValue(value.StringVal()), nil
	case pdata.AttributeValueINT:
		return lineprotocol.MustNewValue(value.IntVal()), nil
	case pdata.AttributeValueDOUBLE:
		return lineprotocol.MustNewValue(value.DoubleVal()), nil
	case pdata.AttributeValueBOOL:
		return lineprotocol.MustNewValue(value.BoolVal()), nil
	case pdata.AttributeValueMAP:
		jsonStr, err := json.Marshal(tracetranslator.AttributeMapToMap(value.MapVal()))
		if err != nil {
			return lineprotocol.Value{}, err
		}
		return lineprotocol.MustNewValue(string(jsonStr)), nil
	case pdata.AttributeValueARRAY:
		jsonStr, err := json.Marshal(tracetranslator.AttributeArrayToSlice(value.ArrayVal()))
		if err != nil {
			return lineprotocol.Value{}, err
		}
		return lineprotocol.MustNewValue(string(jsonStr)), nil
	default:
		return lineprotocol.Value{}, fmt.Errorf("Unknown attribute value type %q", value.Type())
	}
}
