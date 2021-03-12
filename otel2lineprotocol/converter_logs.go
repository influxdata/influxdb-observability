package otel2lineprotocol

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlplogs "go.opentelemetry.io/proto/otlp/logs/v1"
	otlpresource "go.opentelemetry.io/proto/otlp/resource/v1"
)

func (c *OpenTelemetryToLineProtocolConverter) WriteLogs(resourceLogss []*otlplogs.ResourceLogs, w io.Writer) (droppedLogRecords int) {
	for _, resourceLogs := range resourceLogss {
		resource := resourceLogs.Resource
		for _, ilLogs := range resourceLogs.InstrumentationLibraryLogs {
			instrumentationLibrary := ilLogs.InstrumentationLibrary
			for _, logRecord := range ilLogs.Logs {
				if err := c.writeLogRecord(resource, instrumentationLibrary, logRecord, w); err != nil {
					droppedLogRecords++
					c.logger.Debug("failed to convert log record to InfluxDB point", err)
				}
			}
		}
	}
	return
}

func (c *OpenTelemetryToLineProtocolConverter) writeLogRecord(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, logRecord *otlplogs.LogRecord, w io.Writer) error {
	timestamp := time.Unix(0, int64(logRecord.TimeUnixNano))
	if timestamp.IsZero() {
		// This is a valid condition in OpenTelemetry, but not in InfluxDB.
		// From otel proto field Logrecord.time_unix_name:
		// "Value of 0 indicates unknown or missing timestamp."
		return errors.New("log record has no time stamp")
	}

	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()
	encoder.StartLine(measurementLogs)

	// TODO handle logRecord.Flags()
	c.resourceToTags(resource, encoder)
	instrumentationLibraryToTags(instrumentationLibrary, encoder)

	if name := logRecord.Name; name != "" {
		encoder.AddTag(attributeName, name)
	}
	if traceID := hex.EncodeToString(logRecord.TraceId); len(traceID) > 0 {
		encoder.AddTag(attributeTraceID, traceID)
		if spanID := hex.EncodeToString(logRecord.SpanId); len(spanID) > 0 {
			encoder.AddTag(attributeSpanID, spanID)
		}
	}

	if severityNumber := logRecord.SeverityNumber; severityNumber != otlplogs.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED {
		encoder.AddField(attributeSeverityNumber, lineprotocol.IntValue(int64(severityNumber)))
	}
	if severityText := logRecord.SeverityText; severityText != "" {
		if v, ok := lineprotocol.StringValue(severityText); !ok {
			c.logger.Debug("invalid log record severity text", "severity-text", severityText)
		} else {
			encoder.AddField(attributeSeverityText, v)
		}
	}
	if body := logRecord.Body; body != nil {
		if v, err := otlpValueToLPV(body); err != nil {
			c.logger.Debug("invalid log record body", err)
		} else {
			encoder.AddField(attributeBody, v)
		}
	}

	droppedAttributesCount := uint64(logRecord.DroppedAttributesCount)
	for _, attribute := range logRecord.Attributes {
		if k := attribute.Key; k == "" {
			droppedAttributesCount++
			c.logger.Debug("log record attribute key is empty")
		} else if v, err := otlpValueToLPV(attribute.Value); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid log record attribute value", err)
		} else {
			encoder.AddField(k, v)
		}
	}
	if droppedAttributesCount > 0 {
		encoder.AddField(attributeDroppedAttributesCount, lineprotocol.UintValue(droppedAttributesCount))
	}

	encoder.EndLine(timestamp)
	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		b = append(b, '\n')
		return fmt.Errorf("failed to encode log record: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return err
	}

	return nil
}
