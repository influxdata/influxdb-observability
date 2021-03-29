package otel2influx

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlplogs "go.opentelemetry.io/proto/otlp/logs/v1"
	otlpresource "go.opentelemetry.io/proto/otlp/resource/v1"
)

func (c *OpenTelemetryToInfluxConverter) WriteLogs(ctx context.Context, resourceLogss []*otlplogs.ResourceLogs, w InfluxWriter) (droppedLogRecords int) {
	for _, resourceLogs := range resourceLogss {
		resource := resourceLogs.Resource
		for _, ilLogs := range resourceLogs.InstrumentationLibraryLogs {
			instrumentationLibrary := ilLogs.InstrumentationLibrary
			for _, logRecord := range ilLogs.Logs {
				if err := c.writeLogRecord(ctx, resource, instrumentationLibrary, logRecord, w); err != nil {
					droppedLogRecords++
					c.logger.Debug("failed to convert log record to InfluxDB point", err)
				}
			}
		}
	}
	return
}

func (c *OpenTelemetryToInfluxConverter) writeLogRecord(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, logRecord *otlplogs.LogRecord, w InfluxWriter) error {
	ts := time.Unix(0, int64(logRecord.TimeUnixNano))
	if ts.IsZero() {
		// This is a valid condition in OpenTelemetry, but not in InfluxDB.
		// From otel proto field Logrecord.time_unix_name:
		// "Value of 0 indicates unknown or missing timestamp."
		return errors.New("log record has no time stamp")
	}

	measurement := measurementLogs
	tags := make(map[string]string)
	fields := make(map[string]interface{})

	// TODO handle logRecord.Flags()
	var droppedResourceAttributesCount uint64
	tags, droppedResourceAttributesCount = c.resourceToTags(resource, tags)
	if droppedResourceAttributesCount > 0 {
		fields[attributeDroppedResourceAttributesCount] = droppedResourceAttributesCount
	}
	tags = c.instrumentationLibraryToTags(instrumentationLibrary, tags)

	if name := logRecord.Name; name != "" {
		fields[attributeName] = name
	}
	if traceID := hex.EncodeToString(logRecord.TraceId); len(traceID) > 0 {
		tags[attributeTraceID] = traceID
		if spanID := hex.EncodeToString(logRecord.SpanId); len(spanID) > 0 {
			tags[attributeSpanID] = spanID
		}
	}

	if severityNumber := logRecord.SeverityNumber; severityNumber != otlplogs.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED {
		fields[attributeSeverityNumber] = int64(severityNumber)
	}
	if severityText := logRecord.SeverityText; severityText != "" {
		fields[attributeSeverityText] = severityText
	}
	if v, err := otlpValueToInfluxFieldValue(logRecord.Body); err != nil {
		c.logger.Debug("invalid log record body", err)
		fields[attributeBody] = nil
	} else {
		fields[attributeBody] = v
	}

	droppedAttributesCount := uint64(logRecord.DroppedAttributesCount)
	for _, attribute := range logRecord.Attributes {
		if k := attribute.Key; k == "" {
			droppedAttributesCount++
			c.logger.Debug("log record attribute key is empty")
		} else if v, err := otlpValueToInfluxFieldValue(attribute.Value); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid log record attribute value", err)
		} else {
			fields[k] = v
		}
	}
	if droppedAttributesCount > 0 {
		fields[attributeDroppedSpanAttributesCount] = droppedAttributesCount
	}

	if err := w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
		return fmt.Errorf("failed to write point for int gauge: %w", err)
	}

	return nil
}
