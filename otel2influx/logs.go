package otel2influx

import (
	"context"
	"errors"
	"fmt"

	"github.com/influxdata/influxdb-observability/common"
	"go.opentelemetry.io/collector/model/pdata"
)

type OtelLogsToLineProtocol struct {
	logger common.Logger
}

func NewOtelLogsToLineProtocol(logger common.Logger) *OtelLogsToLineProtocol {
	return &OtelLogsToLineProtocol{
		logger: logger,
	}
}

func (c *OtelLogsToLineProtocol) WriteLogs(ctx context.Context, ld pdata.Logs, w InfluxWriter) error {
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLogs := ld.ResourceLogs().At(i)
		for j := 0; j < resourceLogs.InstrumentationLibraryLogs().Len(); j++ {
			ilLogs := resourceLogs.InstrumentationLibraryLogs().At(j)
			for k := 0; k < ilLogs.Logs().Len(); k++ {
				logRecord := ilLogs.Logs().At(k)
				if err := c.writeLogRecord(ctx, resourceLogs.Resource(), ilLogs.InstrumentationLibrary(), logRecord, w); err != nil {
					return fmt.Errorf("failed to convert OTLP log record to line protocol: %w", err)
				}
			}
		}
	}
	return nil
}

func (c *OtelLogsToLineProtocol) writeLogRecord(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, logRecord pdata.LogRecord, w InfluxWriter) error {
	ts := logRecord.Timestamp().AsTime()
	if ts.IsZero() {
		// This is a valid condition in OpenTelemetry, but not in InfluxDB.
		// From otel proto field Logrecord.time_unix_name:
		// "Value of 0 indicates unknown or missing timestamp."
		return errors.New("log record has no time stamp")
	}

	measurement := common.MeasurementLogs
	tags := make(map[string]string)
	fields := make(map[string]interface{})

	// TODO handle logRecord.Flags()
	tags = ResourceToTags(c.logger, resource, tags)
	tags = InstrumentationLibraryToTags(instrumentationLibrary, tags)

	if name := logRecord.Name(); name != "" {
		fields[common.AttributeName] = name
	}
	if traceID := logRecord.TraceID(); !traceID.IsEmpty() {
		tags[common.AttributeTraceID] = traceID.HexString()
		if spanID := logRecord.SpanID(); !spanID.IsEmpty() {
			tags[common.AttributeSpanID] = spanID.HexString()
		}
	}

	if severityNumber := logRecord.SeverityNumber(); severityNumber != pdata.SeverityNumberUNDEFINED {
		fields[common.AttributeSeverityNumber] = int64(severityNumber)
	}
	if severityText := logRecord.SeverityText(); severityText != "" {
		fields[common.AttributeSeverityText] = severityText
	}
	if v, err := AttributeValueToInfluxFieldValue(logRecord.Body()); err != nil {
		c.logger.Debug("invalid log record body", err)
		fields[common.AttributeBody] = nil
	} else {
		fields[common.AttributeBody] = v
	}

	droppedAttributesCount := uint64(logRecord.DroppedAttributesCount())
	logRecord.Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		if k == "" {
			droppedAttributesCount++
			c.logger.Debug("log record attribute key is empty")
		} else if v, err := AttributeValueToInfluxFieldValue(v); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid log record attribute value", err)
		} else {
			fields[k] = v
		}
		return true
	})
	if droppedAttributesCount > 0 {
		fields[common.AttributeDroppedSpanAttributesCount] = droppedAttributesCount
	}

	if err := w.WritePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
		return fmt.Errorf("failed to write point for int gauge: %w", err)
	}

	return nil
}
