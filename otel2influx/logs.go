package otel2influx

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"

	"github.com/influxdata/influxdb-observability/common"
)

type OtelLogsToLineProtocolConfig struct {
	Logger common.Logger
	Writer InfluxWriter
}

func DefaultOtelLogsToLineProtocolConfig() *OtelLogsToLineProtocolConfig {
	return &OtelLogsToLineProtocolConfig{
		Logger: new(common.NoopLogger),
		Writer: new(NoopInfluxWriter),
	}
}

type OtelLogsToLineProtocol struct {
	logger common.Logger
	writer InfluxWriter
}

func NewOtelLogsToLineProtocol(config *OtelLogsToLineProtocolConfig) (*OtelLogsToLineProtocol, error) {
	return &OtelLogsToLineProtocol{
		logger: config.Logger,
		writer: config.Writer,
	}, nil
}

func (c *OtelLogsToLineProtocol) WriteLogs(ctx context.Context, ld plog.Logs) error {
	batch := c.writer.NewBatch()
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		resourceLogs := ld.ResourceLogs().At(i)
		for j := 0; j < resourceLogs.ScopeLogs().Len(); j++ {
			ilLogs := resourceLogs.ScopeLogs().At(j)
			for k := 0; k < ilLogs.LogRecords().Len(); k++ {
				logRecord := ilLogs.LogRecords().At(k)
				if err := c.enqueueLogRecord(resourceLogs.Resource(), ilLogs.Scope(), logRecord, batch); err != nil {
					return consumererror.NewPermanent(fmt.Errorf("failed to convert OTLP log record to line protocol: %w", err))
				}
			}
		}
	}
	return batch.WriteBatch(ctx)
}

func (c *OtelLogsToLineProtocol) enqueueLogRecord(resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, logRecord plog.LogRecord, batch InfluxWriterBatch) error {
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
	tags = InstrumentationScopeToTags(instrumentationLibrary, tags)

	if traceID := logRecord.TraceID(); !traceID.IsEmpty() {
		tags[common.AttributeTraceID] = hex.EncodeToString(traceID[:])
		if spanID := logRecord.SpanID(); !spanID.IsEmpty() {
			tags[common.AttributeSpanID] = hex.EncodeToString(spanID[:])
		}
	}

	if severityNumber := logRecord.SeverityNumber(); severityNumber != plog.SeverityNumberUnspecified {
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
	logRecord.Attributes().Range(func(k string, v pcommon.Value) bool {
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
		fields[common.AttributeDroppedAttributesCount] = droppedAttributesCount
	}

	if err := batch.EnqueuePoint(measurement, tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
		return fmt.Errorf("failed to write point for int gauge: %w", err)
	}

	return nil
}
