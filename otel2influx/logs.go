package otel2influx

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcollectorlogs "github.com/influxdata/influxdb-observability/otlp/collector/logs/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlplogs "github.com/influxdata/influxdb-observability/otlp/logs/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

type OtelLogsToLineProtocol struct {
	logger common.Logger
}

func NewOtelLogsToLineProtocol(logger common.Logger) *OtelLogsToLineProtocol {
	return &OtelLogsToLineProtocol{
		logger: logger,
	}
}

func (c *OtelLogsToLineProtocol) WriteLogsFromRequestBytes(ctx context.Context, b []byte, w InfluxWriter) error {
	var req otlpcollectorlogs.ExportLogsServiceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	return c.WriteLogs(ctx, req.ResourceLogs, w)
}

func (c *OtelLogsToLineProtocol) WriteLogs(ctx context.Context, resourceLogss []*otlplogs.ResourceLogs, w InfluxWriter) error {
	for _, resourceLogs := range resourceLogss {
		resource := resourceLogs.Resource
		for _, ilLogs := range resourceLogs.InstrumentationLibraryLogs {
			instrumentationLibrary := ilLogs.InstrumentationLibrary
			for _, logRecord := range ilLogs.Logs {
				if err := c.writeLogRecord(ctx, resource, instrumentationLibrary, logRecord, w); err != nil {
					return fmt.Errorf("failed to convert OTLP log record to line protocol: %w", err)
				}
			}
		}
	}
	return nil
}

func (c *OtelLogsToLineProtocol) writeLogRecord(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, logRecord *otlplogs.LogRecord, w InfluxWriter) error {
	ts := time.Unix(0, int64(logRecord.TimeUnixNano))
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
	tags = resourceToTags(c.logger, resource, tags)
	tags = instrumentationLibraryToTags(instrumentationLibrary, tags)

	if name := logRecord.Name; name != "" {
		fields[common.AttributeName] = name
	}
	if traceID := hex.EncodeToString(logRecord.TraceId); len(traceID) > 0 {
		tags[common.AttributeTraceID] = traceID
		if spanID := hex.EncodeToString(logRecord.SpanId); len(spanID) > 0 {
			tags[common.AttributeSpanID] = spanID
		}
	}

	if severityNumber := logRecord.SeverityNumber; severityNumber != otlplogs.SeverityNumber_SEVERITY_NUMBER_UNSPECIFIED {
		fields[common.AttributeSeverityNumber] = int64(severityNumber)
	}
	if severityText := logRecord.SeverityText; severityText != "" {
		fields[common.AttributeSeverityText] = severityText
	}
	if v, err := otlpValueToInfluxFieldValue(logRecord.Body); err != nil {
		c.logger.Debug("invalid log record body", err)
		fields[common.AttributeBody] = nil
	} else {
		fields[common.AttributeBody] = v
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
		fields[common.AttributeDroppedSpanAttributesCount] = droppedAttributesCount
	}

	if err := w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
		return fmt.Errorf("failed to write point for int gauge: %w", err)
	}

	return nil
}
