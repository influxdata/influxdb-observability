package otel2influx

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"
	"golang.org/x/exp/maps"

	"github.com/influxdata/influxdb-observability/common"
)

type OtelLogsToLineProtocolConfig struct {
	Logger common.Logger
	Writer InfluxWriter
	// LogRecordDimensions are log record attributes to be used as line protocol tags.
	// These are always included as tags, if available:
	// - trace ID
	// - span ID
	// The default values:
	// - service.name
	// Other common attributes can be found here:
	// - https://github.com/open-telemetry/opentelemetry-collector/tree/main/semconv
	// When using InfluxDB for both logs and traces, be certain that LogRecordDimensions
	// matches the tracing SpanDimensions value.
	LogRecordDimensions []string
}

func DefaultOtelLogsToLineProtocolConfig() *OtelLogsToLineProtocolConfig {
	return &OtelLogsToLineProtocolConfig{
		Logger: new(common.NoopLogger),
		Writer: new(NoopInfluxWriter),
		LogRecordDimensions: []string{
			semconv.AttributeServiceName,
		},
	}
}

type OtelLogsToLineProtocol struct {
	logger common.Logger
	writer InfluxWriter

	logRecordDimensions map[string]struct{}
}

func NewOtelLogsToLineProtocol(config *OtelLogsToLineProtocolConfig) (*OtelLogsToLineProtocol, error) {
	logRecordDimensions := make(map[string]struct{}, len(config.LogRecordDimensions))
	{
		duplicateDimensions := make(map[string]struct{})
		for _, k := range config.LogRecordDimensions {
			if _, found := logRecordDimensions[k]; found {
				duplicateDimensions[k] = struct{}{}
			} else {
				logRecordDimensions[k] = struct{}{}
			}
		}
		if len(duplicateDimensions) > 0 {
			return nil, fmt.Errorf("duplicate record dimension(s) configured: %s",
				strings.Join(maps.Keys(duplicateDimensions), ","))
		}

	}
	return &OtelLogsToLineProtocol{
		logger:              config.Logger,
		writer:              config.Writer,
		logRecordDimensions: logRecordDimensions,
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
				if err := c.enqueueLogRecord(ctx, resourceLogs.Resource(), ilLogs.Scope(), logRecord, batch); err != nil {
					return consumererror.NewPermanent(fmt.Errorf("failed to convert OTLP log record to line protocol: %w", err))
				}
			}
		}
	}
	return batch.WriteBatch(ctx)
}

func (c *OtelLogsToLineProtocol) enqueueLogRecord(ctx context.Context, resource pcommon.Resource, instrumentationScope pcommon.InstrumentationScope, logRecord plog.LogRecord, batch InfluxWriterBatch) error {
	ts := logRecord.Timestamp().AsTime()
	if ts.IsZero() {
		// This is a valid condition in OpenTelemetry, but not in InfluxDB.
		// From otel proto field Logrecord.time_unix_name:
		// "Value of 0 indicates unknown or missing timestamp."
		ts = time.Now()
	}

	tags := make(map[string]string, len(c.logRecordDimensions)+2)
	fields := make(map[string]interface{})

	if ots := logRecord.ObservedTimestamp().AsTime(); !ots.IsZero() && !ots.Equal(time.Unix(0, 0)) {
		fields[common.AttributeObservedTimeUnixNano] = ots.UnixNano()
	}

	if traceID, spanID := logRecord.TraceID(), logRecord.SpanID(); !traceID.IsEmpty() && !spanID.IsEmpty() {
		tags[common.AttributeTraceID] = traceID.String()
		tags[common.AttributeSpanID] = spanID.String()
	}

	if severityNumber := logRecord.SeverityNumber(); severityNumber != plog.SeverityNumberUnspecified {
		fields[common.AttributeSeverityNumber] = int64(severityNumber)
	}
	if severityText := logRecord.SeverityText(); severityText != "" {
		fields[common.AttributeSeverityText] = severityText
	}
	fields[common.AttributeBody] = logRecord.Body().AsString()

	droppedAttributesCount := uint64(logRecord.DroppedAttributesCount())
	attributesField := make(map[string]any)
	for _, attributes := range []pcommon.Map{resource.Attributes(), instrumentationScope.Attributes(), logRecord.Attributes()} {
		attributes.Range(func(k string, v pcommon.Value) bool {
			if k == "" {
				return true
			}
			if _, found := c.logRecordDimensions[k]; found {
				tags[k] = v.AsString()
			} else {
				attributesField[k] = v.AsRaw()
			}
			return true
		})
	}
	if len(attributesField) > 0 {
		marshalledAttributes, err := json.Marshal(attributesField)
		if err != nil {
			c.logger.Debug("failed to marshal attributes to JSON", err)
			droppedAttributesCount += uint64(logRecord.Attributes().Len())
		} else {
			fields[common.AttributeAttributes] = string(marshalledAttributes)
		}
	}
	for k := range tags {
		if _, found := fields[k]; found {
			c.logger.Debug("tag and field keys conflict; field will be dropped", "key", k)
			droppedAttributesCount++
			delete(fields, k)
		}
	}
	if droppedAttributesCount > 0 {
		fields[common.AttributeDroppedAttributesCount] = droppedAttributesCount
	}

	if err := batch.EnqueuePoint(ctx, common.MeasurementLogs, tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
		return fmt.Errorf("failed to write point for int gauge: %w", err)
	}

	return nil
}
