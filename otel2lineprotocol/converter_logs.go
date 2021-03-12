package otel2lineprotocol

import (
	"errors"
	"fmt"
	"io"

	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
	"go.opentelemetry.io/collector/consumer/pdata"
	tracetranslator "go.opentelemetry.io/collector/translator/trace"
	"go.uber.org/zap"
)

func (c *OpenTelemetryToLineProtocolConverter) WriteLogs(ld pdata.Logs, w io.Writer) (droppedLogRecords int) {
	resourceLogss := ld.ResourceLogs()
	for i := 0; i < resourceLogss.Len(); i++ {
		resourceLogs := resourceLogss.At(i)
		resource := resourceLogs.Resource()
		ilLogss := resourceLogs.InstrumentationLibraryLogs()
		for j := 0; j < ilLogss.Len(); j++ {
			ilLogs := ilLogss.At(j)
			instrumentationLibrary := ilLogs.InstrumentationLibrary()
			logs := ilLogs.Logs()
			for k := 0; k < logs.Len(); k++ {
				logRecord := logs.At(k)
				if err := c.writeLogRecord(resource, instrumentationLibrary, logRecord, w); err != nil {
					droppedLogRecords++
					c.logger.Debug("failed to convert log record to InfluxDB point", zap.Error(err))
				}
			}
		}
	}
	return
}

func (c *OpenTelemetryToLineProtocolConverter) writeLogRecord(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, logRecord pdata.LogRecord, w io.Writer) error {
	timestamp := logRecord.Timestamp().AsTime()
	if timestamp.IsZero() {
		// This is a valid condition in OpenTelemetry, but not in InfluxDB.
		// From otel proto field Logrecord.time_unix_name:
		// "Value of 0 indicates unknown or missing timestamp."
		return errors.New("log record contains no time stamp")
	}

	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()
	encoder.StartLine(measurementLogs)

	// TODO handle logRecord.Flags()
	c.resourceToTags(resource, encoder)
	c.instrumentationLibraryToTags(instrumentationLibrary, encoder)

	if name := logRecord.Name(); name != "" {
		encoder.AddTag(attributeName, name)
	}
	if !logRecord.TraceID().IsEmpty() {
		encoder.AddTag(attributeTraceID, logRecord.TraceID().HexString())
		if !logRecord.SpanID().IsEmpty() {
			encoder.AddTag(attributeSpanID, logRecord.SpanID().HexString())
		}
	}

	if severityNumber := logRecord.SeverityNumber(); severityNumber != pdata.SeverityNumberUNDEFINED {
		encoder.AddField(attributeSeverityNumber, lineprotocol.MustNewValue(int64(severityNumber)))
	}
	if severityText := logRecord.SeverityText(); severityText != "" {
		encoder.AddField(attributeSeverityText, lineprotocol.MustNewValue(severityText))
	}
	if body := tracetranslator.AttributeValueToString(logRecord.Body(), false); body != "" {
		encoder.AddField(attributeBody, lineprotocol.MustNewValue(body))
	}

	droppedAttributesCount := logRecord.DroppedAttributesCount()
	logRecord.Attributes().ForEach(func(k string, av pdata.AttributeValue) {
		lpv, err := c.attributeValueToLPV(av)
		if err != nil {
			droppedAttributesCount++
			c.logger.Debug("failed to convert log attribute value to line protocol value", zap.Error(err))
		} else {
			encoder.AddField(k, lpv)
		}
	})
	if droppedAttributesCount > 0 {
		encoder.AddField(attributeDroppedAttributesCount, lineprotocol.MustNewValue(droppedAttributesCount))
	}

	encoder.EndLine(timestamp)
	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		b = append(b, '\n')
		return fmt.Errorf("failed to convert log record to line protocol: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return fmt.Errorf("failed to write log record as line protocol: %w", err)
	}

	return nil
}
