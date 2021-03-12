package otel2lineprotocol

import (
	"errors"
	"fmt"
	"io"
	"time"

	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

func (c *OpenTelemetryToLineProtocolConverter) WriteTraces(td pdata.Traces, w io.Writer) (droppedSpans int) {
	resourceSpanss := td.ResourceSpans()
	for i := 0; i < resourceSpanss.Len(); i++ {
		resourceSpans := resourceSpanss.At(i)
		resource := resourceSpans.Resource()
		ilSpanss := resourceSpans.InstrumentationLibrarySpans()
		for j := 0; j < ilSpanss.Len(); j++ {
			ilSpans := ilSpanss.At(j)
			instrumentationLibrary := ilSpans.InstrumentationLibrary()
			spans := ilSpans.Spans()
			for k := 0; k < spans.Len(); k++ {
				span := spans.At(k)
				if err := c.writeSpan(resource, instrumentationLibrary, span, w); err != nil {
					droppedSpans++
					c.logger.Debug("failed to convert span to line protocol", zap.Error(err))
				}
			}
		}
	}
	return
}

func (c *OpenTelemetryToLineProtocolConverter) writeSpan(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, span pdata.Span, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()
	encoder.StartLine(measurementSpans)

	c.resourceToTags(resource, encoder)
	c.instrumentationLibraryToTags(instrumentationLibrary, encoder)

	if span.TraceID().IsEmpty() {
		return errors.New("span contains no trace ID")
	}
	traceID := span.TraceID().HexString()
	encoder.AddTag(attributeTraceID, traceID)

	if span.SpanID().IsEmpty() {
		return errors.New("span contains no span ID")
	}
	spanID := span.SpanID().HexString()
	encoder.AddTag(attributeSpanID, spanID)

	if span.TraceState() != "" {
		encoder.AddTag(attributeTraceState, string(span.TraceState()))
	}
	if !span.ParentSpanID().IsEmpty() {
		encoder.AddTag(attributeParentSpanID, span.ParentSpanID().HexString())
	}
	if span.Name() != "" {
		encoder.AddTag(attributeName, span.Name())
	}
	if pdata.SpanKindUNSPECIFIED != span.Kind() {
		encoder.AddTag(attributeSpanKind, span.Kind().String())
	}

	timestamp := span.StartTime().AsTime()
	if timestamp.IsZero() {
		return errors.New("span contains no timestamp")
	}

	if endTime := span.EndTime().AsTime(); !endTime.IsZero() {
		encoder.AddField(attributeEndTimeUnixNano, lineprotocol.MustNewValue(endTime.UnixNano()))
		encoder.AddField(attributeDurationNano, lineprotocol.MustNewValue(endTime.Sub(timestamp).Nanoseconds()))
	}

	droppedAttributesCount := span.DroppedAttributesCount()
	span.Attributes().ForEach(func(k string, av pdata.AttributeValue) {
		lpv, err := c.attributeValueToLPV(av)
		if err != nil {
			droppedAttributesCount++
			c.logger.Debug("failed to convert span attribute value to line protocol value", zap.Error(err))
		} else {
			encoder.AddField(k, lpv)
		}
	})
	if droppedAttributesCount > 0 {
		encoder.AddField(attributeDroppedAttributesCount, lineprotocol.MustNewValue(droppedAttributesCount))
	}

	droppedEventsCount := span.DroppedEventsCount()
	for events, i := span.Events(), 0; i < events.Len(); i++ {
		lp, err := c.spanEventToLP(traceID, spanID, events.At(i))
		if err != nil {
			droppedEventsCount++
			c.logger.Debug("failed to convert span event to InfluxDB write point", zap.Error(err))
		} else if _, err = w.Write(lp); err != nil {
			return fmt.Errorf("failed to write span event as line protocol: %w", err)
		} else if _, err = w.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
	}
	if droppedEventsCount > 0 {
		encoder.AddField(attributeDroppedEventsCount, lineprotocol.MustNewValue(droppedEventsCount))
	}

	droppedLinksCount := span.DroppedLinksCount()
	for links, i := span.Links(), 0; i < links.Len(); i++ {
		lp, err := c.spanLinkToLP(traceID, spanID, timestamp, links.At(i))
		if err != nil {
			droppedLinksCount++
			c.logger.Debug("failed to convert span link to JSON string", zap.Error(err))
		} else if _, err = w.Write(lp); err != nil {
			return fmt.Errorf("failed to write span link as line protocol: %w", err)
		} else if _, err = w.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
	}
	if droppedLinksCount > 0 {
		encoder.AddField(attributeDroppedLinksCount, lineprotocol.MustNewValue(droppedLinksCount))
	}

	if span.Status().Code() != pdata.StatusCodeUnset {
		encoder.AddField(attributeStatusCode, lineprotocol.MustNewValue(span.Status().Code().String()))
	}
	if span.Status().Message() != "" {
		encoder.AddField(attributeStatusMessage, lineprotocol.MustNewValue(span.Status().Message()))
	}

	encoder.EndLine(timestamp)
	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return fmt.Errorf("failed to convert span to line protocol: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return fmt.Errorf("failed to write span as line protocol: %w", err)
	} else if _, err = w.Write([]byte("\n")); err != nil {
		return fmt.Errorf("failed to write newline: %w", err)
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) spanEventToLP(traceID, spanID string, spanEvent pdata.SpanEvent) ([]byte, error) {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()
	encoder.StartLine(measurementLogs)

	if spanEvent.Name() != "" {
		encoder.AddTag(attributeName, spanEvent.Name())
	}
	encoder.AddTag(attributeTraceID, traceID)
	encoder.AddTag(attributeSpanID, spanID)

	droppedAttributesCount := spanEvent.DroppedAttributesCount()
	spanEvent.Attributes().ForEach(func(k string, av pdata.AttributeValue) {
		lpv, err := c.attributeValueToLPV(av)
		if err != nil {
			droppedAttributesCount++
			c.logger.Debug("failed to convert span attribute value to interface{}", zap.Error(err))
		} else {
			encoder.AddField(k, lpv)
		}
	})
	if droppedAttributesCount > 0 {
		encoder.AddField(attributeDroppedAttributesCount, lineprotocol.MustNewValue(droppedAttributesCount))
	}

	if timestamp := spanEvent.Timestamp().AsTime(); timestamp.IsZero() {
		return nil, errors.New("span event contains no timestamp")
	} else {
		encoder.EndLine(timestamp)
	}
	return encoder.Bytes(), encoder.Err()
}

func (c *OpenTelemetryToLineProtocolConverter) spanLinkToLP(traceID, spanID string, timestamp time.Time, spanLink pdata.SpanLink) ([]byte, error) {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()
	encoder.StartLine(measurementSpanLinks)

	if spanLink.TraceID().IsEmpty() {
		return nil, errors.New("span link contains no trace ID")
	}
	linkedTraceID := spanLink.TraceID().HexString()

	if spanLink.SpanID().IsEmpty() {
		return nil, errors.New("span link contains no span ID")
	}
	linkedSpanID := spanLink.SpanID().HexString()

	if traceState := string(spanLink.TraceState()); traceState != "" {
		encoder.AddTag(attributeTraceState, traceState)
	}
	encoder.AddTag(attributeTraceID, traceID)
	encoder.AddTag(attributeSpanID, spanID)
	encoder.AddTag(attributeLinkedTraceID, linkedTraceID)
	encoder.AddTag(attributeLinkedSpanID, linkedSpanID)

	droppedAttributesCount := spanLink.DroppedAttributesCount()
	spanLink.Attributes().ForEach(func(k string, av pdata.AttributeValue) {
		lpv, err := c.attributeValueToLPV(av)
		if err != nil {
			droppedAttributesCount++
			c.logger.Debug("failed to convert span attribute value to interface{}", zap.Error(err))
		} else {
			encoder.AddField(k, lpv)
		}
	})
	if droppedAttributesCount > 0 {
		encoder.AddField(attributeDroppedAttributesCount, lineprotocol.MustNewValue(droppedAttributesCount))
	}
	encoder.EndLine(timestamp)

	return encoder.Bytes(), encoder.Err()
}
