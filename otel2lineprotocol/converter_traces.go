package otel2lineprotocol

import (
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"time"

	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlpresource "go.opentelemetry.io/proto/otlp/resource/v1"
	otlptrace "go.opentelemetry.io/proto/otlp/trace/v1"
)

func (c *OpenTelemetryToLineProtocolConverter) WriteTraces(resourceSpanss []*otlptrace.ResourceSpans, w io.Writer) (droppedSpans int) {
	for _, resourceSpans := range resourceSpanss {
		resource := resourceSpans.Resource
		for _, ilSpans := range resourceSpans.InstrumentationLibrarySpans {
			instrumentationLibrary := ilSpans.InstrumentationLibrary
			for _, span := range ilSpans.Spans {
				if err := c.writeSpan(resource, instrumentationLibrary, span, w); err != nil {
					droppedSpans++
					c.logger.Debug("failed to convert span", err)
				}
			}
		}
	}
	return
}

func (c *OpenTelemetryToLineProtocolConverter) writeSpan(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, span *otlptrace.Span, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()
	encoder.StartLine(measurementSpans)

	c.resourceToTags(resource, encoder)
	instrumentationLibraryToTags(instrumentationLibrary, encoder)

	traceID := hex.EncodeToString(span.TraceId)
	if len(traceID) == 0 {
		return errors.New("span has no trace ID")
	}
	encoder.AddTag(attributeTraceID, traceID)

	spanID := hex.EncodeToString(span.SpanId)
	if len(spanID) == 0 {
		return errors.New("span has no span ID")
	}
	encoder.AddTag(attributeSpanID, spanID)

	if span.TraceState != "" {
		encoder.AddTag(attributeTraceState, span.TraceState)
	}
	if len(span.ParentSpanId) > 0 {
		encoder.AddTag(attributeParentSpanID, hex.EncodeToString(span.ParentSpanId))
	}
	if span.Name != "" {
		encoder.AddTag(attributeName, span.Name)
	}
	if otlptrace.Span_SPAN_KIND_UNSPECIFIED != span.Kind {
		encoder.AddTag(attributeSpanKind, span.Kind.String())
	}

	timestamp := time.Unix(0, int64(span.StartTimeUnixNano))
	if timestamp.IsZero() {
		return errors.New("span has no timestamp")
	}

	if endTime := time.Unix(0, int64(span.EndTimeUnixNano)); !endTime.IsZero() {
		encoder.AddField(attributeEndTimeUnixNano, lineprotocol.IntValue(endTime.UnixNano()))
		encoder.AddField(attributeDurationNano, lineprotocol.IntValue(endTime.Sub(timestamp).Nanoseconds()))
	}

	droppedAttributesCount := uint64(span.DroppedAttributesCount)
	for _, attribute := range span.Attributes {
		if k := attribute.Key; k == "" {
			droppedAttributesCount++
			c.logger.Debug("span attribute key is empty")
		} else if v, err := otlpValueToLPV(attribute.Value); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span attribute value", "key", k, err)
		} else {
			encoder.AddField(k, v)
		}
	}
	if droppedAttributesCount > 0 {
		encoder.AddField(attributeDroppedAttributesCount, lineprotocol.UintValue(droppedAttributesCount))
	}

	droppedEventsCount := uint64(span.DroppedEventsCount)
	for _, event := range span.Events {
		if lp, err := c.spanEventToLP(traceID, spanID, event); err != nil {
			droppedEventsCount++
			c.logger.Debug("invalid span event", err)
		} else if _, err = w.Write(lp); err != nil {
			return err
		}
	}
	if droppedEventsCount > 0 {
		encoder.AddField(attributeDroppedEventsCount, lineprotocol.UintValue(droppedEventsCount))
	}

	droppedLinksCount := uint64(span.DroppedLinksCount)
	for _, link := range span.Links {
		if lp, err := c.spanLinkToLP(traceID, spanID, timestamp, link); err != nil {
			droppedLinksCount++
			c.logger.Debug("invalid span link", err)
		} else if _, err = w.Write(lp); err != nil {
			return err
		}
	}
	if droppedLinksCount > 0 {
		encoder.AddField(attributeDroppedLinksCount, lineprotocol.UintValue(droppedLinksCount))
	}

	if status := span.Status; status != nil {
		if code := status.Code; code != otlptrace.Status_STATUS_CODE_UNSET {
			if v, ok := lineprotocol.StringValue(code.String()); !ok {
				c.logger.Debug("invalid span status code", "code", code.String())
			} else {
				encoder.AddField(attributeStatusCode, v)
			}
		}

		if message := status.Message; message != "" {
			if v, ok := lineprotocol.StringValue(message); !ok {
				c.logger.Debug("invalid span status message", "message", message)
			} else {
				encoder.AddField(attributeStatusMessage, v)
			}
		}
	}

	encoder.EndLine(timestamp)
	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return fmt.Errorf("failed to encode span: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return err
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) spanEventToLP(traceID, spanID string, spanEvent *otlptrace.Span_Event) ([]byte, error) {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()
	encoder.StartLine(measurementLogs)

	encoder.AddTag(attributeTraceID, traceID)
	encoder.AddTag(attributeSpanID, spanID)
	// TODO add resources and instrumentation library?

	if name := spanEvent.Name; name != "" {
		encoder.AddTag(attributeName, name)
	}

	droppedAttributesCount := uint64(spanEvent.DroppedAttributesCount)
	for _, attribute := range spanEvent.Attributes {
		if k := attribute.Key; k == "" {
			droppedAttributesCount++
			c.logger.Debug("span event attribute key is empty")
		} else if v, err := otlpValueToLPV(attribute.Value); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span event attribute value", err)
		} else {
			encoder.AddField(k, v)
		}
	}
	if droppedAttributesCount > 0 {
		encoder.AddField(attributeDroppedAttributesCount, lineprotocol.UintValue(droppedAttributesCount))
	}

	if timestamp := time.Unix(0, int64(spanEvent.TimeUnixNano)); timestamp.IsZero() {
		return nil, errors.New("span event has no timestamp")
	} else {
		encoder.EndLine(timestamp)
	}

	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return nil, fmt.Errorf("failed to encode span event %w", err)
	} else {
		return b, nil
	}
}

func (c *OpenTelemetryToLineProtocolConverter) spanLinkToLP(traceID, spanID string, timestamp time.Time, spanLink *otlptrace.Span_Link) ([]byte, error) {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()
	encoder.StartLine(measurementSpanLinks)

	encoder.AddTag(attributeTraceID, traceID)
	encoder.AddTag(attributeSpanID, spanID)

	if linkedTraceID := hex.EncodeToString(spanLink.TraceId); len(linkedTraceID) == 0 {
		return nil, errors.New("span link has no trace ID")
	} else {
		encoder.AddTag(attributeLinkedTraceID, linkedTraceID)
	}

	if linkedSpanID := hex.EncodeToString(spanLink.SpanId); len(linkedSpanID) == 0 {
		return nil, errors.New("span link has no span ID")
	} else {
		encoder.AddTag(attributeLinkedSpanID, linkedSpanID)
	}

	if traceState := spanLink.TraceState; traceState != "" {
		encoder.AddTag(attributeTraceState, traceState)
	}

	droppedAttributesCount := uint64(spanLink.DroppedAttributesCount)
	for _, attribute := range spanLink.Attributes {
		if k := attribute.Key; k == "" {
			droppedAttributesCount++
			c.logger.Debug("span link attribute key is empty")
		} else if v, err := otlpValueToLPV(attribute.Value); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span link attribute value", err)
		} else {
			encoder.AddField(k, v)
		}
	}
	if droppedAttributesCount > 0 {
		encoder.AddField(attributeDroppedAttributesCount, lineprotocol.UintValue(droppedAttributesCount))
	}
	encoder.EndLine(timestamp)

	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return nil, fmt.Errorf("failed to encode span link %w", err)
	} else {
		return b, nil
	}
}
