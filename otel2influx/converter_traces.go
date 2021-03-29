package otel2influx

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	otlpcommon "github.com/open-telemetry/opentelemetry-proto/gen/go/common/v1"
	otlpresource "github.com/open-telemetry/opentelemetry-proto/gen/go/resource/v1"
	otlptrace "github.com/open-telemetry/opentelemetry-proto/gen/go/trace/v1"
)

// TODO return which spans were dropped
func (c *OpenTelemetryToInfluxConverter) WriteTraces(ctx context.Context, resourceSpanss []*otlptrace.ResourceSpans, w InfluxWriter) (droppedSpans int) {
	for _, resourceSpans := range resourceSpanss {
		resource := resourceSpans.Resource
		for _, ilSpans := range resourceSpans.InstrumentationLibrarySpans {
			instrumentationLibrary := ilSpans.InstrumentationLibrary
			for _, span := range ilSpans.Spans {
				if err := c.writeSpan(ctx, resource, instrumentationLibrary, span, w); err != nil {
					droppedSpans++
					c.logger.Debug("failed to convert span", err)
				}
			}
		}
	}
	return
}

func (c *OpenTelemetryToInfluxConverter) writeSpan(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, span *otlptrace.Span, w InfluxWriter) error {
	measurement := measurementSpans
	tags := make(map[string]string)
	fields := make(map[string]interface{})

	var droppedResourceAttributesCount uint64
	tags, droppedResourceAttributesCount = c.resourceToTags(resource, tags)
	if droppedResourceAttributesCount > 0 {
		fields[attributeDroppedResourceAttributesCount] = droppedResourceAttributesCount
	}
	tags = c.instrumentationLibraryToTags(instrumentationLibrary, tags)

	traceID := hex.EncodeToString(span.TraceId)
	if len(traceID) == 0 {
		return errors.New("span has no trace ID")
	}
	tags[attributeTraceID] = traceID

	spanID := hex.EncodeToString(span.SpanId)
	if len(spanID) == 0 {
		return errors.New("span has no span ID")
	}
	tags[attributeSpanID] = spanID

	if span.TraceState != "" {
		tags[attributeTraceState] = span.TraceState
	}
	if len(span.ParentSpanId) > 0 {
		tags[attributeParentSpanID] = hex.EncodeToString(span.ParentSpanId)
	}
	if span.Name != "" {
		tags[attributeName] = span.Name
	}
	if otlptrace.Span_SPAN_KIND_UNSPECIFIED != span.Kind {
		tags[attributeSpanKind] = span.Kind.String()
	}

	ts := time.Unix(0, int64(span.StartTimeUnixNano))
	if ts.IsZero() {
		return errors.New("span has no timestamp")
	}

	if endTime := time.Unix(0, int64(span.EndTimeUnixNano)); !endTime.IsZero() {
		fields[attributeEndTimeUnixNano] = endTime.UnixNano()
		fields[attributeDurationNano] = endTime.Sub(ts).Nanoseconds()
	}

	droppedAttributesCount := uint64(span.DroppedAttributesCount)
	for _, attribute := range span.Attributes {
		if k := attribute.Key; k == "" {
			droppedAttributesCount++
			c.logger.Debug("span attribute key is empty")
		} else if v, err := otlpValueToInfluxFieldValue(attribute.Value); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span attribute value", "key", k, err)
		} else {
			fields[k] = v
		}
	}
	if droppedAttributesCount > 0 {
		fields[attributeDroppedSpanAttributesCount] = droppedAttributesCount
	}

	droppedEventsCount := uint64(span.DroppedEventsCount)
	for _, event := range span.Events {
		if measurement, tags, fields, ts, err := c.spanEventToLP(traceID, spanID, resource, instrumentationLibrary, event); err != nil {
			droppedEventsCount++
			c.logger.Debug("invalid span event", err)
		} else if err = w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
			return fmt.Errorf("failed to write point for span event: %w", err)
		}
	}
	if droppedEventsCount > 0 {
		fields[attributeDroppedEventsCount] = droppedEventsCount
	}

	droppedLinksCount := uint64(span.DroppedLinksCount)
	for _, link := range span.Links {
		if measurement, tags, fields, err := c.spanLinkToLP(traceID, spanID, link); err != nil {
			droppedLinksCount++
			c.logger.Debug("invalid span link", err)
		} else if err = w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
			return fmt.Errorf("failed to write point for span link: %w", err)
		}
	}
	if droppedLinksCount > 0 {
		fields[attributeDroppedLinksCount] = droppedLinksCount
	}

	if status := span.Status; status != nil {
		switch status.Code {
		case otlptrace.Status_STATUS_CODE_UNSET:
		case otlptrace.Status_STATUS_CODE_OK:
			fields[attributeStatusCode] = attributeStatusCodeOK
		case otlptrace.Status_STATUS_CODE_ERROR:
			fields[attributeStatusCode] = attributeStatusCodeError
		default:
			c.logger.Debug("status code not recognized: %q", status.Code)
		}

		if message := status.Message; message != "" {
			fields[attributeStatusMessage] = message
		}
	}

	if err := w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
		return fmt.Errorf("failed to write point for span: %w", err)
	}

	return nil
}

func (c *OpenTelemetryToInfluxConverter) spanEventToLP(traceID, spanID string, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, spanEvent *otlptrace.Span_Event) (measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, err error) {
	measurement = measurementLogs
	tags = make(map[string]string)
	fields = make(map[string]interface{})

	var droppedResourceAttributesCount uint64
	tags, droppedResourceAttributesCount = c.resourceToTags(resource, tags)
	if droppedResourceAttributesCount > 0 {
		fields[attributeDroppedResourceAttributesCount] = droppedResourceAttributesCount
	}
	tags = c.instrumentationLibraryToTags(instrumentationLibrary, tags)

	tags[attributeTraceID] = traceID
	tags[attributeSpanID] = spanID
	if name := spanEvent.Name; name != "" {
		tags[attributeName] = name
	}

	droppedAttributesCount := uint64(spanEvent.DroppedAttributesCount)
	for _, attribute := range spanEvent.Attributes {
		if k := attribute.Key; k == "" {
			droppedAttributesCount++
			c.logger.Debug("span event attribute key is empty")
		} else if v, err := otlpValueToInfluxFieldValue(attribute.Value); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span event attribute value", err)
		} else {
			fields[k] = v
		}
	}
	if droppedAttributesCount > 0 {
		fields[attributeDroppedEventAttributesCount] = droppedAttributesCount
	}

	if len(fields) == 0 {
		// TODO remove when tags and fields are just columns
		fields["count"] = uint64(1)
	}

	ts = time.Unix(0, int64(spanEvent.TimeUnixNano))
	if ts.IsZero() {
		err = errors.New("span event has no timestamp")
		return
	}

	return
}

func (c *OpenTelemetryToInfluxConverter) spanLinkToLP(traceID, spanID string, spanLink *otlptrace.Span_Link) (measurement string, tags map[string]string, fields map[string]interface{}, err error) {
	measurement = measurementSpanLinks
	tags = make(map[string]string)
	fields = make(map[string]interface{})

	tags[attributeTraceID] = traceID
	tags[attributeSpanID] = spanID

	if linkedTraceID := hex.EncodeToString(spanLink.TraceId); len(linkedTraceID) == 0 {
		err = errors.New("span link has no trace ID")
		return
	} else {
		tags[attributeLinkedTraceID] = linkedTraceID
	}

	if linkedSpanID := hex.EncodeToString(spanLink.SpanId); len(linkedSpanID) == 0 {
		err = errors.New("span link has no span ID")
		return
	} else {
		tags[attributeLinkedSpanID] = linkedSpanID
	}

	if traceState := spanLink.TraceState; traceState != "" {
		tags[attributeTraceState] = traceState
	}

	droppedAttributesCount := uint64(spanLink.DroppedAttributesCount)
	for _, attribute := range spanLink.Attributes {
		if k := attribute.Key; k == "" {
			droppedAttributesCount++
			c.logger.Debug("span link attribute key is empty")
		} else if v, err := otlpValueToInfluxFieldValue(attribute.Value); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span link attribute value", err)
		} else {
			fields[k] = v
		}
	}
	if droppedAttributesCount > 0 {
		fields[attributeDroppedLinkAttributesCount] = droppedAttributesCount
	}

	if len(fields) == 0 {
		// TODO remove when tags and fields are just columns
		fields["count"] = uint64(1)
	}

	return
}
