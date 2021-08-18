package otel2influx

import (
	"context"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"go.opentelemetry.io/collector/model/pdata"
)

type OtelTracesToLineProtocol struct {
	logger common.Logger
}

func NewOtelTracesToLineProtocol(logger common.Logger) *OtelTracesToLineProtocol {
	return &OtelTracesToLineProtocol{
		logger: logger,
	}
}

func (c *OtelTracesToLineProtocol) WriteTraces(ctx context.Context, td pdata.Traces, w InfluxWriter) error {
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpans.InstrumentationLibrarySpans().Len(); j++ {
			ilSpans := resourceSpans.InstrumentationLibrarySpans().At(j)
			for k := 0; k < ilSpans.Spans().Len(); k++ {
				span := ilSpans.Spans().At(k)
				if err := c.writeSpan(ctx, resourceSpans.Resource(), ilSpans.InstrumentationLibrary(), span, w); err != nil {
					return fmt.Errorf("failed to convert OTLP span to line protocol: %w", err)
				}
			}
		}
	}
	return nil
}

func (c *OtelTracesToLineProtocol) writeSpan(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, span pdata.Span, w InfluxWriter) error {
	measurement := common.MeasurementSpans
	tags := make(map[string]string)
	fields := make(map[string]interface{})

	tags = ResourceToTags(c.logger, resource, tags)
	tags = InstrumentationLibraryToTags(instrumentationLibrary, tags)

	traceID := span.TraceID()
	if traceID.IsEmpty() {
		return errors.New("span has no trace ID")
	}
	tags[common.AttributeTraceID] = traceID.HexString()

	spanID := span.SpanID()
	if spanID.IsEmpty() {
		return errors.New("span has no span ID")
	}
	tags[common.AttributeSpanID] = spanID.HexString()

	if span.TraceState() != pdata.TraceStateEmpty {
		tags[common.AttributeTraceState] = string(span.TraceState())
	}
	if parentSpanID := span.ParentSpanID(); !parentSpanID.IsEmpty() {
		tags[common.AttributeParentSpanID] = parentSpanID.HexString()
	}
	if name := span.Name(); name != "" {
		tags[common.AttributeName] = name
	}
	if kind := span.Kind(); kind != pdata.SpanKindUnspecified {
		tags[common.AttributeSpanKind] = kind.String()
	}

	ts := span.StartTimestamp().AsTime()
	if ts.IsZero() {
		return errors.New("span has no timestamp")
	}

	if endTime := span.EndTimestamp().AsTime(); !endTime.IsZero() {
		fields[common.AttributeEndTimeUnixNano] = endTime.UnixNano()
		fields[common.AttributeDurationNano] = endTime.Sub(ts).Nanoseconds()
	}

	droppedAttributesCount := uint64(span.DroppedAttributesCount())
	span.Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		if k == "" {
			droppedAttributesCount++
			c.logger.Debug("span attribute key is empty")
		} else if v, err := AttributeValueToInfluxFieldValue(v); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span attribute value", "key", k, err)
		} else {
			fields[k] = v
		}
		return true
	})
	if droppedAttributesCount > 0 {
		fields[common.AttributeDroppedSpanAttributesCount] = droppedAttributesCount
	}

	droppedEventsCount := uint64(span.DroppedEventsCount())
	for i := 0; i < span.Events().Len(); i++ {
		event := span.Events().At(i)
		if measurement, tags, fields, ts, err := c.spanEventToLP(traceID, spanID, resource, instrumentationLibrary, event); err != nil {
			droppedEventsCount++
			c.logger.Debug("invalid span event", err)
		} else if err = w.WritePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
			return fmt.Errorf("failed to write point for span event: %w", err)
		}
	}
	if droppedEventsCount > 0 {
		fields[common.AttributeDroppedEventsCount] = droppedEventsCount
	}

	droppedLinksCount := uint64(span.DroppedLinksCount())
	for i := 0; i < span.Links().Len(); i++ {
		link := span.Links().At(i)
		if measurement, tags, fields, err := c.spanLinkToLP(traceID, spanID, link); err != nil {
			droppedLinksCount++
			c.logger.Debug("invalid span link", err)
		} else if err = w.WritePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
			return fmt.Errorf("failed to write point for span link: %w", err)
		}
	}
	if droppedLinksCount > 0 {
		fields[common.AttributeDroppedLinksCount] = droppedLinksCount
	}

	status := span.Status()
	switch status.Code() {
	case pdata.StatusCodeUnset:
	case pdata.StatusCodeOk:
		fields[common.AttributeStatusCode] = common.AttributeStatusCodeOK
	case pdata.StatusCodeError:
		fields[common.AttributeStatusCode] = common.AttributeStatusCodeError
	default:
		c.logger.Debug("status code not recognized", "code", status.Code())
	}
	if message := status.Message(); message != "" {
		fields[common.AttributeStatusMessage] = message
	}

	if err := w.WritePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
		return fmt.Errorf("failed to write point for span: %w", err)
	}

	return nil
}

func (c *OtelTracesToLineProtocol) spanEventToLP(traceID pdata.TraceID, spanID pdata.SpanID, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, spanEvent pdata.SpanEvent) (measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, err error) {
	measurement = common.MeasurementLogs
	tags = make(map[string]string)
	fields = make(map[string]interface{})

	tags = ResourceToTags(c.logger, resource, tags)
	tags = InstrumentationLibraryToTags(instrumentationLibrary, tags)

	tags[common.AttributeTraceID] = traceID.HexString()
	tags[common.AttributeSpanID] = spanID.HexString()
	if name := spanEvent.Name(); name != "" {
		tags[common.AttributeName] = name
	}

	droppedAttributesCount := uint64(spanEvent.DroppedAttributesCount())
	spanEvent.Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		if k == "" {
			droppedAttributesCount++
			c.logger.Debug("span event attribute key is empty")
		} else if v, err := AttributeValueToInfluxFieldValue(v); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span event attribute value", err)
		} else {
			fields[k] = v
		}
		return true
	})
	if droppedAttributesCount > 0 {
		fields[common.AttributeDroppedEventAttributesCount] = droppedAttributesCount
	}

	if len(fields) == 0 {
		// TODO remove when tags and fields are just columns
		fields["count"] = uint64(1)
	}

	ts = spanEvent.Timestamp().AsTime()
	if ts.IsZero() {
		err = errors.New("span event has no timestamp")
		return
	}

	return
}

func (c *OtelTracesToLineProtocol) spanLinkToLP(traceID pdata.TraceID, spanID pdata.SpanID, spanLink pdata.SpanLink) (measurement string, tags map[string]string, fields map[string]interface{}, err error) {
	measurement = common.MeasurementSpanLinks
	tags = make(map[string]string)
	fields = make(map[string]interface{})

	tags[common.AttributeTraceID] = traceID.HexString()
	tags[common.AttributeSpanID] = spanID.HexString()

	if linkedTraceID := spanLink.TraceID(); linkedTraceID.IsEmpty() {
		err = errors.New("span link has no trace ID")
		return
	} else {
		tags[common.AttributeLinkedTraceID] = linkedTraceID.HexString()
	}

	if linkedSpanID := spanLink.SpanID(); linkedSpanID.IsEmpty() {
		err = errors.New("span link has no span ID")
		return
	} else {
		tags[common.AttributeLinkedSpanID] = linkedSpanID.HexString()
	}

	if traceState := spanLink.TraceState(); traceState != pdata.TraceStateEmpty {
		tags[common.AttributeTraceState] = string(traceState)
	}

	droppedAttributesCount := uint64(spanLink.DroppedAttributesCount())
	spanLink.Attributes().Range(func(k string, v pdata.AttributeValue) bool {
		if k == "" {
			droppedAttributesCount++
			c.logger.Debug("span link attribute key is empty")
		} else if v, err := AttributeValueToInfluxFieldValue(v); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span link attribute value", err)
		} else {
			fields[k] = v
		}
		return true
	})
	if droppedAttributesCount > 0 {
		fields[common.AttributeDroppedLinkAttributesCount] = droppedAttributesCount
	}

	if len(fields) == 0 {
		// TODO remove when tags and fields are just columns
		fields["count"] = uint64(1)
	}

	return
}
