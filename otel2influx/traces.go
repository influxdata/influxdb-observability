package otel2influx

import (
	"context"
	"encoding/hex"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcollectortrace "github.com/influxdata/influxdb-observability/otlp/collector/trace/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	otlptrace "github.com/influxdata/influxdb-observability/otlp/trace/v1"
	"google.golang.org/protobuf/proto"
)

type OtelTracesToLineProtocol struct {
	logger common.Logger
}

func NewOtelTracesToLineProtocol(logger common.Logger) *OtelTracesToLineProtocol {
	return &OtelTracesToLineProtocol{
		logger: logger,
	}
}

func (c *OtelTracesToLineProtocol) WriteTracesFromRequestBytes(ctx context.Context, b []byte, w InfluxWriter) error {
	var req otlpcollectortrace.ExportTraceServiceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	return c.WriteTraces(ctx, req.ResourceSpans, w)
}

func (c *OtelTracesToLineProtocol) WriteTraces(ctx context.Context, resourceSpanss []*otlptrace.ResourceSpans, w InfluxWriter) error {
	for _, resourceSpans := range resourceSpanss {
		resource := resourceSpans.Resource
		for _, ilSpans := range resourceSpans.InstrumentationLibrarySpans {
			instrumentationLibrary := ilSpans.InstrumentationLibrary
			for _, span := range ilSpans.Spans {
				if err := c.writeSpan(ctx, resource, instrumentationLibrary, span, w); err != nil {
					return fmt.Errorf("failed to convert OTLP span to line protocol: %w", err)
				}
			}
		}
	}
	return nil
}

func (c *OtelTracesToLineProtocol) writeSpan(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, span *otlptrace.Span, w InfluxWriter) error {
	measurement := common.MeasurementSpans
	tags := make(map[string]string)
	fields := make(map[string]interface{})

	tags = resourceToTags(c.logger, resource, tags)
	tags = instrumentationLibraryToTags(instrumentationLibrary, tags)

	traceID := hex.EncodeToString(span.TraceId)
	if len(traceID) == 0 {
		return errors.New("span has no trace ID")
	}
	tags[common.AttributeTraceID] = traceID

	spanID := hex.EncodeToString(span.SpanId)
	if len(spanID) == 0 {
		return errors.New("span has no span ID")
	}
	tags[common.AttributeSpanID] = spanID

	if span.TraceState != "" {
		tags[common.AttributeTraceState] = span.TraceState
	}
	if len(span.ParentSpanId) > 0 {
		tags[common.AttributeParentSpanID] = hex.EncodeToString(span.ParentSpanId)
	}
	if span.Name != "" {
		tags[common.AttributeName] = span.Name
	}
	if otlptrace.Span_SPAN_KIND_UNSPECIFIED != span.Kind {
		tags[common.AttributeSpanKind] = span.Kind.String()
	}

	ts := time.Unix(0, int64(span.StartTimeUnixNano))
	if ts.IsZero() {
		return errors.New("span has no timestamp")
	}

	if endTime := time.Unix(0, int64(span.EndTimeUnixNano)); !endTime.IsZero() {
		fields[common.AttributeEndTimeUnixNano] = endTime.UnixNano()
		fields[common.AttributeDurationNano] = endTime.Sub(ts).Nanoseconds()
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
		fields[common.AttributeDroppedSpanAttributesCount] = droppedAttributesCount
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
		fields[common.AttributeDroppedEventsCount] = droppedEventsCount
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
		fields[common.AttributeDroppedLinksCount] = droppedLinksCount
	}

	if status := span.Status; status != nil {
		switch status.Code {
		case otlptrace.Status_STATUS_CODE_UNSET:
		case otlptrace.Status_STATUS_CODE_OK:
			fields[common.AttributeStatusCode] = common.AttributeStatusCodeOK
		case otlptrace.Status_STATUS_CODE_ERROR:
			fields[common.AttributeStatusCode] = common.AttributeStatusCodeError
		default:
			c.logger.Debug("status code not recognized: %q", status.Code)
		}

		if message := status.Message; message != "" {
			fields[common.AttributeStatusMessage] = message
		}
	}

	if err := w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
		return fmt.Errorf("failed to write point for span: %w", err)
	}

	return nil
}

func (c *OtelTracesToLineProtocol) spanEventToLP(traceID, spanID string, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, spanEvent *otlptrace.Span_Event) (measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, err error) {
	measurement = common.MeasurementLogs
	tags = make(map[string]string)
	fields = make(map[string]interface{})

	tags = resourceToTags(c.logger, resource, tags)
	tags = instrumentationLibraryToTags(instrumentationLibrary, tags)

	tags[common.AttributeTraceID] = traceID
	tags[common.AttributeSpanID] = spanID
	if name := spanEvent.Name; name != "" {
		tags[common.AttributeName] = name
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
		fields[common.AttributeDroppedEventAttributesCount] = droppedAttributesCount
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

func (c *OtelTracesToLineProtocol) spanLinkToLP(traceID, spanID string, spanLink *otlptrace.Span_Link) (measurement string, tags map[string]string, fields map[string]interface{}, err error) {
	measurement = common.MeasurementSpanLinks
	tags = make(map[string]string)
	fields = make(map[string]interface{})

	tags[common.AttributeTraceID] = traceID
	tags[common.AttributeSpanID] = spanID

	if linkedTraceID := hex.EncodeToString(spanLink.TraceId); len(linkedTraceID) == 0 {
		err = errors.New("span link has no trace ID")
		return
	} else {
		tags[common.AttributeLinkedTraceID] = linkedTraceID
	}

	if linkedSpanID := hex.EncodeToString(spanLink.SpanId); len(linkedSpanID) == 0 {
		err = errors.New("span link has no span ID")
		return
	} else {
		tags[common.AttributeLinkedSpanID] = linkedSpanID
	}

	if traceState := spanLink.TraceState; traceState != "" {
		tags[common.AttributeTraceState] = traceState
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
		fields[common.AttributeDroppedLinkAttributesCount] = droppedAttributesCount
	}

	if len(fields) == 0 {
		// TODO remove when tags and fields are just columns
		fields["count"] = uint64(1)
	}

	return
}
