package otel2influx

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"
	"go.uber.org/multierr"
	"golang.org/x/exp/maps"

	"github.com/influxdata/influxdb-observability/common"
)

type OtelTracesToLineProtocolConfig struct {
	Logger common.Logger
	Writer InfluxWriter
	// SpanDimensions are span attributes to be used as line protocol tags.
	// These are always included as tags:
	// - trace ID
	// - span ID
	// The default values are strongly recommended for use with Jaeger:
	// - service.name
	// - span.name
	// Other common attributes can be found here:
	// - https://github.com/open-telemetry/opentelemetry-collector/tree/main/semconv
	SpanDimensions []string
}

func DefaultOtelTracesToLineProtocolConfig() *OtelTracesToLineProtocolConfig {
	return &OtelTracesToLineProtocolConfig{
		Logger: new(common.NoopLogger),
		Writer: new(NoopInfluxWriter),
		SpanDimensions: []string{
			semconv.AttributeServiceName,
			common.AttributeSpanName,
		},
	}
}

type OtelTracesToLineProtocol struct {
	logger       common.Logger
	influxWriter InfluxWriter

	spanDimensions map[string]struct{}
}

func NewOtelTracesToLineProtocol(config *OtelTracesToLineProtocolConfig) (*OtelTracesToLineProtocol, error) {
	spanDimensions := make(map[string]struct{}, len(config.SpanDimensions))
	{
		duplicateDimensions := make(map[string]struct{})
		for _, k := range config.SpanDimensions {
			if _, found := spanDimensions[k]; found {
				duplicateDimensions[k] = struct{}{}
			} else {
				spanDimensions[k] = struct{}{}
			}
		}
		if len(duplicateDimensions) > 0 {
			return nil, fmt.Errorf("duplicate dimension(s) configured: %s",
				strings.Join(maps.Keys(duplicateDimensions), ","))
		}
	}

	return &OtelTracesToLineProtocol{
		logger:         config.Logger,
		influxWriter:   config.Writer,
		spanDimensions: spanDimensions,
	}, nil
}

func (c *OtelTracesToLineProtocol) WriteTraces(ctx context.Context, td ptrace.Traces) error {
	batch := c.influxWriter.NewBatch()
	for i := 0; i < td.ResourceSpans().Len(); i++ {
		resourceSpans := td.ResourceSpans().At(i)
		for j := 0; j < resourceSpans.ScopeSpans().Len(); j++ {
			scopeSpans := resourceSpans.ScopeSpans().At(j)
			for k := 0; k < scopeSpans.Spans().Len(); k++ {
				span := scopeSpans.Spans().At(k)
				if err := c.enqueueSpan(ctx, span, scopeSpans.Scope().Attributes(), resourceSpans.Resource().Attributes(), batch); err != nil {
					return consumererror.NewPermanent(fmt.Errorf("failed to convert OTLP span to line protocol: %w", err))
				}
			}
		}
	}
	return batch.WriteBatch(ctx)
}

func (c *OtelTracesToLineProtocol) enqueueSpan(ctx context.Context, span ptrace.Span, scopeAttributes, resourceAttributes pcommon.Map, batch InfluxWriterBatch) (err error) {
	defer func() {
		if r := recover(); r != nil {
			var rerr error
			switch v := r.(type) {
			case error:
				rerr = v
			case string:
				rerr = errors.New(v)
			default:
				rerr = fmt.Errorf("%+v", r)
			}
			err = multierr.Combine(err, rerr)
		}
	}()

	traceID := span.TraceID()
	if traceID.IsEmpty() {
		err = errors.New("span has no trace ID")
		return
	}
	spanID := span.SpanID()
	if spanID.IsEmpty() {
		err = errors.New("span has no span ID")
		return
	}

	measurement := common.MeasurementSpans
	tags := make(map[string]string, len(c.spanDimensions)+2)
	fields := make(map[string]interface{}, scopeAttributes.Len()+resourceAttributes.Len()+10)

	droppedAttributesCount := uint64(span.DroppedAttributesCount())
	attributesField := make(map[string]any)

	for _, attributes := range []pcommon.Map{resourceAttributes, scopeAttributes, span.Attributes()} {
		attributes.Range(func(k string, v pcommon.Value) bool {
			if _, found := c.spanDimensions[k]; found {
				if _, found = tags[k]; found {
					c.logger.Debug("dimension %s already exists as a tag", k)
					attributesField[k] = v.AsRaw()
				}
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
			droppedAttributesCount += uint64(span.Attributes().Len())
		} else {
			fields[common.AttributeAttributes] = string(marshalledAttributes)
		}
	}

	if traceState := span.TraceState().AsRaw(); traceState != "" {
		fields[common.AttributeTraceState] = traceState
	}
	if parentSpanID := span.ParentSpanID(); !parentSpanID.IsEmpty() {
		fields[common.AttributeParentSpanID] = hex.EncodeToString(parentSpanID[:])
	}
	if name := span.Name(); name != "" {
		fields[common.AttributeSpanName] = name
	}
	if kind := span.Kind(); kind != ptrace.SpanKindUnspecified {
		fields[common.AttributeSpanKind] = kind.String()
	}

	ts := span.StartTimestamp().AsTime()
	if ts.IsZero() {
		err = errors.New("span has no timestamp")
		return
	}

	if endTime := span.EndTimestamp().AsTime(); !endTime.IsZero() {
		fields[common.AttributeEndTimeUnixNano] = endTime.UnixNano()
		fields[common.AttributeDurationNano] = endTime.Sub(ts).Nanoseconds()
	}

	droppedEventsCount := uint64(span.DroppedEventsCount())
	for i := 0; i < span.Events().Len(); i++ {
		if err = c.writeSpanEvent(ctx, traceID, spanID, span.Events().At(i), batch); err != nil {
			droppedEventsCount++
			c.logger.Debug("invalid span event", err)
		}
	}
	if droppedEventsCount > 0 {
		fields[common.AttributeDroppedEventsCount] = droppedEventsCount
	}

	droppedLinksCount := uint64(span.DroppedLinksCount())
	for i := 0; i < span.Links().Len(); i++ {
		if err = c.writeSpanLink(ctx, traceID, spanID, ts, span.Links().At(i), batch); err != nil {
			droppedLinksCount++
			c.logger.Debug("invalid span link", err)
		}
	}
	if droppedLinksCount > 0 {
		fields[common.AttributeDroppedLinksCount] = droppedLinksCount
	}

	status := span.Status()
	switch status.Code() {
	case ptrace.StatusCodeUnset:
	case ptrace.StatusCodeOk, ptrace.StatusCodeError:
		fields[semconv.OtelStatusCode] = status.Code().String()
	default:
		c.logger.Debug("status code not recognized", "code", status.Code())
	}
	if message := status.Message(); message != "" {
		fields[semconv.OtelStatusDescription] = message
	}

	tags[common.AttributeTraceID] = hex.EncodeToString(traceID[:])
	tags[common.AttributeSpanID] = hex.EncodeToString(spanID[:])

	for k := range tags {
		if _, found := fields[k]; found {
			c.logger.Debug("tag already exists as a field; field will be dropped", "tag key", k)
			droppedAttributesCount++
		}
	}
	if droppedAttributesCount > 0 {
		fields[common.AttributeDroppedAttributesCount] = droppedAttributesCount
	}

	if err = batch.EnqueuePoint(measurement, tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
		return fmt.Errorf("failed to enqueue point for span: %w", err)
	}

	return nil
}

func (c *OtelTracesToLineProtocol) writeSpanEvent(ctx context.Context, traceID pcommon.TraceID, spanID pcommon.SpanID, spanEvent ptrace.SpanEvent, batch InfluxWriterBatch) error {
	fields := make(map[string]interface{}, 2)
	if name := spanEvent.Name(); name != "" {
		fields[semconv.AttributeEventName] = name
	}

	droppedAttributesCount := uint64(spanEvent.DroppedAttributesCount())
	attributes := make(map[string]interface{}, spanEvent.Attributes().Len())
	spanEvent.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			droppedAttributesCount++
			c.logger.Debug("span event attribute key is empty")
		} else if v, err := AttributeValueToInfluxFieldValue(v); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span event attribute value", err)
		} else {
			attributes[k] = v
		}
		return true
	})
	if len(attributes) > 0 {
		marshalledAttributes, err := json.Marshal(attributes)
		if err != nil {
			c.logger.Debug("failed to marshal attributes to JSON", err)
			droppedAttributesCount += uint64(len(attributes))
		} else {
			fields[common.AttributeAttributes] = string(marshalledAttributes)
		}
	}
	if droppedAttributesCount > 0 {
		fields[common.AttributeDroppedAttributesCount] = droppedAttributesCount
	}

	tags := map[string]string{
		common.AttributeTraceID: hex.EncodeToString(traceID[:]),
		common.AttributeSpanID:  hex.EncodeToString(spanID[:]),
	}

	err := batch.EnqueuePoint(common.MeasurementLogs, tags, fields, spanEvent.Timestamp().AsTime(), common.InfluxMetricValueTypeUntyped)
	if err != nil {
		return fmt.Errorf("failed to write point for span event: %w", err)
	}
	return nil
}

func (c *OtelTracesToLineProtocol) writeSpanLink(ctx context.Context, traceID pcommon.TraceID, spanID pcommon.SpanID, ts time.Time, spanLink ptrace.SpanLink, batch InfluxWriterBatch) error {
	fields := make(map[string]interface{}, 2)

	linkedTraceID := spanLink.TraceID()
	if linkedTraceID.IsEmpty() {
		return errors.New("span link has no trace ID")
	}
	linkedSpanID := spanLink.SpanID()
	if linkedSpanID.IsEmpty() {
		return errors.New("span link has no span ID")
	}

	tags := map[string]string{
		common.AttributeTraceID:       hex.EncodeToString(traceID[:]),
		common.AttributeSpanID:        hex.EncodeToString(spanID[:]),
		common.AttributeLinkedTraceID: hex.EncodeToString(linkedTraceID[:]),
		common.AttributeLinkedSpanID:  hex.EncodeToString(linkedSpanID[:]),
	}

	if traceState := spanLink.TraceState().AsRaw(); traceState != "" {
		fields[common.AttributeTraceState] = traceState
	}

	droppedAttributesCount := uint64(spanLink.DroppedAttributesCount())
	attributes := make(map[string]interface{}, spanLink.Attributes().Len())
	spanLink.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			droppedAttributesCount++
			c.logger.Debug("span link attribute key is empty")
		} else if v, err := AttributeValueToInfluxFieldValue(v); err != nil {
			droppedAttributesCount++
			c.logger.Debug("invalid span link attribute value", err)
		} else {
			attributes[k] = v
		}
		return true
	})
	if len(attributes) > 0 {
		marshalledAttributes, err := json.Marshal(attributes)
		if err != nil {
			c.logger.Debug("failed to marshal attributes to JSON", err)
			droppedAttributesCount += uint64(len(attributes))
		} else {
			fields[common.AttributeAttributes] = string(marshalledAttributes)
		}
	}
	if droppedAttributesCount > 0 {
		fields[common.AttributeDroppedAttributesCount] = droppedAttributesCount
	}

	if err := batch.EnqueuePoint(common.MeasurementSpanLinks, tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
		return fmt.Errorf("failed to write point for span link: %w", err)
	}
	return nil
}
