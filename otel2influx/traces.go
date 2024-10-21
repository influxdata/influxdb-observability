package otel2influx

import (
	"context"
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

const (
	CustomKeyAttribute = "emq_service"
)

type Trace struct {
	Table string `mapstructure:"table"`
	// SpanDimensions are span attributes to be used as line protocol tags.
	// Besides attributes, the following span structures can also be specified as tags:
	// - trace_id (trace ID)
	// - span_id (span ID)
	// - span.name (span name)
	// - span.kind (span kind)
	// - parent_span_id (span parent)
	// Other common attributes can be found here:
	// - https://opentelemetry.io/docs/specs/semconv/
	SpanDimensions []string `mapstructure:"span_dimensions"`
	// SpanFields are span attributes to be used as line protocol fields.
	// Besides attributes, the following span structures are always included as fields if not specified as tags:
	// - trace_id
	// - span_id
	// - span.name
	// - span.kind
	// - parent_span_id
	// - trace_state
	// - end_time_unix_nano
	// - duration_nano
	// - otel.status_code (span status)
	// - otel.status_description (span status message)
	// SpanFields can be empty.
	SpanFields []string `mapstructure:"span_fields"`
}

type OtelTracesToLineProtocolConfig struct {
	Logger       common.Logger
	Writer       InfluxWriter
	GlobalTrace  Trace
	CustomTraces map[string]Trace
}

func DefaultOtelTracesToLineProtocolConfig() *OtelTracesToLineProtocolConfig {
	return &OtelTracesToLineProtocolConfig{
		Logger: new(common.NoopLogger),
		Writer: new(NoopInfluxWriter),
		GlobalTrace: Trace{
			SpanDimensions: []string{
				common.AttributeTraceID,
				common.AttributeSpanID,
			},
		},
	}
}

type OtelTracesToLineProtocol struct {
	logger       common.Logger
	influxWriter InfluxWriter

	globalTable          string
	globalSpanDimensions map[string]struct{} //key: attribute name
	globalSpanFields     map[string]struct{}

	customTable          map[string]string
	customSpanDimensions map[string]map[string]struct{} //outer key: custom key; inner key: attribute name
	customSpanFields     map[string]map[string]struct{}
}

func NewOtelTracesToLineProtocol(config *OtelTracesToLineProtocolConfig) (*OtelTracesToLineProtocol, error) {
	globalTable := config.GlobalTrace.Table
	globalSpanDimensions := make(map[string]struct{}, len(config.GlobalTrace.SpanDimensions))
	globalSpanFields := make(map[string]struct{}, len(config.GlobalTrace.SpanFields))
	{
		duplicateDimensions := make(map[string]struct{})
		for _, k := range config.GlobalTrace.SpanDimensions {
			if _, found := globalSpanDimensions[k]; found {
				duplicateDimensions[k] = struct{}{}
			} else {
				globalSpanDimensions[k] = struct{}{}
			}
		}
		if len(duplicateDimensions) > 0 {
			return nil, fmt.Errorf("duplicate span dimension(s) configured: %s",
				strings.Join(maps.Keys(duplicateDimensions), ","))
		}
		duplicateFields := make(map[string]struct{})
		for _, k := range config.GlobalTrace.SpanFields {
			if _, found := globalSpanFields[k]; found {
				duplicateFields[k] = struct{}{}
			} else {
				globalSpanFields[k] = struct{}{}
			}
		}
		if len(duplicateFields) > 0 {
			return nil, fmt.Errorf("duplicate span field(s) configured: %s",
				strings.Join(maps.Keys(duplicateFields), ","))
		}
	}

	customTable := map[string]string{}
	customSpanDimensions := map[string]map[string]struct{}{}
	customSpanFields := map[string]map[string]struct{}{}
	{
		for key, custom := range config.CustomTraces {
			customTable[key] = custom.Table
			duplicateDimensions := make(map[string]struct{})
			for _, k := range custom.SpanDimensions {
				if outer, outerFound := customSpanDimensions[key]; outerFound {
					if _, innerFound := outer[k]; innerFound {
						duplicateDimensions[k] = struct{}{}
					} else {
						outer[k] = struct{}{}
						customSpanDimensions[key] = outer
					}
				} else {
					customSpanDimensions[key] = map[string]struct{}{k: {}}
				}
			}
			duplicateFields := make(map[string]struct{})
			for _, k := range custom.SpanFields {
				if outer, outerFound := customSpanFields[key]; outerFound {
					if _, innerFound := outer[k]; innerFound {
						duplicateFields[k] = struct{}{}
					} else {
						outer[k] = struct{}{}
						customSpanFields[key] = outer
					}
				} else {
					customSpanFields[key] = map[string]struct{}{k: {}}
				}
			}

			if len(duplicateDimensions) > 0 {
				return nil, fmt.Errorf("duplicate custom span dimension(s) configured: %s",
					strings.Join(maps.Keys(duplicateDimensions), ","))
			}
			if len(duplicateFields) > 0 {
				return nil, fmt.Errorf("duplicate custom span field(s) configured: %s",
					strings.Join(maps.Keys(duplicateFields), ","))
			}
		}
	}

	return &OtelTracesToLineProtocol{
		logger:               config.Logger,
		influxWriter:         config.Writer,
		globalTable:          globalTable,
		globalSpanDimensions: globalSpanDimensions,
		globalSpanFields:     globalSpanFields,
		customTable:          customTable,
		customSpanDimensions: customSpanDimensions,
		customSpanFields:     customSpanFields,
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

	measurement := c.globalTable
	tags := map[string]string{}
	fields := map[string]interface{}{}

	spanDimensions := c.globalSpanDimensions
	spanFields := c.globalSpanFields

	customKey := ""
	for _, attributes := range []pcommon.Map{resourceAttributes, scopeAttributes, span.Attributes()} {
		attributes.Range(func(k string, v pcommon.Value) bool {
			if k == CustomKeyAttribute {
				customKey = v.AsString()
				return false
			}
			return true
		})
	}
	if len(customKey) > 0 {
		customTable, found := c.customTable[customKey]
		if found {
			measurement = customTable
		}
		customTags, found := c.customSpanDimensions[customKey]
		if found {
			spanDimensions = customTags
		}
		customFields, found := c.customSpanFields[customKey]
		if found {
			spanFields = customFields
		}
	}

	droppedAttributesCount := uint64(span.DroppedAttributesCount())
	attributesField := make(map[string]any)

	if _, found := spanDimensions[common.AttributeTraceID]; found {
		tags[common.AttributeTraceID] = traceID.String()
	} else {
		fields[common.AttributeTraceID] = traceID.String()
	}

	if _, found := spanDimensions[common.AttributeSpanID]; found {
		tags[common.AttributeSpanID] = spanID.String()
	} else {
		fields[common.AttributeSpanID] = spanID.String()
	}

	if traceState := span.TraceState().AsRaw(); traceState != "" {
		fields[common.AttributeTraceState] = traceState
	}
	if parentSpanID := span.ParentSpanID(); !parentSpanID.IsEmpty() {
		if _, found := spanDimensions[common.AttributeParentSpanID]; found {
			tags[common.AttributeParentSpanID] = parentSpanID.String()
		} else {
			fields[common.AttributeParentSpanID] = parentSpanID.String()
		}
	}
	if name := span.Name(); name != "" {
		if _, found := spanDimensions[common.AttributeSpanName]; found {
			tags[common.AttributeSpanName] = name
		} else {
			fields[common.AttributeSpanName] = name
		}
	}
	if kind := span.Kind(); kind != ptrace.SpanKindUnspecified {
		if _, found := spanDimensions[common.AttributeSpanKind]; found {
			tags[common.AttributeSpanKind] = kind.String()
		} else {
			fields[common.AttributeSpanKind] = kind.String()
		}
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

	for _, attributes := range []pcommon.Map{resourceAttributes, scopeAttributes, span.Attributes()} {
		attributes.Range(func(k string, v pcommon.Value) bool {
			asAttr := true
			if _, found := spanDimensions[k]; found {
				if _, found = tags[k]; found {
					c.logger.Debug("attribute %s already exists as a tag", k)
					attributesField[k] = v.AsRaw()
				}
				tags[k] = v.AsString()
				asAttr = false
			}
			if _, found := spanFields[k]; found {
				if _, found = fields[k]; found {
					c.logger.Debug("attribute %s already exists as a field", k)
					attributesField[k] = v.AsRaw()
				}
				fields[k] = v.AsString()
				asAttr = false
			}
			if asAttr {
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

	droppedEventsCount := uint64(span.DroppedEventsCount())
	eventMeasurement := measurement + "_logs"
	for i := 0; i < span.Events().Len(); i++ {
		if err = c.enqueueSpanEvent(ctx, eventMeasurement, traceID, spanID, span.Events().At(i), batch); err != nil {
			droppedEventsCount++
			c.logger.Debug("invalid span event", err)
		}
	}
	if droppedEventsCount > 0 {
		fields[common.AttributeDroppedEventsCount] = droppedEventsCount
	}

	droppedLinksCount := uint64(span.DroppedLinksCount())
	linkMeasurement := measurement + "_links"
	for i := 0; i < span.Links().Len(); i++ {
		if err = c.writeSpanLink(ctx, linkMeasurement, traceID, spanID, ts, span.Links().At(i), batch); err != nil {
			droppedLinksCount++
			c.logger.Debug("invalid span link", err)
		}
	}
	if droppedLinksCount > 0 {
		fields[common.AttributeDroppedLinksCount] = droppedLinksCount
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

	if err = batch.EnqueuePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
		return fmt.Errorf("failed to enqueue point for span: %w", err)
	}

	return nil
}

func (c *OtelTracesToLineProtocol) enqueueSpanEvent(ctx context.Context, measurement string, traceID pcommon.TraceID, spanID pcommon.SpanID, spanEvent ptrace.SpanEvent, batch InfluxWriterBatch) error {
	fields := make(map[string]interface{}, 2)
	if name := spanEvent.Name(); name != "" {
		fields[semconv.AttributeEventName] = name
	}

	if spanEvent.Attributes().Len() > 0 {
		droppedAttributesCount := uint64(spanEvent.DroppedAttributesCount())
		marshalledAttributes, err := json.Marshal(spanEvent.Attributes().AsRaw())
		if err != nil {
			c.logger.Debug("failed to marshal attributes to JSON", err)
			droppedAttributesCount += uint64(spanEvent.Attributes().Len())
		} else {
			fields[common.AttributeAttributes] = string(marshalledAttributes)
		}
		if droppedAttributesCount > 0 {
			fields[common.AttributeDroppedAttributesCount] = droppedAttributesCount
		}
	}

	tags := map[string]string{
		common.AttributeTraceID: traceID.String(),
		common.AttributeSpanID:  spanID.String(),
	}

	err := batch.EnqueuePoint(ctx, measurement, tags, fields, spanEvent.Timestamp().AsTime(), common.InfluxMetricValueTypeUntyped)
	if err != nil {
		return fmt.Errorf("failed to write point for span event: %w", err)
	}
	return nil
}

func (c *OtelTracesToLineProtocol) writeSpanLink(ctx context.Context, measurement string, traceID pcommon.TraceID, spanID pcommon.SpanID, ts time.Time, spanLink ptrace.SpanLink, batch InfluxWriterBatch) error {
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
		common.AttributeTraceID:       traceID.String(),
		common.AttributeSpanID:        spanID.String(),
		common.AttributeLinkedTraceID: linkedTraceID.String(),
		common.AttributeLinkedSpanID:  linkedSpanID.String(),
	}

	if traceState := spanLink.TraceState().AsRaw(); traceState != "" {
		fields[common.AttributeTraceState] = traceState
	}

	if spanLink.Attributes().Len() > 0 {
		droppedAttributesCount := uint64(spanLink.DroppedAttributesCount())
		marshalledAttributes, err := json.Marshal(spanLink.Attributes().AsRaw())
		if err != nil {
			c.logger.Debug("failed to marshal attributes to JSON", err)
			droppedAttributesCount += uint64(spanLink.Attributes().Len())
		} else {
			fields[common.AttributeAttributes] = string(marshalledAttributes)
		}
		if droppedAttributesCount > 0 {
			fields[common.AttributeDroppedAttributesCount] = droppedAttributesCount
		}
	}

	if err := batch.EnqueuePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeUntyped); err != nil {
		return fmt.Errorf("failed to write point for span link: %w", err)
	}
	return nil
}
