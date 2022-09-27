package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/jaegertracing/jaeger/model"
)

const (
	// These attribute key names are influenced by the proto message keys.
	// https://github.com/open-telemetry/opentelemetry-proto/blob/abbf7b7b49a5342d0d6c0e86e91d713bbedb6580/opentelemetry/proto/trace/v1/trace.proto
	attributeTime             = "time"
	attributeTraceID          = "trace_id"
	attributeSpanID           = "span_id"
	attributeParentSpanID     = "parent_span_id"
	attributeName             = "name"
	attributeBody             = "body"
	attributeSpanKind         = "kind"
	attributeEndTimeUnixNano  = "end_time_unix_nano"
	attributeDurationNano     = "duration_nano"
	attributeStatusCode       = "otel.status_code"
	attributeStatusCodeError  = "ERROR"
	attributeLinkedTraceID    = "linked_trace_id"
	attributeLinkedSpanID     = "linked_span_id"
	attributeServiceName      = "service.name"
	attributeTelemetrySDKName = "telemetry.sdk.name"
	attributeAttribute        = "attribute"
)

func recordToSpan(record map[string]interface{}) (*model.Span, error) {
	span := model.Span{
		Process: &model.Process{
			ServiceName: "<unknown>",
		},
	}
	parentSpanRef := model.SpanRef{
		RefType: model.SpanRefType_CHILD_OF,
	}
	// TODO add more process attributes
	var err error
	for k, v := range record {
		if vv, ok := v.(string); ok && vv == "NULL" {
			continue
		}
		switch k {
		case attributeTime:
			if vv, ok := v.(time.Time); !ok {
				return nil, fmt.Errorf("time is type %T", v)
			} else {
				span.StartTime = vv
			}
		case attributeTraceID:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("trace ID is type %T", v)
			} else if span.TraceID, err = model.TraceIDFromString(vv); err != nil {
				return nil, err
			}
			parentSpanRef.TraceID = span.TraceID
		case attributeSpanID:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("span ID is type %T", v)
			} else if span.SpanID, err = model.SpanIDFromString(vv); err != nil {
				return nil, err
			}
		case attributeServiceName:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("service name is type %T", v)
			} else {
				span.Process.ServiceName = vv
			}
		case attributeName:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("operation name is type %T", v)
			} else {
				span.OperationName = vv
			}
		case attributeSpanKind:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("span kind is type %T", v)
			} else {
				switch vv {
				case "SPAN_KIND_SERVER":
					span.Tags = append(span.Tags, model.String("span.kind", "server"))
				case "SPAN_KIND_CLIENT":
					span.Tags = append(span.Tags, model.String("span.kind", "client"))
				case "SPAN_KIND_PRODUCER":
					span.Tags = append(span.Tags, model.String("span.kind", "producer"))
				case "SPAN_KIND_CONSUMER":
					span.Tags = append(span.Tags, model.String("span.kind", "consumer"))
				}
			}
		case attributeDurationNano:
			if vv, ok := v.(int64); !ok {
				return nil, fmt.Errorf("duration nanoseconds is type %T", v)
			} else {
				span.Duration = time.Duration(vv)
			}
		case attributeEndTimeUnixNano:
			// Jaeger likes duration ^^
			continue
		case attributeParentSpanID:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("parent span ID is type %T", v)
			} else {
				parentSpanRef.SpanID, err = model.SpanIDFromString(vv)
			}
			if err != nil {
				return nil, err
			}
		case attributeStatusCode:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("status code is type %T", v)
			} else {
				span.Tags = append(span.Tags, model.String(k, vv))
				if v == attributeStatusCodeError {
					span.Tags = append(span.Tags, model.Bool("error", true))
				}
			}
		case attributeAttribute:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("attribute is type %T", v)
			} else {
				m := make(map[string]interface{})
				if err = json.Unmarshal([]byte(vv), &m); err != nil {
					return nil, fmt.Errorf("failed to unmarshal JSON-encoded attributes: %w", err)
				}
				for attributeKey, attributeValue := range m {
					span.Tags = append(span.Tags, kvToKeyValue(attributeKey, attributeValue))
				}
			}
		default:
			if common.ResourceNamespace.MatchString(k) {
				span.Process.Tags = append(span.Process.Tags, kvToKeyValue(k, v))
			} else {
				span.Tags = append(span.Tags, kvToKeyValue(k, v))
			}
		}
	}

	if span.StartTime.IsZero() || (span.TraceID.High == 0 && span.TraceID.Low == 0) || span.SpanID == 0 {
		return nil, errors.New("incomplete span")
	}
	if parentSpanRef.SpanID != 0 {
		span.References = []model.SpanRef{parentSpanRef}
	}

	return &span, nil
}

func kvToKeyValue(k string, v interface{}) model.KeyValue {
	switch vv := v.(type) {
	case bool:
		return model.Bool(k, vv)
	case float64:
		return model.Float64(k, vv)
	case int64:
		return model.Int64(k, vv)
	case string:
		return model.String(k, vv)
	default:
		return model.String(k, fmt.Sprint(vv))
	}
}

func recordToLog(record map[string]interface{}) (model.TraceID, model.SpanID, *model.Log, error) {
	log := new(model.Log)
	var traceID model.TraceID
	var spanID model.SpanID
	var err error
	for k, v := range record {
		if vv, ok := v.(string); ok && vv == "NULL" {
			continue
		}
		switch k {
		case attributeTime:
			if vv, ok := v.(time.Time); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("time is type %T", v)
			} else {
				log.Timestamp = vv
			}
		case attributeTraceID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("trace ID is type %T", v)
			} else if traceID, err = model.TraceIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		case attributeSpanID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("span ID is type %T", v)
			} else if spanID, err = model.SpanIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		case attributeName:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("log name is type %T", v)
			} else {
				log.Fields = append(log.Fields, model.String("event", vv))
			}
		case attributeBody:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("log body is type %T", v)
			} else {
				log.Fields = append(log.Fields, model.String("message", vv))
			}
		default:
			log.Fields = append(log.Fields, kvToKeyValue(k, v))
		}
	}

	if log.Timestamp.IsZero() || (traceID.High == 0 && traceID.Low == 0) || spanID == 0 {
		return model.TraceID{}, 0, nil, errors.New("incomplete span event")
	}

	return traceID, spanID, log, nil
}

func recordToSpanRef(record map[string]interface{}) (model.TraceID, model.SpanID, *model.SpanRef, error) {
	spanRef := &model.SpanRef{
		RefType: model.FollowsFrom,
	}
	var traceID model.TraceID
	var spanID model.SpanID
	var err error
	for k, v := range record {
		if vv, ok := v.(string); ok && vv == "NULL" {
			continue
		}
		switch k {
		case attributeTraceID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("trace ID is type %T", v)
			} else if traceID, err = model.TraceIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		case attributeSpanID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("span ID is type %T", v)
			} else if spanID, err = model.SpanIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		case attributeLinkedTraceID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("linked trace ID is type %T", v)
			} else if spanRef.TraceID, err = model.TraceIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		case attributeLinkedSpanID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("linked span ID is type %T", v)
			} else if spanRef.SpanID, err = model.SpanIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		default:
			// OpenTelemetry links do not have timestamps/attributes/fields/labels
		}
	}

	if (spanRef.TraceID.High == 0 && spanRef.TraceID.Low == 0) || spanRef.SpanID == 0 || (traceID.High == 0 && traceID.Low == 0) || spanID == 0 {
		return model.TraceID{}, 0, nil, errors.New("incomplete span link")
	}

	return traceID, spanID, spanRef, nil
}
