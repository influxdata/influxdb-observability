package internal

import (
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"strings"
	"time"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/jaegertracing/jaeger/model"
	"github.com/opentracing/opentracing-go/ext"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"

	"github.com/influxdata/influxdb-observability/common"
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
		if vv, ok := v.(string); (ok && vv == "NULL") || (!ok && v == nil) {
			continue
		}
		switch k {
		case common.AttributeTime:
			if vv, ok := v.(time.Time); !ok {
				return nil, fmt.Errorf("time is type %T", v)
			} else {
				span.StartTime = vv
			}
		case common.AttributeTraceID:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("trace ID is type %T", v)
			} else if span.TraceID, err = model.TraceIDFromString(vv); err != nil {
				return nil, err
			}
			parentSpanRef.TraceID = span.TraceID
		case common.AttributeSpanID:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("span ID is type %T", v)
			} else if span.SpanID, err = model.SpanIDFromString(vv); err != nil {
				return nil, err
			}
		case semconv.AttributeServiceName:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("service name is type %T", v)
			} else {
				span.Process.ServiceName = vv
			}
		case common.AttributeSpanName:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("operation name is type %T", v)
			} else {
				span.OperationName = vv
			}
		case common.AttributeSpanKind:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("span kind is type %T", v)
			} else {
				switch vv {
				case ptrace.SpanKindServer.String():
					span.Tags = append(span.Tags, model.String(string(ext.SpanKind), string(ext.SpanKindRPCServerEnum)))
				case ptrace.SpanKindClient.String():
					span.Tags = append(span.Tags, model.String(string(ext.SpanKind), string(ext.SpanKindRPCClientEnum)))
				case ptrace.SpanKindProducer.String():
					span.Tags = append(span.Tags, model.String(string(ext.SpanKind), string(ext.SpanKindProducerEnum)))
				case ptrace.SpanKindConsumer.String():
					span.Tags = append(span.Tags, model.String(string(ext.SpanKind), string(ext.SpanKindConsumerEnum)))
				case ptrace.SpanKindInternal.String():
					span.Tags = append(span.Tags, model.String(string(ext.SpanKind), "internal"))
				}
			}
		case common.AttributeDurationNano:
			if vv, ok := v.(int64); !ok {
				return nil, fmt.Errorf("duration nanoseconds is type %T", v)
			} else {
				span.Duration = time.Duration(vv)
			}
		case common.AttributeEndTimeUnixNano:
			// Jaeger likes duration ^^
			continue
		case common.AttributeParentSpanID:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("parent span ID is type %T", v)
			} else {
				parentSpanRef.SpanID, err = model.SpanIDFromString(vv)
			}
			if err != nil {
				return nil, err
			}
		case semconv.OtelStatusCode:
			if vv, ok := v.(string); !ok {
				return nil, fmt.Errorf("status code is type %T", v)
			} else {
				span.Tags = append(span.Tags, model.String(k, vv))
				if v == ptrace.StatusCodeError {
					span.Tags = append(span.Tags, model.Bool("error", true))
				}
			}
		case common.AttributeAttributes:
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
		case common.AttributeTime:
			if vv, ok := v.(time.Time); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("time is type %T", v)
			} else {
				log.Timestamp = vv
			}
		case common.AttributeTraceID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("trace ID is type %T", v)
			} else if traceID, err = model.TraceIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		case common.AttributeSpanID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("span ID is type %T", v)
			} else if spanID, err = model.SpanIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		case semconv.AttributeEventName:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("log name is type %T", v)
			} else {
				log.Fields = append(log.Fields, model.String("event", vv))
			}
		case common.AttributeBody:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("log body is type %T", v)
			} else {
				log.Fields = append(log.Fields, model.String("message", vv))
			}
		case common.AttributeAttributes:
			vv, ok := v.(string)
			if !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("log attributes attribute is type %T", v)
			}
			var m map[string]interface{}
			if err = json.Unmarshal([]byte(vv), &m); err != nil {
				return model.TraceID{}, 0, nil, fmt.Errorf("failed to unmarshal attributes from JSON: %w", err)
			}
			for mk, mv := range m {
				switch mvv := mv.(type) {
				case nil:
					log.Fields = append(log.Fields, model.String(mk, ""))
				case bool:
					log.Fields = append(log.Fields, model.Bool(mk, mvv))
				case float64:
					if intPart, fracPart := math.Modf(mvv); fracPart == 0 {
						log.Fields = append(log.Fields, model.Int64(mk, int64(intPart)))
					} else {
						log.Fields = append(log.Fields, model.Float64(mk, mvv))
					}
				case string:
					log.Fields = append(log.Fields, model.String(mk, mvv))
				case []interface{}:
					s := make([]string, len(mvv))
					for i := range mvv {
						if mvv[i] == nil {
							s[i] = ""
						} else {
							s[i] = fmt.Sprint(mvv[i])
						}
					}
					log.Fields = append(log.Fields, model.String(mk, strings.Join(s, ",")))
				default:
					// ignore
				}
			}
		case semconv.AttributeServiceName:
			// The span has this information, no need to duplicate
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
		case common.AttributeTraceID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("trace ID is type %T", v)
			} else if traceID, err = model.TraceIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		case common.AttributeSpanID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("span ID is type %T", v)
			} else if spanID, err = model.SpanIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		case common.AttributeLinkedTraceID:
			if vv, ok := v.(string); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("linked trace ID is type %T", v)
			} else if spanRef.TraceID, err = model.TraceIDFromString(vv); err != nil {
				return model.TraceID{}, 0, nil, err
			}
		case common.AttributeLinkedSpanID:
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

var errTableNotFound = regexp.MustCompile(`table '\S+' not found`)

func isTableNotFound(err error) bool {
	aerr, ok := err.(adbc.Error)
	return ok && errTableNotFound.MatchString(aerr.Msg)
}
