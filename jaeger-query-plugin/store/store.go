package store

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/url"
	"regexp"
	"time"

	"github.com/hashicorp/go-hclog"
	"github.com/influxdata/influxdb-observability/jaeger-query-plugin/config"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var (
	_ shared.StoragePlugin   = (*Store)(nil)
	_ spanstore.Reader       = (*Store)(nil)
	_ dependencystore.Reader = (*Store)(nil)
)

const (
	measurementSpans     = "spans"
	measurementLogs      = "logs"
	measurementSpanLinks = "span-links"

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
)

type Store struct {
	host     string
	database string

	httpClient *http.Client

	logger hclog.Logger
}

func NewStore(conf *config.Configuration, logger hclog.Logger) (*Store, error) {
	return &Store{
		host:       conf.Host,
		database:   conf.Database,
		httpClient: &http.Client{Timeout: conf.Timeout},
		logger:     logger,
	}, nil
}

func (s *Store) SpanReader() spanstore.Reader {
	return s
}

func (s *Store) DependencyReader() dependencystore.Reader {
	return s
}

func (s *Store) SpanWriter() spanstore.Writer {
	panic("writer not implemented, use the InfluxDB OpenTelemetry Collector exporter")
}

func (s *Store) executeQuery(ctx context.Context, query string, f func(record map[string]interface{}) error, params ...interface{}) error {
	u, err := url.Parse(s.host)
	if err != nil {
		return err
	}
	u.Path = fmt.Sprintf("iox/api/v1/databases/%s/query", s.database)
	q := make(url.Values) // TODO try POST and see if double quotes problem goes away
	q.Add("q", fmt.Sprintf(query, params...))
	q.Add("format", "json")
	u.RawQuery = q.Encode()
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u.String(), http.NoBody)
	if err != nil {
		return err
	}

	res, err := s.httpClient.Do(req)
	if err != nil {
		return err
	}
	if res.StatusCode == 400 {
		// TODO this is a hack
		// 400 suggests "not found" as in "zero results"
		return nil
	}
	if res.StatusCode/100 != 2 {
		return fmt.Errorf("query status %s", res.Status)
	}

	var m []map[string]interface{}
	decoder := json.NewDecoder(res.Body)
	if err = decoder.Decode(&m); err != nil {
		return err
	}

	for _, line := range m {
		err = f(line)
		if err != nil {
			return err
		}
	}

	return nil
}

func (s *Store) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	// Get spans
	spans := make(map[model.SpanID]*model.Span)

	f := func(record map[string]interface{}) error {
		span, err := recordToSpan(record)
		if err != nil {
			s.logger.Warn("failed to convert span to Span", "error", err)
		} else {
			spans[span.SpanID] = span
		}
		return nil
	}
	err := s.executeQuery(ctx, queryGetTraceSpans(traceID), f, traceIDToString(traceID))
	if err != nil {
		return nil, err
	}

	// Get events
	f = func(record map[string]interface{}) error {
		_, spanID, log, err := recordToLog(record)
		if err != nil {
			s.logger.Warn("failed to convert event to Log", "error", err)
		} else if span, ok := spans[spanID]; !ok {
			s.logger.Warn("span event contains unknown span ID")
		} else {
			// TODO filter span attributes duplicated in logs
			span.Logs = append(span.Logs, *log)
		}
		return nil
	}
	err = s.executeQuery(ctx, queryGetTraceEvents(traceID), f, traceIDToString(traceID))
	if err != nil {
		return nil, err
	}

	// Get links
	f = func(record map[string]interface{}) error {
		_, spanID, spanRef, err := recordToSpanRef(record)
		if err != nil {
			s.logger.Warn("failed to convert link to SpanRef", "error", err)
		} else if span, found := spans[spanID]; !found {
			s.logger.Warn("link contains unknown span ID")
		} else {
			span.References = append(span.References, *spanRef)
		}
		return nil
	}

	err = s.executeQuery(ctx, queryGetTraceLinks(traceID), f, traceIDToString(traceID))
	if err != nil {
		return nil, err
	}

	// Assemble trace
	trace := &model.Trace{
		Spans: make([]*model.Span, 0, len(spans)),
	}
	for _, span := range spans {
		trace.Spans = append(trace.Spans, span)
	}
	return trace, nil
}

// https://github.com/open-telemetry/opentelemetry-specification/tree/v1.0.1/specification/resource/semantic_conventions
var resourceNamespace = regexp.MustCompile(`^(service\.|telemetry\.|container\.|process\.|host\.|os\.|cloud\.|deployment\.|k8s\.|aws\.|gcp\.|azure\.|faas\.name|faas\.id|faas\.version|faas\.instance|faas\.max_memory)`)

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
		switch k {
		case attributeTime:
			if vv, ok := v.(float64); !ok {
				return nil, fmt.Errorf("time is type %T", v)
			} else {
				span.StartTime = time.Unix(0, int64(vv))
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
			if vv, ok := v.(float64); !ok {
				return nil, fmt.Errorf("duration nanoseconds is type %T", v)
			} else {
				span.Duration = time.Duration(int64(vv))
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
		default:
			if resourceNamespace.MatchString(k) {
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
	// For now, only the scalar types used by encoding/json are used.
	switch vv := v.(type) {
	case bool:
		return model.Bool(k, vv)
	case float64:
		if vvi := int64(vv); vv == float64(vvi) {
			return model.Int64(k, vvi)
		}
		return model.Float64(k, vv)
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
		switch k {
		case attributeTime:
			if vv, ok := v.(float64); !ok {
				return model.TraceID{}, 0, nil, fmt.Errorf("time is type %T", v)
			} else {
				log.Timestamp = time.Unix(0, int64(vv))
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

func (s *Store) GetServices(ctx context.Context) ([]string, error) {
	var services []string
	f := func(record map[string]interface{}) error {
		if serviceName, found := record[attributeServiceName]; found {
			services = append(services, serviceName.(string))
		}
		return nil
	}

	err := s.executeQuery(ctx, queryGetServices, f)
	if err != nil {
		return nil, err
	}
	return services, nil
}

func (s *Store) GetOperations(ctx context.Context, query spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	var operations []spanstore.Operation
	f := func(record map[string]interface{}) error {
		if operationName, found := record[attributeName]; found {
			operations = append(operations, spanstore.Operation{Name: operationName.(string)})
		}
		return nil
	}

	err := s.executeQuery(ctx, queryGetOperations, f, query.ServiceName)
	if err != nil {
		return nil, err
	}
	return operations, nil
}

func (s *Store) FindTraces(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	query := queryFindTraceIDs(traceQueryParameters)
	var traceIDs []model.TraceID
	f := func(record map[string]interface{}) error {
		if traceIDString, found := record[attributeTraceID].(string); found {
			traceID, err := model.TraceIDFromString(traceIDString)
			if err != nil {
				return err
			}
			traceIDs = append(traceIDs, traceID)
		}
		return nil
	}

	err := s.executeQuery(ctx, query, f)
	if err != nil {
		return nil, err
	}

	traces := make(map[model.TraceID]map[model.SpanID]*model.Span)

	query = queryGetTraceSpans(traceIDs...)
	f = func(record map[string]interface{}) error {
		if span, err := recordToSpan(record); err != nil {
			return err
		} else if trace, found := traces[span.TraceID]; !found {
			traces[span.TraceID] = map[model.SpanID]*model.Span{span.SpanID: span}
		} else {
			trace[span.SpanID] = span
		}
		return nil
	}

	err = s.executeQuery(ctx, query, f)
	if err != nil {
		return nil, err
	}

	query = queryGetTraceEvents(traceIDs...)
	f = func(record map[string]interface{}) error {
		if traceID, spanID, log, err := recordToLog(record); err != nil {
			return err
		} else if trace, found := traces[traceID]; !found {
			s.logger.Warn("trace not found for log")
		} else if span, found := trace[spanID]; !found {
			s.logger.Warn("span not found for log")
		} else {
			span.Logs = append(span.Logs, *log)
		}
		return nil
	}

	err = s.executeQuery(ctx, query, f)
	if err != nil {
		return nil, err
	}

	query = queryGetTraceLinks(traceIDs...)
	f = func(record map[string]interface{}) error {
		if traceID, spanID, spanRef, err := recordToSpanRef(record); err != nil {
			return err
		} else if trace, found := traces[traceID]; !found {
			s.logger.Warn("trace not found for span ref")
		} else if span, found := trace[spanID]; !found {
			s.logger.Warn("span not found for span ref")
		} else {
			span.References = append(span.References, *spanRef)
		}
		return nil
	}

	err = s.executeQuery(ctx, query, f)
	if err != nil {
		return nil, err
	}

	tracesSlice := make([]*model.Trace, 0, len(traces))
	for _, spans := range traces {
		trace := &model.Trace{Spans: make([]*model.Span, 0, len(traces))}
		for _, span := range spans {
			trace.Spans = append(trace.Spans, span)
		}
		tracesSlice = append(tracesSlice, trace)
	}

	return tracesSlice, nil
}

func (s *Store) FindTraceIDs(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	query := queryFindTraceIDs(traceQueryParameters)
	var traceIDs []model.TraceID
	f := func(record map[string]interface{}) error {
		if traceIDString, found := record[attributeTraceID].(string); found {
			traceID, err := model.TraceIDFromString(traceIDString)
			if err != nil {
				return err
			}
			traceIDs = append(traceIDs, traceID)
		}
		return nil
	}

	err := s.executeQuery(ctx, query, f)
	if err != nil {
		return nil, err
	}

	return traceIDs, nil
}

func (s *Store) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	childServiceByParentService := make(map[string]map[string]uint64)
	sourceByService := make(map[string]string)

	f := func(record map[string]interface{}) error {
		var parentService string
		if v, found := record["parent_service"]; !found {
			s.logger.Warn("parent service not found in span dependency")
			return nil
		} else {
			parentService = v.(string)
		}
		var childService string
		if v, found := record["child_service"]; !found {
			s.logger.Warn("child service not found in span dependency")
			return nil
		} else {
			childService = v.(string)
		}
		if parentSource, found := record["parent_source"]; found {
			sourceByService[parentService] = parentSource.(string)
		}
		if childSource, found := record["child_source"]; found {
			sourceByService[childService] = childSource.(string)
		}
		if parent, found := childServiceByParentService[parentService]; found {
			parent[childService]++
		} else {
			childServiceByParentService[parentService] = map[string]uint64{childService: 1}
		}
		return nil
	}

	err := s.executeQuery(ctx, queryGetDependencies, f, endTs.Add(-lookback).UnixNano(), endTs.UnixNano(), endTs.UnixNano())
	if err != nil {
		return nil, err
	}

	var dependencyLinks []model.DependencyLink
	for parentService, child := range childServiceByParentService {
		for childService, callCount := range child {
			dependencyLinks = append(dependencyLinks, model.DependencyLink{
				Parent:    parentService,
				Child:     childService,
				CallCount: callCount,
				Source:    sourceByService[parentService],
			})
		}
	}

	return dependencyLinks, nil
}
