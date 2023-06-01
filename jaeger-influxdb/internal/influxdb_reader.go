package internal

import (
	"context"
	"database/sql"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"
	"go.uber.org/zap"

	"github.com/influxdata/influxdb-observability/common"
)

var _ spanstore.Reader = (*influxdbReader)(nil)
var _ dependencystore.Reader = (*influxdbDependencyReader)(nil)

type influxdbReader struct {
	logger *zap.Logger

	executeQuery func(ctx context.Context, db *sql.DB, query string, f func(record map[string]interface{}) error) error

	db                                    *sql.DB
	tableSpans, tableLogs, tableSpanLinks string
}

func (ir *influxdbReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	// Get spans
	spansBySpanID := make(map[model.SpanID]*model.Span)

	f := func(record map[string]interface{}) error {
		span, err := recordToSpan(record)
		if err != nil {
			ir.logger.Warn("failed to convert span to Span", zap.Error(err))
		} else {
			spansBySpanID[span.SpanID] = span
		}
		return nil
	}
	err := ir.executeQuery(ctx, ir.db, queryGetTraceSpans(ir.tableSpans, traceID), f)
	switch {
	case err != nil && !isTableNotFound(err): // ignore table not found (schema-on-write)
		return nil, err
	case len(spansBySpanID) == 0:
		return nil, spanstore.ErrTraceNotFound
	}

	// Get events
	f = func(record map[string]interface{}) error {
		if _, spanID, log, err := recordToLog(record); err != nil {
			ir.logger.Warn("failed to convert event to Log", zap.Error(err))
		} else if span, ok := spansBySpanID[spanID]; !ok {
			ir.logger.Warn("span event contains unknown span ID")
		} else {
			span.Logs = append(span.Logs, *log)
		}
		return nil
	}
	err = ir.executeQuery(ctx, ir.db, queryGetTraceEvents(ir.tableLogs, traceID), f)
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}

	// Get links
	f = func(record map[string]interface{}) error {
		_, spanID, spanRef, err := recordToSpanRef(record)
		if err != nil {
			ir.logger.Warn("failed to convert link to SpanRef", zap.Error(err))
		} else if span, found := spansBySpanID[spanID]; !found {
			ir.logger.Warn("link contains unknown span ID")
		} else {
			span.References = append(span.References, *spanRef)
		}
		return nil
	}

	err = ir.executeQuery(ctx, ir.db, queryGetTraceLinks(ir.tableSpanLinks, traceID), f)
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}

	// Assemble trace
	trace := &model.Trace{
		Spans: make([]*model.Span, 0, len(spansBySpanID)),
	}
	for _, span := range spansBySpanID {
		trace.Spans = append(trace.Spans, span)
	}
	return trace, nil
}

func (ir *influxdbReader) GetServices(ctx context.Context) ([]string, error) {
	var services []string
	f := func(record map[string]interface{}) error {
		if v, found := record[semconv.AttributeServiceName]; found && v != nil {
			services = append(services, v.(string))
		}
		return nil
	}

	err := ir.executeQuery(ctx, ir.db, queryGetServices(), f)
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}
	return services, nil
}

func (ir *influxdbReader) GetOperations(ctx context.Context, operationQueryParameters spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	var operations []spanstore.Operation
	f := func(record map[string]interface{}) error {
		if v, found := record[common.AttributeSpanName]; found && v != nil {
			operation := spanstore.Operation{Name: v.(string)}
			if spanKind, found := record[common.AttributeSpanKind]; found && spanKind != nil {
				operation.SpanKind = spanKind.(string)
			}
			operations = append(operations, operation)
		}
		return nil
	}

	err := ir.executeQuery(ctx, ir.db, queryGetOperations(operationQueryParameters.ServiceName), f)
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}
	return operations, nil
}

func (ir *influxdbReader) FindTraces(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	// Get trace IDs
	traceIDs, err := ir.FindTraceIDs(ctx, traceQueryParameters)
	if err != nil || len(traceIDs) == 0 {
		return nil, err
	}

	// Get traces
	spansBySpanIDByTraceID := make(map[model.TraceID]map[model.SpanID]*model.Span)
	f := func(record map[string]interface{}) error {
		if span, err := recordToSpan(record); err != nil {
			return err
		} else if trace, found := spansBySpanIDByTraceID[span.TraceID]; !found {
			spansBySpanIDByTraceID[span.TraceID] = map[model.SpanID]*model.Span{span.SpanID: span}
		} else {
			trace[span.SpanID] = span
		}
		return nil
	}

	err = ir.executeQuery(ctx, ir.db, queryGetTraceSpans(ir.tableSpans, traceIDs...), f)
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}

	// Get events
	f = func(record map[string]interface{}) error {
		if traceID, spanID, log, err := recordToLog(record); err != nil {
			return err
		} else if trace, found := spansBySpanIDByTraceID[traceID]; !found {
			ir.logger.Warn("trace not found for log")
		} else if span, found := trace[spanID]; !found {
			ir.logger.Warn("span not found for log")
		} else {
			span.Logs = append(span.Logs, *log)
		}
		return nil
	}

	err = ir.executeQuery(ctx, ir.db, queryGetTraceEvents(ir.tableLogs, traceIDs...), f)
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}

	// Get links
	f = func(record map[string]interface{}) error {
		if traceID, spanID, spanRef, err := recordToSpanRef(record); err != nil {
			return err
		} else if trace, found := spansBySpanIDByTraceID[traceID]; !found {
			ir.logger.Warn("trace not found for span ref")
		} else if span, found := trace[spanID]; !found {
			ir.logger.Warn("span not found for span ref")
		} else {
			span.References = append(span.References, *spanRef)
		}
		return nil
	}

	err = ir.executeQuery(ctx, ir.db, queryGetTraceLinks(ir.tableSpanLinks, traceIDs...), f)
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}

	traces := make([]*model.Trace, 0, len(spansBySpanIDByTraceID))
	for _, spans := range spansBySpanIDByTraceID {
		trace := &model.Trace{Spans: make([]*model.Span, 0, len(spansBySpanIDByTraceID))}
		for _, span := range spans {
			trace.Spans = append(trace.Spans, span)
		}
		traces = append(traces, trace)
	}

	return traces, nil
}

func (ir *influxdbReader) FindTraceIDs(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]model.TraceID, error) {
	var traceIDs []model.TraceID
	f := func(record map[string]interface{}) error {
		if v, found := record[common.AttributeTraceID]; found && v != nil {
			traceID, err := model.TraceIDFromString(v.(string))
			if err != nil {
				return err
			}
			traceIDs = append(traceIDs, traceID)
		}
		return nil
	}

	err := ir.executeQuery(ctx, ir.db, queryFindTraceIDs(ir.tableSpans, traceQueryParameters), f)
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}
	return traceIDs, nil
}

type influxdbDependencyReader struct {
	logger *zap.Logger
	ir     *influxdbReader
}

func (idr *influxdbDependencyReader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	var dependencyLinks []model.DependencyLink

	f := func(record map[string]interface{}) error {
		var parentService string
		if v, found := record[columnServiceGraphClient]; !found || v == nil {
			idr.logger.Warn("parent service not found in dependency link")
			return nil
		} else {
			parentService = v.(string)
		}
		var childService string
		if v, found := record[columnServiceGraphServer]; !found || v == nil {
			idr.logger.Warn("child service not found in dependency link")
			return nil
		} else {
			childService = v.(string)
		}
		var calls int64
		if v, found := record[columnServiceGraphCount]; !found || v == nil {
			idr.logger.Warn("calls not found in dependency link")
			return nil
		} else {
			calls = v.(int64)
		}

		dependencyLinks = append(dependencyLinks, model.DependencyLink{
			Parent:    parentService,
			Child:     childService,
			CallCount: uint64(calls),
		})

		return nil
	}

	err := idr.ir.executeQuery(ctx, idr.ir.db, queryGetDependencies(endTs, lookback), f)
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}
	return dependencyLinks, nil
}
