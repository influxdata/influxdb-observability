package internal

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.uber.org/zap"
)

var _ spanstore.Reader = (*influxdbReader)(nil)
var _ dependencystore.Reader = (*influxdbReader)(nil)

type influxdbReader struct {
	logger                                *zap.Logger
	db                                    *sql.DB
	tableSpans, tableLogs, tableSpanLinks string
}

func (ir *influxdbReader) GetTrace(ctx context.Context, traceID model.TraceID) (*model.Trace, error) {
	// Get spans
	spans := make(map[model.SpanID]*model.Span)

	f := func(record map[string]interface{}) error {
		span, err := recordToSpan(record)
		if err != nil {
			ir.logger.Warn("failed to convert span to Span", zap.Error(err))
		} else {
			spans[span.SpanID] = span
		}
		return nil
	}
	err := executeQuery(ctx, ir.db, queryGetTraceSpans(ir.tableSpans, traceID), f)
	if err != nil {
		return nil, err
	}

	// Get events
	f = func(record map[string]interface{}) error {
		_, spanID, log, err := recordToLog(record)
		if err != nil {
			ir.logger.Warn("failed to convert event to Log", zap.Error(err))
		} else if span, ok := spans[spanID]; !ok {
			ir.logger.Warn("span event contains unknown span ID")
		} else {
			// TODO filter span attributes duplicated in logs
			span.Logs = append(span.Logs, *log)
		}
		return nil
	}
	err = executeQuery(ctx, ir.db, queryGetTraceEvents(ir.tableLogs, traceID), f)
	if err != nil {
		return nil, err
	}

	// Get links
	f = func(record map[string]interface{}) error {
		_, spanID, spanRef, err := recordToSpanRef(record)
		if err != nil {
			ir.logger.Warn("failed to convert link to SpanRef", zap.Error(err))
		} else if span, found := spans[spanID]; !found {
			ir.logger.Warn("link contains unknown span ID")
		} else {
			span.References = append(span.References, *spanRef)
		}
		return nil
	}

	err = executeQuery(ctx, ir.db, queryGetTraceLinks(ir.tableSpanLinks, traceID), f)
	if err != nil {
		return nil, err
	}

	// Assemble trace
	trace := &model.Trace{
		Spans: make([]*model.Span, len(spans)),
	}
	for i, span := range spans {
		trace.Spans[i] = span
	}
	return trace, nil
}

func (ir *influxdbReader) GetServices(ctx context.Context) ([]string, error) {
	ir.logger.Info("GetServices")
	var services []string
	f := func(record map[string]interface{}) error {
		fmt.Printf("%+v\n", record)
		if serviceName, found := record[attributeServiceName]; found {
			services = append(services, serviceName.(string))
		}
		return nil
	}

	err := executeQuery(ctx, ir.db, queryGetServices(ir.tableSpans), f)
	if err != nil {
		return nil, err
	}
	println(strings.Join(services, "|"))
	return services, nil
}

func (ir *influxdbReader) GetOperations(ctx context.Context, operationQueryParameters spanstore.OperationQueryParameters) ([]spanstore.Operation, error) {
	var operations []spanstore.Operation
	f := func(record map[string]interface{}) error {
		if operationName, found := record[attributeName]; found {
			operation := spanstore.Operation{Name: operationName.(string)}
			if spanKind, found := record[attributeSpanKind]; found {
				operation.SpanKind = spanKind.(string)
			}
			operations = append(operations, operation)
		}
		return nil
	}

	err := executeQuery(ctx, ir.db, queryGetOperations(ir.tableSpans, operationQueryParameters.ServiceName), f)
	if err != nil {
		return nil, err
	}
	return operations, nil
}

func (ir *influxdbReader) FindTraces(ctx context.Context, traceQueryParameters *spanstore.TraceQueryParameters) ([]*model.Trace, error) {
	// Get trace IDs
	traceIDs, err := ir.FindTraceIDs(ctx, traceQueryParameters)
	if err != nil {
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

	err = executeQuery(ctx, ir.db, queryGetTraceSpans(ir.tableSpans, traceIDs...), f)
	if err != nil {
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

	err = executeQuery(ctx, ir.db, queryGetTraceEvents(ir.tableLogs, traceIDs...), f)
	if err != nil {
		ir.logger.Warn("while querying span events", zap.Error(err))
		//return nil, err
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

	err = executeQuery(ctx, ir.db, queryGetTraceLinks(ir.tableSpanLinks, traceIDs...), f)
	if err != nil {
		ir.logger.Warn("while querying span links", zap.Error(err))
		//return nil, err
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
		if traceIDString, found := record[attributeTraceID].(string); found {
			traceID, err := model.TraceIDFromString(traceIDString)
			if err != nil {
				return err
			}
			traceIDs = append(traceIDs, traceID)
		}
		return nil
	}

	err := executeQuery(ctx, ir.db, queryFindTraceIDs(ir.tableSpans, traceQueryParameters), f)
	if err != nil {
		return nil, err
	}
	return traceIDs, nil
}

func (ir *influxdbReader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	childServiceByParentService := make(map[string]map[string]uint64)
	sourceByService := make(map[string]string)

	f := func(record map[string]interface{}) error {
		var parentService string
		if v, found := record["parent_service"]; !found {
			ir.logger.Warn("parent service not found in span dependency")
			return nil
		} else {
			parentService = v.(string)
		}
		var childService string
		if v, found := record["child_service"]; !found {
			ir.logger.Warn("child service not found in span dependency")
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

	err := executeQuery(ctx, ir.db, queryGetDependencies(ir.tableSpans, endTs, lookback), f)
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
