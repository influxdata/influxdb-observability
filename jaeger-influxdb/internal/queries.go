package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/metricsstore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.opentelemetry.io/collector/pdata/ptrace"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"

	"github.com/influxdata/influxdb-observability/common"
)

// timeNow exists to allow for testing; do not change in production code
var timeNow func() time.Time = time.Now

func traceIDToString(traceID model.TraceID) string {
	// model.TraceID.String() does not convert the high portion if it is zero
	return fmt.Sprintf("%016x%016x", traceID.High, traceID.Low)
}

func queryGetAllWhereTraceID(table string, traceIDs ...model.TraceID) string {
	if len(traceIDs) == 0 {
		return fmt.Sprintf(`SELECT * FROM '%s' WHERE false`, table)
	}
	traceIDStrings := make([]string, len(traceIDs))
	for i, traceID := range traceIDs {
		traceIDStrings[i] = traceIDToString(traceID)
	}
	return fmt.Sprintf(`SELECT * FROM '%s' WHERE "%s" IN ('%s')`,
		table, common.AttributeTraceID, strings.Join(traceIDStrings, `','`))
}

func queryGetTraceSpans(tableSpans string, traceIDs ...model.TraceID) string {
	return queryGetAllWhereTraceID(tableSpans, traceIDs...)
}

func queryGetTraceEvents(tableLogs string, traceIDs ...model.TraceID) string {
	return queryGetAllWhereTraceID(tableLogs, traceIDs...)
}

func queryGetTraceLinks(tableSpanLinks string, traceIDs ...model.TraceID) string {
	return queryGetAllWhereTraceID(tableSpanLinks, traceIDs...)
}

func queryGetServices() string {
	return fmt.Sprintf(`SELECT "%s" FROM '%s' GROUP BY "%s"`,
		semconv.AttributeServiceName, tableSpanMetricsCalls, semconv.AttributeServiceName)
}

func queryGetOperations(serviceName string) string {
	return fmt.Sprintf(`SELECT "%s", "%s" FROM '%s' WHERE "%s" = '%s' GROUP BY "%s", "%s"`,
		common.AttributeSpanName, common.AttributeSpanKind, tableSpanMetricsCalls, semconv.AttributeServiceName, serviceName, common.AttributeSpanName, common.AttributeSpanKind)
}

func queryGetDependencies(endTs time.Time, lookback time.Duration) string {
	return fmt.Sprintf(`
SELECT "%s", "%s", SUM("%s") AS "%s"
FROM '%s'
WHERE "%s" >= to_timestamp(%d) AND "%s" <= to_timestamp(%d)
GROUP BY "%s", "%s"`,
		columnServiceGraphClient, columnServiceGraphServer, columnServiceGraphCount, columnServiceGraphCount,
		tableServiceGraphRequestCount,
		common.AttributeTime, endTs.Add(-lookback).UnixNano(), common.AttributeTime, endTs.UnixNano(),
		columnServiceGraphClient, columnServiceGraphServer)
}

func queryFindTraceIDs(tableSpans string, tqp *spanstore.TraceQueryParameters) string {
	tags := make(map[string]string, len(tqp.Tags)+2)
	for k, v := range tqp.Tags {
		tags[k] = v
	}
	if tqp.ServiceName != "" {
		tags[semconv.AttributeServiceName] = tqp.ServiceName
	}
	if tqp.OperationName != "" {
		tags[common.AttributeSpanName] = tqp.OperationName
	}

	predicates := make([]string, 0, len(tags)+4)
	for k, v := range tags {
		predicates = append(predicates, fmt.Sprintf(`"%s" = '%s'`, k, v))
	}
	if !tqp.StartTimeMin.IsZero() {
		predicates = append(predicates, fmt.Sprintf(`"%s" >= to_timestamp(%d)`, common.AttributeTime, tqp.StartTimeMin.UnixNano()))
	}
	if !tqp.StartTimeMax.IsZero() {
		predicates = append(predicates, fmt.Sprintf(`"%s" <= to_timestamp(%d)`, common.AttributeTime, tqp.StartTimeMax.UnixNano()))
	}
	if tqp.DurationMin > 0 {
		predicates = append(predicates,
			fmt.Sprintf(`"%s" >= %d`, common.AttributeDurationNano, tqp.DurationMin.Nanoseconds()))
	}
	if tqp.DurationMax > 0 {
		predicates = append(predicates,
			fmt.Sprintf(`"%s" <= %d`, common.AttributeDurationNano, tqp.DurationMax.Nanoseconds()))
	}

	query := fmt.Sprintf(`SELECT "%s", MAX("%s") AS t FROM '%s'`, common.AttributeTraceID, common.AttributeTime, tableSpans)
	if len(predicates) > 0 {
		query += fmt.Sprintf(" WHERE %s", strings.Join(predicates, " AND "))
	}
	query += fmt.Sprintf(` GROUP BY "%s" ORDER BY t DESC LIMIT %d`, common.AttributeTraceID, tqp.NumTraces)

	return query
}

type spanMetricQueryType int

const (
	spanMetricQueryTypeLatencies spanMetricQueryType = iota
	spanMetricQueryTypeCallRates
	spanMetricQueryTypeErrorRates
)

func querySpanMetrics(tableSpans string, queryType spanMetricQueryType, params metricsstore.BaseQueryParameters, quantile float64) string {
	step := time.Minute
	if params.Step != nil && *params.Step > 0 {
		step = *params.Step
	}

	const (
		columnAliasService    = "service"
		columnAliasOperation  = "operation"
		columnAliasTimeBucket = "t"
		columnAliasValue      = "value"
	)

	resultColumns := []string{
		fmt.Sprintf(`"%s" AS %s`,
			semconv.AttributeServiceName, columnAliasService),
		fmt.Sprintf(`date_bin(INTERVAL '%d nanoseconds', %s) AS %s`,
			step.Nanoseconds(), common.AttributeTime, columnAliasTimeBucket),
	}
	if params.GroupByOperation {
		resultColumns = append(resultColumns,
			fmt.Sprintf(`"%s" AS %s`,
				common.AttributeSpanName, columnAliasOperation))
	} else {
		resultColumns = append(resultColumns,
			fmt.Sprintf(`'' AS %s`,
				columnAliasOperation))
	}
	switch queryType {
	case spanMetricQueryTypeLatencies:
		resultColumns = append(resultColumns,
			// TODO remove coalesce when this is fixed: https://github.com/influxdata/influxdb_iox/issues/7723
			fmt.Sprintf(`coalesce(approx_percentile_cont("%s", %.9f), 0) / %d.0 AS %s`,
				common.AttributeDurationNano, quantile, time.Second, columnAliasValue))
	case spanMetricQueryTypeCallRates:
		resultColumns = append(resultColumns,
			fmt.Sprintf(`count(*) / %.9f AS %s`,
				float64(step)/float64(time.Second), columnAliasValue))
	case spanMetricQueryTypeErrorRates:
		resultColumns = append(resultColumns,
			fmt.Sprintf(`sum(CASE WHEN "%s" = '%s' THEN 1.0 ELSE 0.0 END) / count(*) as %s`,
				semconv.AttributeOtelStatusCode, ptrace.StatusCodeError.String(), columnAliasValue))
	default:
		panic("unknown metric query type")
	}

	groupAndOrderColumns := fmt.Sprintf(`"%s", "%s", "%s"`,
		columnAliasService, columnAliasOperation, columnAliasTimeBucket)

	var predicates []string
	if len(params.ServiceNames) > 0 {
		predicates = append(predicates, fmt.Sprintf(`"%s" IN ('%s')`,
			semconv.AttributeServiceName, strings.Join(params.ServiceNames, "','")))
	}
	endTime := timeNow()
	if params.EndTime != nil {
		endTime = *params.EndTime
	}
	startTime := endTime.Add(-10 * step)
	if params.Lookback != nil {
		startTime = endTime.Add(-*params.Lookback)
	}
	predicates = append(predicates,
		fmt.Sprintf(`"%s" >= to_timestamp(%d) AND "%s" <= to_timestamp(%d)`,
			common.AttributeTime, startTime.UnixNano(), common.AttributeTime, endTime.UnixNano()))

	if len(params.SpanKinds) > 0 {
		ptraceSpanKinds := make([]string, 0, len(params.SpanKinds))
		for _, spanKind := range params.SpanKinds {
			if ptraceSpanKind, ok := spanKindOtelInternalToPtrace[spanKind]; ok {
				ptraceSpanKinds = append(ptraceSpanKinds, ptraceSpanKind)
			}
		}
		predicates = append(predicates, fmt.Sprintf(`"%s" IN ('%s')`,
			common.AttributeSpanKind, strings.Join(ptraceSpanKinds, "','")))
	}

	return fmt.Sprintf(`SELECT %s FROM '%s' WHERE %s GROUP BY %s ORDER BY %s`,
		strings.Join(resultColumns, ", "), tableSpans, strings.Join(predicates, " AND "), groupAndOrderColumns, groupAndOrderColumns)
}

var spanKindOtelInternalToPtrace = map[string]string{
	"SPAN_KIND_INTERNAL": ptrace.SpanKindInternal.String(),
	"SPAN_KIND_SERVER":   ptrace.SpanKindServer.String(),
	"SPAN_KIND_CLIENT":   ptrace.SpanKindClient.String(),
	"SPAN_KIND_PRODUCER": ptrace.SpanKindProducer.String(),
	"SPAN_KIND_CONSUMER": ptrace.SpanKindConsumer.String(),
}
