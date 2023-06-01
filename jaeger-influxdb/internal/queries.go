package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"

	"github.com/influxdata/influxdb-observability/common"
)

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
