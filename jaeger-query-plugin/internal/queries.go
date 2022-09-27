package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

func traceIDToString(traceID model.TraceID) string {
	// model.TraceID.String() does not convert the high portion if it is zero
	return fmt.Sprintf("%016x%016x", traceID.High, traceID.Low)
}

func traceIDsToStrings(traceIDs []model.TraceID) []string {
	traceIDStrings := make([]string, len(traceIDs))
	for i, traceID := range traceIDs {
		traceIDStrings[i] = traceIDToString(traceID)
	}
	return traceIDStrings
}

func queryGetTraceSpans(tableSpans string, traceIDs ...model.TraceID) string {
	predicates := make([]string, len(traceIDs))
	for i, traceID := range traceIDs {
		predicates[i] = fmt.Sprintf(`"%s" = '%s'`, attributeTraceID, traceID.String())
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s",
		tableSpans, strings.Join(predicates, " OR "))
}

func queryGetTraceEvents(tableLogs string, traceIDs ...model.TraceID) string {
	predicates := make([]string, len(traceIDs))
	for i, traceID := range traceIDs {
		predicates[i] = fmt.Sprintf(`"%s" = '%s'`, attributeTraceID, traceID.String())
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s",
		tableLogs, strings.Join(predicates, " OR "))
}

func queryGetTraceLinks(tableSpanLinks string, traceIDs ...model.TraceID) string {
	predicates := make([]string, len(traceIDs))
	for i, traceID := range traceIDs {
		predicates[i] = fmt.Sprintf(`"%s" = '%s'`, attributeTraceID, traceID.String())
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s",
		tableSpanLinks, strings.Join(predicates, " OR "))
}

func queryGetServices(tableSpans string) string {
	return fmt.Sprintf(`SELECT "%s" FROM %s GROUP BY "%s"`,
		attributeServiceName, tableSpans, attributeServiceName)
}

func queryGetOperations(tableSpans, serviceName string) string {
	return fmt.Sprintf(`SELECT "%s", "%s" FROM %s WHERE "%s" = '%s' GROUP BY "%s", "%s"`,
		attributeName, attributeSpanKind, tableSpans, attributeServiceName, serviceName, attributeName, attributeSpanKind)
}

func queryGetDependencies(tableSpans string, endTs time.Time, lookback time.Duration) string {
	return fmt.Sprintf(`
SELECT parent."%s" AS parent_service, parent."%s" AS parent_source, child."%s" AS child_service, child."%s" AS child_source
FROM %s AS parent JOIN %s AS child ON (parent."%s" = child."%s")
WHERE %s >= %d AND %s <= %d AND %s <= %d`,
		attributeServiceName, attributeServiceName, attributeTelemetrySDKName, attributeTelemetrySDKName,
		tableSpans, tableSpans, attributeSpanID, attributeParentSpanID,
		attributeTime, endTs.Add(-lookback).UnixNano(), attributeTime, endTs.UnixNano(), attributeEndTimeUnixNano, endTs.UnixNano())
}

func queryFindTraceIDs(tableSpans string, tqp *spanstore.TraceQueryParameters) string {
	tags := make(map[string]string, len(tqp.Tags)+2)
	for k, v := range tqp.Tags {
		tags[k] = v
	}
	if tqp.ServiceName != "" {
		tags[attributeServiceName] = tqp.ServiceName
	}
	if tqp.OperationName != "" {
		tags[attributeName] = tqp.OperationName
	}

	predicates := make([]string, 0, len(tags)+4)
	for k, v := range tags {
		predicates = append(predicates, fmt.Sprintf(`"%s" = '%s'`, k, v))
	}
	if !tqp.StartTimeMin.IsZero() {
		predicates = append(predicates, fmt.Sprintf(`"%s" >= to_timestamp(%d)`, attributeTime, tqp.StartTimeMin.UnixNano()))
	}
	if !tqp.StartTimeMax.IsZero() {
		predicates = append(predicates, fmt.Sprintf(`"%s" <= to_timestamp(%d)`, attributeTime, tqp.StartTimeMax.UnixNano()))
	}
	if tqp.DurationMin > 0 {
		predicates = append(predicates,
			fmt.Sprintf(`"%s" >= %d`, attributeDurationNano, tqp.DurationMin.Nanoseconds()))
	}
	if tqp.DurationMax > 0 {
		predicates = append(predicates,
			fmt.Sprintf(`"%s" <= %d`, attributeDurationNano, tqp.DurationMax.Nanoseconds()))
	}

	query := fmt.Sprintf(`SELECT "%s", MAX("%s") AS t FROM %s`, attributeTraceID, attributeTime, tableSpans)
	if len(predicates) > 0 {
		query += fmt.Sprintf(" WHERE %s", strings.Join(predicates, " AND "))
	}
	query += fmt.Sprintf(` GROUP BY "%s" ORDER BY t DESC LIMIT %d`, attributeTraceID, tqp.NumTraces)

	return query
}
