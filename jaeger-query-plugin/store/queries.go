package store

import (
	"fmt"
	"strings"

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

func queryGetTraceSpans(traceIDs ...model.TraceID) string {
	return fmt.Sprintf("SELECT * FROM %s WHERE %s IN ('%s')",
		measurementSpans, attributeTraceID, strings.Join(traceIDsToStrings(traceIDs), "','"))
}

func queryGetTraceEvents(traceIDs ...model.TraceID) string {
	return fmt.Sprintf("SELECT * FROM %s WHERE %s IN ('%s')",
		measurementLogs, attributeTraceID, strings.Join(traceIDsToStrings(traceIDs), "','"))
}

func queryGetTraceLinks(traceIDs ...model.TraceID) string {
	return fmt.Sprintf("SELECT * FROM %s WHERE %s IN ('%s')",
		measurementSpanLinks, attributeTraceID, strings.Join(traceIDsToStrings(traceIDs), "','"))
}

var queryGetServices = fmt.Sprintf(`SELECT "%s" FROM %s GROUP BY "%s"`, attributeServiceName, measurementSpans, attributeServiceName)

var queryGetOperations = fmt.Sprintf(`SELECT "%s" FROM %s WHERE "%s" = '%%s' GROUP BY "%s"`, attributeName, measurementSpans, attributeServiceName, attributeName)

// TODO this query fails due to a DataFusion bug
// https://issues.apache.org/jira/browse/ARROW-11522
// https://issues.apache.org/jira/browse/ARROW-11523
var queryGetDependencies = fmt.Sprintf(`
SELECT parent."%s" AS parent_service, parent."%s" AS parent_source, child."%s" AS child_service, child."%s" AS child_source
FROM %s AS parent JOIN %s AS child ON (parent."%s" = child."%s")
WHERE %s >= %%d AND %s <= %%d AND %s <= %%d`,
	attributeServiceName, attributeServiceName, attributeTelemetrySDKName, attributeTelemetrySDKName,
	measurementSpans, measurementSpans, attributeSpanID, attributeParentSpanID,
	attributeTime, attributeTime, attributeEndTimeUnixNano)

func queryFindTraceIDs(tqp *spanstore.TraceQueryParameters) string {
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
		predicates = append(predicates, fmt.Sprintf(`"%s" >= %d`, attributeTime, tqp.StartTimeMin.UnixNano()))
	}
	if !tqp.StartTimeMax.IsZero() {
		predicates = append(predicates, fmt.Sprintf(`"%s" <= %d`, attributeTime, tqp.StartTimeMax.UnixNano()))
	}
	if tqp.DurationMin > 0 {
		predicates = append(predicates,
			fmt.Sprintf(`"%s" >= %d`, attributeDurationNano, tqp.DurationMin.Nanoseconds()))
	}
	if tqp.DurationMax > 0 {
		predicates = append(predicates,
			fmt.Sprintf(`"%s" <= %d`, attributeDurationNano, tqp.DurationMax.Nanoseconds()))
	}

	query := fmt.Sprintf(`SELECT "%s", MAX("%s") AS t FROM %s`, attributeTraceID, attributeTime, measurementSpans)
	if len(predicates) > 0 {
		query += fmt.Sprintf(" WHERE %s", strings.Join(predicates, " AND "))
	}
	query += fmt.Sprintf(` GROUP BY "%s" ORDER BY t DESC LIMIT %d`, attributeTraceID, tqp.NumTraces)

	return query
}
