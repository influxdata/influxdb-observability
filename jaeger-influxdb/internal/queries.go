package internal

import (
	"fmt"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	semconv "go.opentelemetry.io/collector/semconv/v1.12.0"

	"github.com/influxdata/influxdb-observability/common"
)

func traceIDToString(traceID model.TraceID) string {
	// model.TraceID.String() does not convert the high portion if it is zero
	return fmt.Sprintf("%016x%016x", traceID.High, traceID.Low)
}

func queryGetTraceSpans(tableSpans string, traceIDs ...model.TraceID) string {
	predicates := make([]string, len(traceIDs))
	for i, traceID := range traceIDs {
		predicates[i] = fmt.Sprintf(`"%s" = '%s'`, common.AttributeTraceID, traceIDToString(traceID))
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s",
		tableSpans, strings.Join(predicates, " OR "))
}

func queryGetTraceEvents(tableLogs string, traceIDs ...model.TraceID) string {
	predicates := make([]string, len(traceIDs))
	for i, traceID := range traceIDs {
		predicates[i] = fmt.Sprintf(`"%s" = '%s'`, common.AttributeTraceID, traceIDToString(traceID))
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s",
		tableLogs, strings.Join(predicates, " OR "))
}

func queryGetTraceLinks(tableSpanLinks string, traceIDs ...model.TraceID) string {
	predicates := make([]string, len(traceIDs))
	for i, traceID := range traceIDs {
		predicates[i] = fmt.Sprintf(`"%s" = '%s'`, common.AttributeTraceID, traceIDToString(traceID))
	}
	return fmt.Sprintf("SELECT * FROM %s WHERE %s",
		tableSpanLinks, strings.Join(predicates, " OR "))
}

func queryGetServices(tableSpans string) string {
	return fmt.Sprintf(`SELECT "%s" FROM %s GROUP BY "%s"`,
		semconv.AttributeServiceName, tableSpans, semconv.AttributeServiceName)
}

func queryGetOperations(tableSpans, serviceName string) string {
	return fmt.Sprintf(`SELECT "%s", "%s" FROM %s WHERE "%s" = '%s' GROUP BY "%s", "%s"`,
		common.AttributeName, common.AttributeSpanKind, tableSpans, semconv.AttributeServiceName, serviceName, common.AttributeName, common.AttributeSpanKind)
}

func queryGetDependencies(tableDependencyLinks string, endTs time.Time, lookback time.Duration) string {
	// TODO limit time range
	return fmt.Sprintf(`
select parent, child, sum(calls) as calls
from '%s'
group by parent, child
`,
		tableDependencyLinks)
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
		tags[common.AttributeName] = tqp.OperationName
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

	query := fmt.Sprintf(`SELECT "%s", MAX("%s") AS t FROM %s`, common.AttributeTraceID, common.AttributeTime, tableSpans)
	if len(predicates) > 0 {
		query += fmt.Sprintf(" WHERE %s", strings.Join(predicates, " AND "))
	}
	query += fmt.Sprintf(` GROUP BY "%s" ORDER BY t DESC LIMIT %d`, common.AttributeTraceID, tqp.NumTraces)

	return query
}

func archiveTraceDetails(bucketName, tableSource, tableDestination string, traceID model.TraceID) string {
	return fmt.Sprintf(`
from(bucket: "%s")
  |> range(start: -8760h)
  |> filter(fn: (r) => r._measurement == "%s" and r.trace_id == "%s")
  |> map(fn: (r) => ({ r with _measurement: "%s" }))
  |> to(bucket: "%s")
`,
		bucketName,
		tableSource, traceIDToString(traceID),
		tableDestination,
		bucketName)
}
