package store

import (
	"fmt"
	"strings"
	"time"

	"github.com/jaegertracing/jaeger/storage/spanstore"
)

// SQLTraceQuery abstracts an SQL query for spans
type SQLTraceQuery struct {
	startTimeMin, startTimeMax time.Time
	durationMin, durationMax   time.Duration
	tags                       map[string]string
	numTraces                  int
}

// SQLTraceQueryFromTQP constructs an SQLTraceQuery using parameters in a spanstore.TraceQueryParameters
func SQLTraceQueryFromTQP(query *spanstore.TraceQueryParameters) *SQLTraceQuery {
	q := &SQLTraceQuery{tags: map[string]string{}}

	if query.ServiceName != "" {
		q.ServiceName(query.ServiceName)
	}
	if query.OperationName != "" {
		q.OperationName(query.OperationName)
	}
	if !query.StartTimeMin.IsZero() {
		q.StartTimeMin(query.StartTimeMin)
	}
	if !query.StartTimeMax.IsZero() {
		q.StartTimeMax(query.StartTimeMax)
	}
	for k, v := range query.Tags {
		q.Tag(k, v)
	}
	if query.DurationMin > 0 {
		q.DurationMin(query.DurationMin)
	}
	if query.DurationMax > 0 {
		q.DurationMax(query.DurationMax)
	}
	if query.NumTraces > 0 {
		q.NumTraces(query.NumTraces)
	}

	return q
}

// ServiceName sets the query service name.
func (q *SQLTraceQuery) ServiceName(serviceName string) *SQLTraceQuery {
	q.tags[attributeServiceName] = serviceName
	return q
}

// OperationName sets the query operation name.
func (q *SQLTraceQuery) OperationName(operationName string) *SQLTraceQuery {
	q.tags[attributeName] = operationName
	return q
}

// Tag adds a query tag key:value pair.
func (q *SQLTraceQuery) Tag(k, v string) *SQLTraceQuery {
	q.tags[k] = v
	return q
}

// StartTimeMin sets the min start time to query.
func (q *SQLTraceQuery) StartTimeMin(startTimeMin time.Time) *SQLTraceQuery {
	q.startTimeMin = startTimeMin
	return q
}

// StartTimeMax sets the max start time to query.
func (q *SQLTraceQuery) StartTimeMax(startTimeMax time.Time) *SQLTraceQuery {
	q.startTimeMax = startTimeMax
	return q
}

// DurationMax sets the query max duration threshold.
func (q *SQLTraceQuery) DurationMax(durationMax time.Duration) *SQLTraceQuery {
	q.durationMax = durationMax
	return q
}

// DurationMin sets the query min duration threshold.
func (q *SQLTraceQuery) DurationMin(durationMin time.Duration) *SQLTraceQuery {
	q.durationMin = durationMin
	return q
}

// NumTraces sets the query max traces threshold.
func (q *SQLTraceQuery) NumTraces(numTraces int) *SQLTraceQuery {
	q.numTraces = numTraces
	return q
}

// BuildTraceIDQuery builds an SQL query that returns Trace IDs.
func (q *SQLTraceQuery) BuildTraceIDQuery() string {
	var innerBuilder []string
	innerBuilder = append(innerBuilder, fmt.Sprintf(`SELECT "%s", MIN("%s") FROM %s`, attributeTraceID, attributeTime, measurementSpans))

	var predicates []string
	for k, v := range q.tags {
		predicates = append(predicates, fmt.Sprintf(`"%s" = '%s'`, k, v))
	}
	if q.durationMin > 0 {
		predicates = append(predicates,
			fmt.Sprintf(`"%s" >= %d`, attributeDurationNano, q.durationMin.Nanoseconds()))
	}
	if q.durationMax > 0 {
		predicates = append(predicates,
			fmt.Sprintf(`"%s" <= %d`, attributeDurationNano, q.durationMax.Nanoseconds()))
	}
	if len(predicates) > 0 {
		innerBuilder = append(innerBuilder, fmt.Sprintf("WHERE %s", strings.Join(predicates, " AND ")))
	}

	innerBuilder = append(innerBuilder, fmt.Sprintf(`GROUP BY "%s" LIMIT 1`, attributeTraceID))

	innerQuery := strings.Join(innerBuilder, " ")

	var outerBuilder []string
	outerBuilder = append(outerBuilder, fmt.Sprintf(`SELECT "%s" FROM (%s)`, attributeTraceID, innerQuery))

	predicates = nil

	if !q.startTimeMin.IsZero() {
		predicates = append(predicates, fmt.Sprintf(`"%s" >= %d`, attributeTime, q.startTimeMin.UnixNano()))
	}
	if !q.startTimeMax.IsZero() {
		predicates = append(predicates, fmt.Sprintf(`"%s" <= %d`, attributeTime, q.startTimeMax.UnixNano()))
	}
	if len(predicates) > 0 {
		outerBuilder = append(outerBuilder, fmt.Sprintf("WHERE %s", strings.Join(predicates, " AND ")))
	}

	outerBuilder = append(outerBuilder, fmt.Sprintf(`ORDER BY "%s" DESC`, attributeTime))

	if q.numTraces > 0 {
		outerBuilder = append(outerBuilder, fmt.Sprintf(`LIMIT %d`, q.numTraces))
	}

	return strings.Join(outerBuilder, " ")
}
