package internal

import (
	"fmt"
	"testing"
	"time"

	"github.com/jaegertracing/jaeger/storage/metricsstore"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var tz *time.Location

func init() {
	var err error
	tz, err = time.LoadLocation("US/Pacific")
	if err != nil {
		panic(err)
	}
	timeNow = func() time.Time {
		return time.Date(2023, time.May, 1, 8, 35, 23, 0, tz)
	}
}

func TestQueryGetCallRates(t *testing.T) {
	for i, test := range []struct {
		ServiceNames     []string
		GroupByOperation bool
		EndTime          *time.Time
		Lookback         *time.Duration
		Step             *time.Duration
		RatePer          *time.Duration
		SpanKinds        []string

		expectedQuery string
	}{
		{
			ServiceNames:     nil,
			GroupByOperation: false,
			EndTime:          nil,
			Lookback:         nil,
			Step:             nil,
			RatePer:          nil,
			SpanKinds:        nil,

			expectedQuery: `SELECT "service.name", count(*) / 1 AS rate, date_bin(INTERVAL '60000000000 nanoseconds', time) AS time FROM 'spans' WHERE "time" >= to_timestamp(1682954723000000000) AND "time" <= to_timestamp(1682955323000000000) GROUP BY "service.name", "time"`,
		},
		{
			ServiceNames:     []string{"service-a", "service-b"},
			GroupByOperation: true,
			EndTime:          func() *time.Time { t := time.Date(2023, time.May, 1, 8, 35, 0, 0, tz); return &t }(),
			Lookback:         func() *time.Duration { d := time.Hour; return &d }(),
			Step:             func() *time.Duration { d := 15 * time.Minute; return &d }(),
			RatePer:          func() *time.Duration { d := time.Minute; return &d }(),
			SpanKinds:        []string{ptrace.SpanKindProducer.String(), ptrace.SpanKindServer.String()},

			expectedQuery: `SELECT "service.name", count(*) / 15 AS rate, date_bin(INTERVAL '900000000000 nanoseconds', time) AS time, "name" FROM 'spans' WHERE "service.name" IN ('service-a','service-b') AND "time" >= to_timestamp(1682951700000000000) AND "time" <= to_timestamp(1682955300000000000) AND "kind" IN ('Producer','Server') GROUP BY "service.name", "time", "name"`,
		},
	} {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			params := metricsstore.BaseQueryParameters{
				ServiceNames:     test.ServiceNames,
				GroupByOperation: test.GroupByOperation,
				EndTime:          test.EndTime,
				Lookback:         test.Lookback,
				Step:             test.Step,
				RatePer:          test.RatePer,
				SpanKinds:        test.SpanKinds,
			}
			actualQuery := querySpanMetrics(tableSpans, spanMetricQueryTypeCallRates, params, 0)
			assert.Equal(t, test.expectedQuery, actualQuery)
		})
	}
}
