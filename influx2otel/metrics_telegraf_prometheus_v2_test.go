package influx2otel_test

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/influx2otel"
)

func TestAddPoint_v2_gauge(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"engine_id":            "0",
		},
		map[string]interface{}{
			"cache_age_seconds": float64(23.9),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeGauge)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"engine_id":            "1",
		},
		map[string]interface{}{
			"cache_age_seconds": float64(11.9),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeGauge)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("cache_age_seconds")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("engine_id", "0")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(23.9)
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("engine_id", "1")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(11.9)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v2_untypedGauge(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"engine_id":            "0",
		},
		map[string]interface{}{
			"cache_age_seconds": float64(23.9),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"engine_id":            "1",
		},
		map[string]interface{}{
			"cache_age_seconds": float64(11.9),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("cache_age_seconds")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("engine_id", "0")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(23.9)
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("engine_id", "1")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(11.9)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v2_sum(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"http_requests_total": float64(1027),
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeSum)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "400",
		},
		map[string]interface{}{
			"http_requests_total": float64(3),
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeSum)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetEmptySum()
	m.Sum().SetIsMonotonic(true)
	m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(1027)
	dp = m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "400")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(3)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v2_untypedSum(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"http_requests_total": float64(1027),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "400",
		},
		map[string]interface{}{
			"http_requests_total": float64(3),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(1027)
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "400")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(3)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v2_histogram(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"http_request_duration_seconds_count": float64(144320),
			"http_request_duration_seconds_sum":   float64(53423),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.05",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(24054),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.1",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(33444),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.2",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(100392),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.5",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(129389),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "1",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(133988),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "+Inf",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(144320),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_request_duration_seconds")
	m.SetEmptyHistogram()
	m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetCount(144320)
	dp.SetSum(53423)
	dp.BucketCounts().FromRaw([]uint64{24054, 9390, 66948, 28997, 4599, 10332})
	dp.ExplicitBounds().FromRaw([]float64{0.05, 0.1, 0.2, 0.5, 1})

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v2_histogram_missingInfinityBucket(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"http_request_duration_seconds_count": float64(144320),
			"http_request_duration_seconds_sum":   float64(53423),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.05",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(24054),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.1",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(33444),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.2",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(100392),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.5",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(129389),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "1",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(133988),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_request_duration_seconds")
	m.SetEmptyHistogram()
	m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetCount(144320)
	dp.SetSum(53423)
	dp.BucketCounts().FromRaw([]uint64{24054, 9390, 66948, 28997, 4599, 10332})
	dp.ExplicitBounds().FromRaw([]float64{0.05, 0.1, 0.2, 0.5, 1})

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v2_untypedHistogram(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"http_request_duration_seconds_count": float64(144320),
			"http_request_duration_seconds_sum":   float64(53423),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.05",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(24054),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.1",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(33444),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.2",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(100392),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "0.5",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(129389),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"le":                   "1",
		},
		map[string]interface{}{
			"http_request_duration_seconds_bucket": float64(133988),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_request_duration_seconds")
	m.SetEmptyHistogram()
	m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetCount(144320)
	dp.SetSum(53423)
	dp.BucketCounts().FromRaw([]uint64{24054, 9390, 66948, 28997, 4599, 10332})
	dp.ExplicitBounds().FromRaw([]float64{0.05, 0.1, 0.2, 0.5, 1})

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v2_summary(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"rpc_duration_seconds_count": float64(2693),
			"rpc_duration_seconds_sum":   float64(17560473),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeSummary)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"quantile":             "0.01",
		},
		map[string]interface{}{
			"rpc_duration_seconds": float64(3102),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeSummary)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"quantile":             "0.05",
		},
		map[string]interface{}{
			"rpc_duration_seconds": float64(3272),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeSummary)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"quantile":             "0.5",
		},
		map[string]interface{}{
			"rpc_duration_seconds": float64(4773),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeSummary)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"quantile":             "0.9",
		},
		map[string]interface{}{
			"rpc_duration_seconds": float64(9001),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeSummary)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"quantile":             "0.99",
		},
		map[string]interface{}{
			"rpc_duration_seconds": float64(76656),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeSummary)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("rpc_duration_seconds")
	m.SetEmptySummary()
	dp := m.Summary().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetCount(2693)
	dp.SetSum(17560473)
	qv := dp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.01)
	qv.SetValue(3102)
	qv = dp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.05)
	qv.SetValue(3272)
	qv = dp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.5)
	qv.SetValue(4773)
	qv = dp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.9)
	qv.SetValue(9001)
	qv = dp.QuantileValues().AppendEmpty()
	qv.SetQuantile(0.99)
	qv.SetValue(76656)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v2_untypedSummary(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"rpc_duration_seconds_count": float64(2693),
			"rpc_duration_seconds_sum":   float64(17560473),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"quantile":             "0.01",
		},
		map[string]interface{}{
			"rpc_duration_seconds": float64(3102),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"quantile":             "0.05",
		},
		map[string]interface{}{
			"rpc_duration_seconds": float64(3272),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"quantile":             "0.5",
		},
		map[string]interface{}{
			"rpc_duration_seconds": float64(4773),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"quantile":             "0.9",
		},
		map[string]interface{}{
			"rpc_duration_seconds": float64(9001),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint(common.MeasurementPrometheus,
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
			"quantile":             "0.99",
		},
		map[string]interface{}{
			"rpc_duration_seconds": float64(76656),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("rpc_duration_seconds")
	m.SetEmptyHistogram()
	m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetCount(2693)
	dp.SetSum(17560473)
	dp.BucketCounts().FromRaw([]uint64{3102, 3272, 4773, 9001, 76656, 2693})
	dp.ExplicitBounds().FromRaw([]float64{0.01, 0.05, 0.5, 0.9, 0.99})

	assertMetricsEqual(t, expect, b.GetMetrics())
}
