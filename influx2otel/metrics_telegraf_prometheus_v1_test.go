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

func TestAddPoint_v1_gauge(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("cache_age_seconds",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"engine_id":            "0",
		},
		map[string]interface{}{
			"gauge": float64(23.9),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeGauge)
	require.NoError(t, err)

	err = b.AddPoint("cache_age_seconds",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"engine_id":            "1",
		},
		map[string]interface{}{
			"gauge": float64(11.9),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeGauge)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	isMetrics := rm.ScopeMetrics().AppendEmpty()
	isMetrics.Scope().SetName("My Library")
	isMetrics.Scope().SetVersion("latest")
	m := isMetrics.Metrics().AppendEmpty()
	m.SetName("cache_age_seconds")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("engine_id", "0")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetDoubleValue(23.9)
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("engine_id", "1")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetDoubleValue(11.9)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v1_untypedGauge(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("cache_age_seconds",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"engine_id":            "0",
		},
		map[string]interface{}{
			"gauge": float64(23.9),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint("cache_age_seconds",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"engine_id":            "1",
		},
		map[string]interface{}{
			"gauge": float64(11.9),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	isMetrics := rm.ScopeMetrics().AppendEmpty()
	isMetrics.Scope().SetName("My Library")
	isMetrics.Scope().SetVersion("latest")
	m := isMetrics.Metrics().AppendEmpty()
	m.SetName("cache_age_seconds")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("engine_id", "0")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetDoubleValue(23.9)
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("engine_id", "1")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetDoubleValue(11.9)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v1_untyped(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("some_custom_metric",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
		},
		map[string]any{
			"count":          int64(1),
			"something_else": float64(2.3),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	isMetrics := rm.ScopeMetrics().AppendEmpty()
	isMetrics.Scope().SetName("My Library")
	isMetrics.Scope().SetVersion("latest")
	m := isMetrics.Metrics().AppendEmpty()
	m.SetName("some_custom_metric_count")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetIntValue(1)
	m = isMetrics.Metrics().AppendEmpty()
	m.SetName("some_custom_metric_something_else")
	m.SetEmptyGauge()
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetDoubleValue(2.3)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v1_sum(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("http_requests_total",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"counter": float64(1027),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeSum)
	require.NoError(t, err)

	err = b.AddPoint("http_requests_total",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "400",
		},
		map[string]interface{}{
			"counter": float64(3),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeSum)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	isMetrics := rm.ScopeMetrics().AppendEmpty()
	isMetrics.Scope().SetName("My Library")
	isMetrics.Scope().SetVersion("latest")
	m := isMetrics.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetEmptySum()
	m.Sum().SetIsMonotonic(true)
	m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetDoubleValue(1027)
	dp = m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "400")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetDoubleValue(3)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v1_untypedSum(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("http_requests_total",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"counter": float64(1027),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	err = b.AddPoint("http_requests_total",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "400",
		},
		map[string]interface{}{
			"counter": float64(3),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	isMetrics := rm.ScopeMetrics().AppendEmpty()
	isMetrics.Scope().SetName("My Library")
	isMetrics.Scope().SetVersion("latest")
	m := isMetrics.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetEmptySum()
	m.Sum().SetIsMonotonic(true)
	m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetDoubleValue(1027)
	dp = m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "400")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetDoubleValue(3)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v1_histogram(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("http_request_duration_seconds",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"count": float64(144320),
			"sum":   float64(53423),
			"0.05":  float64(24054),
			"0.1":   float64(33444),
			"0.2":   float64(100392),
			"0.5":   float64(129389),
			"1":     float64(133988),
			"+Inf":  float64(144320),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	isMetrics := rm.ScopeMetrics().AppendEmpty()
	isMetrics.Scope().SetName("My Library")
	isMetrics.Scope().SetVersion("latest")
	m := isMetrics.Metrics().AppendEmpty()
	m.SetName("http_request_duration_seconds")
	m.SetEmptyHistogram()
	m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetCount(144320)
	dp.SetSum(53423)
	dp.BucketCounts().FromRaw([]uint64{24054, 9390, 66948, 28997, 4599, 10332})
	dp.ExplicitBounds().FromRaw([]float64{0.05, 0.1, 0.2, 0.5, 1})

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v1_histogram_missingInfinityBucket(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("http_request_duration_seconds",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"count": float64(144320),
			"sum":   float64(53423),
			"0.05":  float64(24054),
			"0.1":   float64(33444),
			"0.2":   float64(100392),
			"0.5":   float64(129389),
			"1":     float64(133988),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	isMetrics := rm.ScopeMetrics().AppendEmpty()
	isMetrics.Scope().SetName("My Library")
	isMetrics.Scope().SetVersion("latest")
	m := isMetrics.Metrics().AppendEmpty()
	m.SetName("http_request_duration_seconds")
	m.SetEmptyHistogram()
	m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetCount(144320)
	dp.SetSum(53423)
	dp.BucketCounts().FromRaw([]uint64{24054, 9390, 66948, 28997, 4599, 10332})
	dp.ExplicitBounds().FromRaw([]float64{0.05, 0.1, 0.2, 0.5, 1})

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v1_untypedHistogram(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("http_request_duration_seconds",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"count": float64(144320),
			"sum":   float64(53423),
			"0.05":  float64(24054),
			"0.1":   float64(33444),
			"0.2":   float64(100392),
			"0.5":   float64(129389),
			"1":     float64(133988),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	isMetrics := rm.ScopeMetrics().AppendEmpty()
	isMetrics.Scope().SetName("My Library")
	isMetrics.Scope().SetVersion("latest")
	m := isMetrics.Metrics().AppendEmpty()
	m.SetName("http_request_duration_seconds")
	m.SetEmptyHistogram()
	m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetCount(144320)
	dp.SetSum(53423)
	dp.BucketCounts().FromRaw([]uint64{24054, 9390, 66948, 28997, 4599, 10332})
	dp.ExplicitBounds().FromRaw([]float64{0.05, 0.1, 0.2, 0.5, 1})

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestAddPoint_v1_summary(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("rpc_duration_seconds",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"count": float64(2693),
			"sum":   float64(17560473),
			"0.01":  float64(3102),
			"0.05":  float64(3272),
			"0.5":   float64(4773),
			"0.9":   float64(9001),
			"0.99":  float64(76656),
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeSummary)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	isMetrics := rm.ScopeMetrics().AppendEmpty()
	isMetrics.Scope().SetName("My Library")
	isMetrics.Scope().SetVersion("latest")
	m := isMetrics.Metrics().AppendEmpty()
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

func TestAddPoint_v1_untypedSummary(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("rpc_duration_seconds",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"method":               "post",
			"code":                 "200",
		},
		map[string]interface{}{
			"count": float64(2693),
			"sum":   float64(17560473),
			"0.01":  float64(3102),
			"0.05":  float64(3272),
			"0.5":   float64(4773),
			"0.9":   float64(9001),
			"0.99":  float64(76656),
		},
		time.Unix(0, 1395066363000000123).UTC(),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	isMetrics := rm.ScopeMetrics().AppendEmpty()
	isMetrics.Scope().SetName("My Library")
	isMetrics.Scope().SetVersion("latest")
	m := isMetrics.Metrics().AppendEmpty()
	m.SetName("rpc_duration_seconds")
	m.SetEmptyHistogram()
	m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("code", "200")
	dp.Attributes().PutStr("method", "post")
	dp.SetTimestamp(pcommon.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
	dp.SetCount(2693)
	dp.SetSum(17560473)
	dp.BucketCounts().FromRaw([]uint64{3102, 170, 1501, 4228, 67655, 2693})
	dp.ExplicitBounds().FromRaw([]float64{0.01, 0.05, 0.5, 0.9, 0.99})

	assertMetricsEqual(t, expect, b.GetMetrics())
}
