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

func TestStatsdTimingSchema(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("test_service_stage_metrics_biz_success_v4",
		map[string]string{
			"metric_type": "timing",
			"type":        "app",
		},
		map[string]interface{}{
			"count":  float64(10),
			"lower":  float64(10),
			"mean":   float64(10),
			"median": float64(10),
			"stddev": float64(10),
			"sum":    float64(100),
			"upper":  float64(20),
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_service_stage_metrics_biz_success_v4_count")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "timing")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(10)

	m = ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_service_stage_metrics_biz_success_v4_lower")
	m.SetEmptyGauge()
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "timing")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(10)

	m = ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_service_stage_metrics_biz_success_v4_mean")
	m.SetEmptyGauge()
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "timing")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(10)

	m = ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_service_stage_metrics_biz_success_v4_median")
	m.SetEmptyGauge()
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "timing")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(10)

	m = ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_service_stage_metrics_biz_success_v4_stddev")
	m.SetEmptyGauge()
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "timing")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(10)

	m = ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_service_stage_metrics_biz_success_v4_sum")
	m.SetEmptyGauge()
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "timing")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(100)

	m = ilMetrics.Metrics().AppendEmpty()
	m.SetName("test_service_stage_metrics_biz_success_v4_upper")
	m.SetEmptyGauge()
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "timing")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(20)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestStatsCounter(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	// statsd metric:
	// gorets:1|c
	b := c.NewBatch()
	err = b.AddPoint("gorets",
		map[string]string{
			"metric_type": "counter",
			"type":        "app",
		},
		map[string]interface{}{
			"value": int64(10),
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeSum)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("gorets_value")
	m.SetEmptySum()
	m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
	m.Sum().SetIsMonotonic(true)
	dp := m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "counter")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetIntValue(10)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestStatsDeltaCounter(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	// statsd metric:
	// gorets:1|c
	b := c.NewBatch()
	err = b.AddPoint("gorets",
		map[string]string{
			"metric_type": "counter",
			"type":        "app",
			"temporality": "delta",
		},
		map[string]interface{}{
			"value": int64(10),
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeSum)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("gorets_value")
	m.SetEmptySum()
	m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	m.Sum().SetIsMonotonic(true)
	dp := m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "counter")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetIntValue(10)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestStatsGauge(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	// statsd metric:
	// gaugor:333|g
	b := c.NewBatch()
	err = b.AddPoint("gaugor",
		map[string]string{
			"metric_type": "gauge",
			"type":        "app",
		},
		map[string]interface{}{
			"value": int64(333),
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeGauge)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("gaugor_value")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()

	dp.Attributes().PutStr("metric_type", "gauge")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetIntValue(333)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestStatsdSetsSchema(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	// statsd metric:
	// uniques:765|s
	b := c.NewBatch()
	err = b.AddPoint("uniques",
		map[string]string{
			"metric_type": "sets",
			"type":        "app",
		},
		map[string]interface{}{
			"value": int64(1),
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeUntyped)

	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("uniques_value")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "sets")
	dp.Attributes().PutStr("type", "app")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetIntValue(1)

	assertMetricsEqual(t, expect, b.GetMetrics())
}

func TestDeltaTemporalityStatsdCounter(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	// statsd metric:
	// gorets:1|c
	b := c.NewBatch()
	err = b.AddPoint("gorets",
		map[string]string{
			"metric_type": "counter",
			"type":        "app",
			"temporality": "delta",
		},
		map[string]interface{}{
			"value":      int64(10),
			"start_time": "2023-04-13T22:34:00.000535129+03:00",
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeSum)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("gorets_value")
	m.SetEmptySum()
	m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityDelta)
	m.Sum().SetIsMonotonic(true)
	dp := m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("metric_type", "counter")
	dp.Attributes().PutStr("type", "app")
	dp.SetStartTimestamp(pcommon.Timestamp(1681414440000535129))
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetIntValue(10)

	assertMetricsEqual(t, expect, b.GetMetrics())
}
