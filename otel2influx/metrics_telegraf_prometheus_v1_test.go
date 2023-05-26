package otel2influx_test

import (
	"context"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
)

func TestWriteMetric_v1_gauge(t *testing.T) {
	w := new(MockInfluxWriter)
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), w, common.MetricsSchemaTelegrafPrometheusV1)
	require.NoError(t, err)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("cache_age_seconds")
	m.SetDescription("Age in seconds of the current cache")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutInt("engine_id", 0)
	dp.SetStartTimestamp(startTimestamp)
	dp.SetTimestamp(timestamp)
	dp.SetDoubleValue(23.9)
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutInt("engine_id", 1)
	dp.SetStartTimestamp(startTimestamp)
	dp.SetTimestamp(timestamp)
	dp.SetDoubleValue(11.9)

	err = c.WriteMetrics(context.Background(), metrics)
	require.NoError(t, err)

	expected := []mockPoint{
		{
			measurement: "cache_age_seconds",
			tags: map[string]string{
				"container.name":       "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"engine_id":            "0",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"gauge":                           float64(23.9),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
			vType: common.InfluxMetricValueTypeGauge,
		},
		{
			measurement: "cache_age_seconds",
			tags: map[string]string{
				"container.name":       "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"engine_id":            "1",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"gauge":                           float64(11.9),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
			vType: common.InfluxMetricValueTypeGauge,
		},
	}

	assert.Equal(t, expected, w.points)
}

func TestWriteMetric_v1_gaugeFromSum(t *testing.T) {
	temporalities := [2]pmetric.AggregationTemporality{pmetric.AggregationTemporalityCumulative, pmetric.AggregationTemporalityDelta}
	for _, temporality := range temporalities {
		w := new(MockInfluxWriter)
		c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), w, common.MetricsSchemaTelegrafPrometheusV1)
		require.NoError(t, err)

		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("container.name", "42")
		ilMetrics := rm.ScopeMetrics().AppendEmpty()
		ilMetrics.Scope().SetName("My Library")
		ilMetrics.Scope().SetVersion("latest")
		m := ilMetrics.Metrics().AppendEmpty()
		m.SetName("cache_age_seconds")
		m.SetDescription("Age in seconds of the current cache")
		m.SetEmptySum()
		m.Sum().SetIsMonotonic(false)
		m.Sum().SetAggregationTemporality(temporality)
		dp := m.Sum().DataPoints().AppendEmpty()
		dp.Attributes().PutInt("engine_id", 0)
		dp.SetStartTimestamp(startTimestamp)
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(23.9)
		dp = m.Sum().DataPoints().AppendEmpty()
		dp.Attributes().PutInt("engine_id", 1)
		dp.SetStartTimestamp(startTimestamp)
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(11.9)

		err = c.WriteMetrics(context.Background(), metrics)
		require.NoError(t, err)

		expected := []mockPoint{
			{
				measurement: "cache_age_seconds",
				tags: map[string]string{
					"container.name":       "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"engine_id":            "0",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano: int64(startTimestamp),
					"gauge":                           float64(23.9),
				},
				ts:    time.Unix(0, 1395066363000000123).UTC(),
				vType: common.InfluxMetricValueTypeGauge,
			},
			{
				measurement: "cache_age_seconds",
				tags: map[string]string{
					"container.name":       "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"engine_id":            "1",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano: int64(startTimestamp),
					"gauge":                           float64(11.9),
				},
				ts:    time.Unix(0, 1395066363000000123).UTC(),
				vType: common.InfluxMetricValueTypeGauge,
			},
		}

		assert.Equal(t, expected, w.points)
	}
}

func TestWriteMetric_v1_sum(t *testing.T) {
	temporalities := [2]pmetric.AggregationTemporality{pmetric.AggregationTemporalityCumulative, pmetric.AggregationTemporalityDelta}

	for _, temporality := range temporalities {
		w := new(MockInfluxWriter)
		c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), w, common.MetricsSchemaTelegrafPrometheusV1)
		require.NoError(t, err)
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("container.name", "42")
		ilMetrics := rm.ScopeMetrics().AppendEmpty()
		ilMetrics.Scope().SetName("My Library")
		ilMetrics.Scope().SetVersion("latest")
		m := ilMetrics.Metrics().AppendEmpty()
		m.SetName("http_requests_total")
		m.SetDescription("The total number of HTTP requests")
		m.SetEmptySum()
		m.Sum().SetIsMonotonic(true)
		m.Sum().SetAggregationTemporality(temporality)
		dp := m.Sum().DataPoints().AppendEmpty()
		dp.Attributes().PutInt("code", 200)
		dp.Attributes().PutStr("method", "post")
		dp.SetStartTimestamp(startTimestamp)
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(1027)
		dp = m.Sum().DataPoints().AppendEmpty()
		dp.Attributes().PutInt("code", 400)
		dp.Attributes().PutStr("method", "post")
		dp.SetStartTimestamp(startTimestamp)
		dp.SetTimestamp(timestamp)
		dp.SetDoubleValue(3)

		err = c.WriteMetrics(context.Background(), metrics)
		require.NoError(t, err)

		expected := []mockPoint{
			{
				measurement: "http_requests_total",
				tags: map[string]string{
					"container.name":       "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano: int64(startTimestamp),
					"counter":                         float64(1027),
				},
				ts:    time.Unix(0, 1395066363000000123).UTC(),
				vType: common.InfluxMetricValueTypeSum,
			},
			{
				measurement: "http_requests_total",
				tags: map[string]string{
					"container.name":       "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "400",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano: int64(startTimestamp),
					"counter":                         float64(3),
				},
				ts:    time.Unix(0, 1395066363000000123).UTC(),
				vType: common.InfluxMetricValueTypeSum,
			},
		}

		assert.Equal(t, expected, w.points)
	}
}

func TestWriteMetric_v1_histogram(t *testing.T) {
	temporalities := [2]pmetric.AggregationTemporality{pmetric.AggregationTemporalityCumulative, pmetric.AggregationTemporalityDelta}
	for _, temporality := range temporalities {
		w := new(MockInfluxWriter)
		c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), w, common.MetricsSchemaTelegrafPrometheusV1)
		require.NoError(t, err)

		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("container.name", "42")
		ilMetrics := rm.ScopeMetrics().AppendEmpty()
		ilMetrics.Scope().SetName("My Library")
		ilMetrics.Scope().SetVersion("latest")
		m := ilMetrics.Metrics().AppendEmpty()
		m.SetName("http_request_duration_seconds")
		m.SetEmptyHistogram()
		m.SetDescription("A histogram of the request duration")
		m.Histogram().SetAggregationTemporality(temporality)
		dp := m.Histogram().DataPoints().AppendEmpty()
		dp.Attributes().PutInt("code", 200)
		dp.Attributes().PutStr("method", "post")
		dp.SetStartTimestamp(startTimestamp)
		dp.SetTimestamp(timestamp)
		dp.SetCount(144320)
		dp.SetSum(53423)
		dp.SetMin(0)
		dp.SetMax(100)
		dp.BucketCounts().FromRaw([]uint64{24054, 9390, 66948, 28997, 4599, 10332})
		dp.ExplicitBounds().FromRaw([]float64{0.05, 0.1, 0.2, 0.5, 1})

		err = c.WriteMetrics(context.Background(), metrics)
		require.NoError(t, err)

		expected := []mockPoint{
			{
				measurement: "http_request_duration_seconds",
				tags: map[string]string{
					"container.name":       "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano: int64(startTimestamp),
					"count":                           float64(144320),
					"sum":                             float64(53423),
					"0.05":                            float64(24054),
					"0.1":                             float64(33444),
					"0.2":                             float64(100392),
					"0.5":                             float64(129389),
					"1":                               float64(133988),
					"min":                             float64(0),
					"max":                             float64(100),
				},
				ts:    time.Unix(0, 1395066363000000123).UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
		}

		assert.Equal(t, expected, w.points)
	}
}

func TestWriteMetric_v1_histogram_missingInfinityBucket(t *testing.T) {
	temporalities := [2]pmetric.AggregationTemporality{pmetric.AggregationTemporalityCumulative, pmetric.AggregationTemporalityDelta}
	for _, temporality := range temporalities {
		w := new(MockInfluxWriter)
		c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), w, common.MetricsSchemaTelegrafPrometheusV1)
		require.NoError(t, err)

		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("container.name", "42")
		ilMetrics := rm.ScopeMetrics().AppendEmpty()
		ilMetrics.Scope().SetName("My Library")
		ilMetrics.Scope().SetVersion("latest")
		m := ilMetrics.Metrics().AppendEmpty()
		m.SetName("http_request_duration_seconds")
		m.SetEmptyHistogram()
		m.SetDescription("A histogram of the request duration")
		m.Histogram().SetAggregationTemporality(temporality)
		dp := m.Histogram().DataPoints().AppendEmpty()
		dp.Attributes().PutInt("code", 200)
		dp.Attributes().PutStr("method", "post")
		dp.SetStartTimestamp(startTimestamp)
		dp.SetTimestamp(timestamp)
		dp.SetCount(144320)
		dp.SetSum(53423)
		dp.BucketCounts().FromRaw([]uint64{24054, 9390, 66948, 28997, 4599, 10332})
		dp.ExplicitBounds().FromRaw([]float64{0.05, 0.1, 0.2, 0.5, 1})

		err = c.WriteMetrics(context.Background(), metrics)
		require.NoError(t, err)

		expected := []mockPoint{
			{
				measurement: "http_request_duration_seconds",
				tags: map[string]string{
					"container.name":       "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano: int64(startTimestamp),
					"count":                           float64(144320),
					"sum":                             float64(53423),
					"0.05":                            float64(24054),
					"0.1":                             float64(33444),
					"0.2":                             float64(100392),
					"0.5":                             float64(129389),
					"1":                               float64(133988),
				},
				ts:    time.Unix(0, 1395066363000000123).UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
		}

		assert.Equal(t, expected, w.points)
	}
}

func TestWriteMetric_v1_summary(t *testing.T) {
	w := new(MockInfluxWriter)
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), w, common.MetricsSchemaTelegrafPrometheusV1)
	require.NoError(t, err)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("rpc_duration_seconds")
	m.SetEmptySummary()
	m.SetDescription("A summary of the RPC duration in seconds")
	dp := m.Summary().DataPoints().AppendEmpty()
	dp.Attributes().PutInt("code", 200)
	dp.Attributes().PutStr("method", "post")
	dp.SetStartTimestamp(startTimestamp)
	dp.SetTimestamp(timestamp)
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

	err = c.WriteMetrics(context.Background(), metrics)
	require.NoError(t, err)

	expected := []mockPoint{
		{
			measurement: "rpc_duration_seconds",
			tags: map[string]string{
				"container.name":       "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"count":                           float64(2693),
				"sum":                             float64(17560473),
				"0.01":                            float64(3102),
				"0.05":                            float64(3272),
				"0.5":                             float64(4773),
				"0.9":                             float64(9001),
				"0.99":                            float64(76656),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
			vType: common.InfluxMetricValueTypeSummary,
		},
	}

	assert.Equal(t, expected, w.points)
}
