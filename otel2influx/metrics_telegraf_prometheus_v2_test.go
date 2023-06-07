package otel2influx_test

import (
	"context"
	"testing"

	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
)

func TestWriteMetric_v2_gauge(t *testing.T) {
	w := new(MockInfluxWriter)
	cfg := otel2influx.DefaultOtelMetricsToLineProtocolConfig()
	cfg.Writer = w
	cfg.Schema = common.MetricsSchemaTelegrafPrometheusV2
	c, err := otel2influx.NewOtelMetricsToLineProtocol(cfg)
	require.NoError(t, err)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("node", "42")
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
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"engine_id":            "0",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"cache_age_seconds":               float64(23.9),
			},
			ts:    timestamp.AsTime().UTC(),
			vType: common.InfluxMetricValueTypeGauge,
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"engine_id":            "1",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"cache_age_seconds":               float64(11.9),
			},
			ts:    timestamp.AsTime().UTC(),
			vType: common.InfluxMetricValueTypeGauge,
		},
	}

	assert.EqualValues(t, expected, w.points)
}

func TestWriteMetric_v2_gaugeFromSum(t *testing.T) {
	for _, temporality := range temporalities {
		w := new(MockInfluxWriter)
		cfg := otel2influx.DefaultOtelMetricsToLineProtocolConfig()
		cfg.Writer = w
		cfg.Schema = common.MetricsSchemaTelegrafPrometheusV2
		c, err := otel2influx.NewOtelMetricsToLineProtocol(cfg)
		require.NoError(t, err)

		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("node", "42")
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
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"engine_id":            "0",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano: int64(startTimestamp),
					"cache_age_seconds":               float64(23.9),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeGauge,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"engine_id":            "1",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano: int64(startTimestamp),
					"cache_age_seconds":               float64(11.9),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeGauge,
			},
		}

		assert.EqualValues(t, expected, w.points)
	}
}

func TestWriteMetric_v2_sum(t *testing.T) {
	for _, temporality := range temporalities {
		w := new(MockInfluxWriter)
		cfg := otel2influx.DefaultOtelMetricsToLineProtocolConfig()
		cfg.Writer = w
		cfg.Schema = common.MetricsSchemaTelegrafPrometheusV2
		c, err := otel2influx.NewOtelMetricsToLineProtocol(cfg)
		require.NoError(t, err)

		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("node", "42")
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
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano: int64(startTimestamp),
					"http_requests_total":             float64(1027),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeSum,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "400",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano: int64(startTimestamp),
					"http_requests_total":             float64(3),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeSum,
			},
		}

		assert.EqualValues(t, expected, w.points)
	}
}

func TestWriteMetric_v2_histogram(t *testing.T) {
	for _, temporality := range temporalities {
		w := new(MockInfluxWriter)
		cfg := otel2influx.DefaultOtelMetricsToLineProtocolConfig()
		cfg.Writer = w
		cfg.Schema = common.MetricsSchemaTelegrafPrometheusV2
		c, err := otel2influx.NewOtelMetricsToLineProtocol(cfg)
		require.NoError(t, err)

		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("node", "42")
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
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:     int64(startTimestamp),
					"http_request_duration_seconds_count": float64(144320),
					"http_request_duration_seconds_sum":   float64(53423),
					"http_request_duration_seconds_min":   float64(0),
					"http_request_duration_seconds_max":   float64(100),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
					"le":                   "0.05",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:      int64(startTimestamp),
					"http_request_duration_seconds_bucket": float64(24054),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
					"le":                   "0.1",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:      int64(startTimestamp),
					"http_request_duration_seconds_bucket": float64(33444),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
					"le":                   "0.2",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:      int64(startTimestamp),
					"http_request_duration_seconds_bucket": float64(100392),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
					"le":                   "0.5",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:      int64(startTimestamp),
					"http_request_duration_seconds_bucket": float64(129389),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
					"le":                   "1",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:      int64(startTimestamp),
					"http_request_duration_seconds_bucket": float64(133988),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
		}

		assert.Equal(t, expected, w.points)
	}
}

func TestWriteMetric_v2_histogram_missingInfinityBucket(t *testing.T) {
	for _, temporality := range temporalities {
		w := new(MockInfluxWriter)
		cfg := otel2influx.DefaultOtelMetricsToLineProtocolConfig()
		cfg.Writer = w
		cfg.Schema = common.MetricsSchemaTelegrafPrometheusV2
		c, err := otel2influx.NewOtelMetricsToLineProtocol(cfg)
		require.NoError(t, err)

		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		rm.Resource().Attributes().PutStr("node", "42")
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
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:     int64(startTimestamp),
					"http_request_duration_seconds_count": float64(144320),
					"http_request_duration_seconds_sum":   float64(53423),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
					"le":                   "0.05",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:      int64(startTimestamp),
					"http_request_duration_seconds_bucket": float64(24054),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
					"le":                   "0.1",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:      int64(startTimestamp),
					"http_request_duration_seconds_bucket": float64(33444),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
					"le":                   "0.2",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:      int64(startTimestamp),
					"http_request_duration_seconds_bucket": float64(100392),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
					"le":                   "0.5",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:      int64(startTimestamp),
					"http_request_duration_seconds_bucket": float64(129389),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
			{
				measurement: "prometheus",
				tags: map[string]string{
					"node":                 "42",
					"otel.library.name":    "My Library",
					"otel.library.version": "latest",
					"method":               "post",
					"code":                 "200",
					"le":                   "1",
				},
				fields: map[string]interface{}{
					common.AttributeStartTimeUnixNano:      int64(startTimestamp),
					"http_request_duration_seconds_bucket": float64(133988),
				},
				ts:    timestamp.AsTime().UTC(),
				vType: common.InfluxMetricValueTypeHistogram,
			},
		}

		assert.Equal(t, expected, w.points)
	}
}

func TestWriteMetric_v2_summary(t *testing.T) {
	w := new(MockInfluxWriter)
	cfg := otel2influx.DefaultOtelMetricsToLineProtocolConfig()
	cfg.Writer = w
	cfg.Schema = common.MetricsSchemaTelegrafPrometheusV2
	c, err := otel2influx.NewOtelMetricsToLineProtocol(cfg)
	require.NoError(t, err)

	metrics := pmetric.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("node", "42")
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
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"rpc_duration_seconds_count":      float64(2693),
				"rpc_duration_seconds_sum":        float64(17560473),
			},
			ts:    timestamp.AsTime().UTC(),
			vType: common.InfluxMetricValueTypeSummary,
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
				"quantile":             "0.01",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"rpc_duration_seconds":            float64(3102),
			},
			ts:    timestamp.AsTime().UTC(),
			vType: common.InfluxMetricValueTypeSummary,
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
				"quantile":             "0.05",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"rpc_duration_seconds":            float64(3272),
			},
			ts:    timestamp.AsTime().UTC(),
			vType: common.InfluxMetricValueTypeSummary,
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
				"quantile":             "0.5",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"rpc_duration_seconds":            float64(4773),
			},
			ts:    timestamp.AsTime().UTC(),
			vType: common.InfluxMetricValueTypeSummary,
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
				"quantile":             "0.9",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"rpc_duration_seconds":            float64(9001),
			},
			ts:    timestamp.AsTime().UTC(),
			vType: common.InfluxMetricValueTypeSummary,
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
				"quantile":             "0.99",
			},
			fields: map[string]interface{}{
				common.AttributeStartTimeUnixNano: int64(startTimestamp),
				"rpc_duration_seconds":            float64(76656),
			},
			ts:    timestamp.AsTime().UTC(),
			vType: common.InfluxMetricValueTypeSummary,
		},
	}

	assert.Equal(t, expected, w.points)
}
