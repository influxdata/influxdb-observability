package otel2influx_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"
)

func TestWriteMetric_v2_gauge(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV2)
	require.NoError(t, err)

	metrics := pdata.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("node", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("cache_age_seconds")
	m.SetDescription("Age in seconds of the current cache")
	m.SetDataType(pdata.MetricDataTypeGauge)
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().InsertInt("engine_id", 0)
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(23.9)
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().InsertInt("engine_id", 1)
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(11.9)

	w := new(MockInfluxWriter)

	err = c.WriteMetrics(context.Background(), metrics, w)
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
				"cache_age_seconds": float64(23.9),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"cache_age_seconds": float64(11.9),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
			vType: common.InfluxMetricValueTypeGauge,
		},
	}

	assert.EqualValues(t, expected, w.points)
}

func TestWriteMetric_v2_sum(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV2)
	require.NoError(t, err)

	metrics := pdata.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("node", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetDescription("The total number of HTTP requests")
	m.SetDataType(pdata.MetricDataTypeSum)
	m.Sum().SetIsMonotonic(true)
	m.Sum().SetAggregationTemporality(pdata.MetricAggregationTemporalityCumulative)
	dp := m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().InsertInt("code", 200)
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(1027)
	dp = m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().InsertInt("code", 400)
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(3)

	w := new(MockInfluxWriter)

	err = c.WriteMetrics(context.Background(), metrics, w)
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
				"http_requests_total": float64(1027),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"http_requests_total": float64(3),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
			vType: common.InfluxMetricValueTypeSum,
		},
	}

	assert.EqualValues(t, expected, w.points)
}

func TestWriteMetric_v2_histogram(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV2)
	require.NoError(t, err)

	metrics := pdata.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("node", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_request_duration_seconds")
	m.SetDataType(pdata.MetricDataTypeHistogram)
	m.SetDescription("A histogram of the request duration")
	m.Histogram().SetAggregationTemporality(pdata.MetricAggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().InsertInt("code", 200)
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetCount(144320)
	dp.SetSum(53423)
	dp.SetBucketCounts([]uint64{24054, 33444, 100392, 129389, 133988, 144320})
	dp.SetExplicitBounds([]float64{0.05, 0.1, 0.2, 0.5, 1})

	w := new(MockInfluxWriter)

	err = c.WriteMetrics(context.Background(), metrics, w)
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
				"http_request_duration_seconds_count": float64(144320),
				"http_request_duration_seconds_sum":   float64(53423),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"http_request_duration_seconds_bucket": float64(24054),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"http_request_duration_seconds_bucket": float64(33444),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"http_request_duration_seconds_bucket": float64(100392),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"http_request_duration_seconds_bucket": float64(129389),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"http_request_duration_seconds_bucket": float64(133988),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
			vType: common.InfluxMetricValueTypeHistogram,
		},
	}

	assert.Equal(t, expected, w.points)
}

func TestWriteMetric_v2_summary(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV2)
	require.NoError(t, err)

	metrics := pdata.NewMetrics()
	rm := metrics.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("node", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("rpc_duration_seconds")
	m.SetDataType(pdata.MetricDataTypeSummary)
	m.SetDescription("A summary of the RPC duration in seconds")
	dp := m.Summary().DataPoints().AppendEmpty()
	dp.Attributes().InsertInt("code", 200)
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
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

	w := new(MockInfluxWriter)

	err = c.WriteMetrics(context.Background(), metrics, w)
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
				"rpc_duration_seconds_count": float64(2693),
				"rpc_duration_seconds_sum":   float64(17560473),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"rpc_duration_seconds": float64(3102),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"rpc_duration_seconds": float64(3272),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"rpc_duration_seconds": float64(4773),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"rpc_duration_seconds": float64(9001),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
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
				"rpc_duration_seconds": float64(76656),
			},
			ts:    time.Unix(0, 1395066363000000123).UTC(),
			vType: common.InfluxMetricValueTypeSummary,
		},
	}

	assert.Equal(t, expected, w.points)
}
