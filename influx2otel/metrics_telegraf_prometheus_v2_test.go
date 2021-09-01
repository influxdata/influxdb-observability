package influx2otel_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/influx2otel"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"
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

	expect := pdata.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("container.name", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("cache_age_seconds")
	m.SetDataType(pdata.MetricDataTypeGauge)
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("engine_id", "0")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(23.9)
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("engine_id", "1")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(11.9)

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

	expect := pdata.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("container.name", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("cache_age_seconds")
	m.SetDataType(pdata.MetricDataTypeGauge)
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("engine_id", "0")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(23.9)
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("engine_id", "1")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(11.9)

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

	expect := pdata.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("container.name", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetDataType(pdata.MetricDataTypeSum)
	m.Sum().SetIsMonotonic(true)
	m.Sum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
	dp := m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("code", "200")
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(1027)
	dp = m.Sum().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("code", "400")
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(3)

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

	expect := pdata.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("container.name", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_requests_total")
	m.SetDataType(pdata.MetricDataTypeGauge)
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("code", "200")
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(1027)
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("code", "400")
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetDoubleVal(3)

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

	expect := pdata.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("container.name", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_request_duration_seconds")
	m.SetDataType(pdata.MetricDataTypeHistogram)
	m.Histogram().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("code", "200")
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetCount(144320)
	dp.SetSum(53423)
	dp.SetBucketCounts([]uint64{24054, 33444, 100392, 129389, 133988, 144320})
	dp.SetExplicitBounds([]float64{0.05, 0.1, 0.2, 0.5, 1})

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

	expect := pdata.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("container.name", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("http_request_duration_seconds")
	m.SetDataType(pdata.MetricDataTypeHistogram)
	m.Histogram().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("code", "200")
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetCount(144320)
	dp.SetSum(53423)
	dp.SetBucketCounts([]uint64{24054, 33444, 100392, 129389, 133988, 144320})
	dp.SetExplicitBounds([]float64{0.05, 0.1, 0.2, 0.5, 1})

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

	expect := pdata.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("container.name", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("rpc_duration_seconds")
	m.SetDataType(pdata.MetricDataTypeSummary)
	dp := m.Summary().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("code", "200")
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.NewTimestampFromTime(time.Unix(0, 1395066363000000123)))
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

	expect := pdata.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().InsertString("container.name", "42")
	ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
	ilMetrics.InstrumentationLibrary().SetName("My Library")
	ilMetrics.InstrumentationLibrary().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("rpc_duration_seconds")
	m.SetDataType(pdata.MetricDataTypeHistogram)
	m.Histogram().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
	dp := m.Histogram().DataPoints().AppendEmpty()
	dp.Attributes().InsertString("code", "200")
	dp.Attributes().InsertString("method", "post")
	dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
	dp.SetCount(2693)
	dp.SetSum(17560473)
	dp.SetBucketCounts([]uint64{3102, 3272, 4773, 9001, 76656, 2693})
	dp.SetExplicitBounds([]float64{0.01, 0.05, 0.5, 0.9, 0.99})

	assertMetricsEqual(t, expect, b.GetMetrics())
}
