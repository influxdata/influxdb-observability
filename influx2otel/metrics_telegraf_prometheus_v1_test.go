package influx2otel_test

import (
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/influx2otel"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestAddPoint_v1_gauge(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
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
		time.Unix(0, 1395066363000000123),
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
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeGauge)
	require.NoError(t, err)

	expect := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "42"}},
					},
				},
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name: "cache_age_seconds",
							Data: &otlpmetrics.Metric_DoubleGauge{
								DoubleGauge: &otlpmetrics.DoubleGauge{
									DataPoints: []*otlpmetrics.DoubleDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "engine_id", Value: "0"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        23.9,
										},
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "engine_id", Value: "1"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        11.9,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	common.SortResourceMetrics(expect)
	got := b.ToProto()
	common.SortResourceMetrics(got)

	assert.Equal(t, expect, got)
}

func TestAddPoint_v1_untypedGauge(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
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
		time.Unix(0, 1395066363000000123),
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
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "42"}},
					},
				},
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name: "cache_age_seconds",
							Data: &otlpmetrics.Metric_DoubleGauge{
								DoubleGauge: &otlpmetrics.DoubleGauge{
									DataPoints: []*otlpmetrics.DoubleDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "engine_id", Value: "0"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        23.9,
										},
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "engine_id", Value: "1"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        11.9,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	common.SortResourceMetrics(expect)
	got := b.ToProto()
	common.SortResourceMetrics(got)

	assert.Equal(t, expect, got)
}

func TestAddPoint_v1_sum(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
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
		time.Unix(0, 1395066363000000123),
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
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeSum)
	require.NoError(t, err)

	expect := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "42"}},
					},
				},
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name: "http_requests_total",
							Data: &otlpmetrics.Metric_DoubleSum{
								DoubleSum: &otlpmetrics.DoubleSum{
									AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*otlpmetrics.DoubleDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "code", Value: "200"},
												{Key: "method", Value: "post"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        1027,
										},
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "code", Value: "400"},
												{Key: "method", Value: "post"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        3,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	common.SortResourceMetrics(expect)
	got := b.ToProto()
	common.SortResourceMetrics(got)

	assert.Equal(t, expect, got)
}

func TestAddPoint_v1_untypedSum(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
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
		time.Unix(0, 1395066363000000123),
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
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "42"}},
					},
				},
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name: "http_requests_total",
							Data: &otlpmetrics.Metric_DoubleSum{
								DoubleSum: &otlpmetrics.DoubleSum{
									AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*otlpmetrics.DoubleDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "code", Value: "200"},
												{Key: "method", Value: "post"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        1027,
										},
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "code", Value: "400"},
												{Key: "method", Value: "post"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        3,
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	common.SortResourceMetrics(expect)
	got := b.ToProto()
	common.SortResourceMetrics(got)

	assert.Equal(t, expect, got)
}

func TestAddPoint_v1_histogram(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
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
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeHistogram)
	require.NoError(t, err)

	expect := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "42"}},
					},
				},
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name: "http_request_duration_seconds",
							Data: &otlpmetrics.Metric_DoubleHistogram{
								DoubleHistogram: &otlpmetrics.DoubleHistogram{
									AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									DataPoints: []*otlpmetrics.DoubleHistogramDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "code", Value: "200"},
												{Key: "method", Value: "post"},
											},
											TimeUnixNano:   1395066363000000123,
											Count:          144320,
											Sum:            53423,
											BucketCounts:   []uint64{24054, 33444, 100392, 129389, 133988, 144320},
											ExplicitBounds: []float64{0.05, 0.1, 0.2, 0.5, 1},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	common.SortResourceMetrics(expect)
	got := b.ToProto()
	common.SortResourceMetrics(got)

	assert.Equal(t, expect, got)
}

func TestAddPoint_v1_untypedHistogram(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
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
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "42"}},
					},
				},
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name: "http_request_duration_seconds",
							Data: &otlpmetrics.Metric_DoubleHistogram{
								DoubleHistogram: &otlpmetrics.DoubleHistogram{
									AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									DataPoints: []*otlpmetrics.DoubleHistogramDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "code", Value: "200"},
												{Key: "method", Value: "post"},
											},
											TimeUnixNano:   1395066363000000123,
											Count:          144320,
											Sum:            53423,
											BucketCounts:   []uint64{24054, 33444, 100392, 129389, 133988, 144320},
											ExplicitBounds: []float64{0.05, 0.1, 0.2, 0.5, 1},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	common.SortResourceMetrics(expect)
	got := b.ToProto()
	common.SortResourceMetrics(got)

	assert.Equal(t, expect, got)
}

func TestAddPoint_v1_summary(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
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

	expect := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "42"}},
					},
				},
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name: "rpc_duration_seconds",
							Data: &otlpmetrics.Metric_DoubleSummary{
								DoubleSummary: &otlpmetrics.DoubleSummary{
									DataPoints: []*otlpmetrics.DoubleSummaryDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "method", Value: "post"},
												{Key: "code", Value: "200"},
											},
											TimeUnixNano: 1395066363000000123,
											Count:        2693,
											Sum:          17560473,
											QuantileValues: []*otlpmetrics.DoubleSummaryDataPoint_ValueAtQuantile{
												{Quantile: 0.01, Value: 3102},
												{Quantile: 0.05, Value: 3272},
												{Quantile: 0.5, Value: 4773},
												{Quantile: 0.9, Value: 9001},
												{Quantile: 0.99, Value: 76656},
											},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	common.SortResourceMetrics(expect)
	got := b.ToProto()
	common.SortResourceMetrics(got)

	assert.Equal(t, expect, got)
}

func TestAddPoint_v1_untypedSummary(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
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
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "42"}},
					},
				},
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name: "rpc_duration_seconds",
							Data: &otlpmetrics.Metric_DoubleHistogram{
								DoubleHistogram: &otlpmetrics.DoubleHistogram{
									AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									DataPoints: []*otlpmetrics.DoubleHistogramDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "method", Value: "post"},
												{Key: "code", Value: "200"},
											},
											TimeUnixNano:   1395066363000000123,
											Count:          2693,
											Sum:            17560473,
											BucketCounts:   []uint64{3102, 3272, 4773, 9001, 76656, 2693},
											ExplicitBounds: []float64{0.01, 0.05, 0.5, 0.9, 0.99},
										},
									},
								},
							},
						},
					},
				},
			},
		},
	}

	common.SortResourceMetrics(expect)
	got := b.ToProto()
	common.SortResourceMetrics(got)

	assert.Equal(t, expect, got)
}
