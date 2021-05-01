package otel2influx_test

import (
	"context"
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestWriteMetric_v1_gauge(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
	require.NoError(t, err)

	rm := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_IntValue{IntValue: 42}},
					},
				},
				DroppedAttributesCount: 1,
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name:        "cache_age_seconds",
							Description: "Age in seconds of the current cache",
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

	w := new(MockInfluxWriter)

	err = c.WriteMetrics(context.Background(), rm, w)
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
				"gauge": float64(23.9),
			},
			ts:    time.Unix(0, 1395066363000000123),
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
				"gauge": float64(11.9),
			},
			ts:    time.Unix(0, 1395066363000000123),
			vType: common.InfluxMetricValueTypeGauge,
		},
	}

	assert.Equal(t, expected, w.points)
}

func TestWriteMetric_v1_sum(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
	require.NoError(t, err)

	rm := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_IntValue{IntValue: 42}},
					},
				},
				DroppedAttributesCount: 1,
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name:        "http_requests_total",
							Description: "The total number of HTTP requests.",
							Data: &otlpmetrics.Metric_DoubleSum{
								DoubleSum: &otlpmetrics.DoubleSum{
									AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*otlpmetrics.DoubleDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "method", Value: "post"},
												{Key: "code", Value: "200"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        1027,
										},
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "method", Value: "post"},
												{Key: "code", Value: "400"},
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

	w := new(MockInfluxWriter)

	err = c.WriteMetrics(context.Background(), rm, w)
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
				"counter": float64(1027),
			},
			ts:    time.Unix(0, 1395066363000000123),
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
				"counter": float64(3),
			},
			ts:    time.Unix(0, 1395066363000000123),
			vType: common.InfluxMetricValueTypeSum,
		},
	}

	assert.Equal(t, expected, w.points)
}

func TestWriteMetric_v1_histogram(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
	require.NoError(t, err)

	rm := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_IntValue{IntValue: 42}},
					},
				},
				DroppedAttributesCount: 1,
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name:        "http_request_duration_seconds",
							Description: "A histogram of the request duration",
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

	w := new(MockInfluxWriter)

	err = c.WriteMetrics(context.Background(), rm, w)
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
				"count": float64(144320),
				"sum":   float64(53423),
				"0.05":  float64(24054),
				"0.1":   float64(33444),
				"0.2":   float64(100392),
				"0.5":   float64(129389),
				"1":     float64(133988),
			},
			ts:    time.Unix(0, 1395066363000000123),
			vType: common.InfluxMetricValueTypeHistogram,
		},
	}

	assert.Equal(t, expected, w.points)
}

func TestWriteMetric_v1_summary(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), common.MetricsSchemaTelegrafPrometheusV1)
	require.NoError(t, err)

	rm := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "container.name",
						Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_IntValue{IntValue: 42}},
					},
				},
				DroppedAttributesCount: 1,
			},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
						Name:    "My Library",
						Version: "latest",
					},
					Metrics: []*otlpmetrics.Metric{
						{
							Name:        "rpc_duration_seconds",
							Description: "A summary of the RPC duration in seconds.",
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

	w := new(MockInfluxWriter)

	err = c.WriteMetrics(context.Background(), rm, w)
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
				"count": float64(2693),
				"sum":   float64(17560473),
				"0.01":  float64(3102),
				"0.05":  float64(3272),
				"0.5":   float64(4773),
				"0.9":   float64(9001),
				"0.99":  float64(76656),
			},
			ts:    time.Unix(0, 1395066363000000123),
			vType: common.InfluxMetricValueTypeSummary,
		},
	}

	assert.Equal(t, expected, w.points)
}
