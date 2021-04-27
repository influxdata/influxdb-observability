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

func TestWriteMetric_v2_gauge(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), otel2influx.MetricsSchemaTelegrafPrometheusV2)
	require.NoError(t, err)

	rm := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "node",
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
							Data: &otlpmetrics.Metric_Gauge{
								Gauge: &otlpmetrics.Gauge{
									DataPoints: []*otlpmetrics.NumberDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "engine_id", Value: "0"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        &otlpmetrics.NumberDataPoint_AsDouble{AsDouble: 23.9},
										},
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "engine_id", Value: "1"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        &otlpmetrics.NumberDataPoint_AsDouble{AsDouble: 11.9},
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
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"engine_id":            "0",
			},
			fields: map[string]interface{}{
				"cache_age_seconds":                      float64(23.9),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
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
				"cache_age_seconds":                      float64(11.9),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
		},
	}

	assert.EqualValues(t, expected, w.points)
}

func TestWriteMetric_v2_sum(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), otel2influx.MetricsSchemaTelegrafPrometheusV2)
	require.NoError(t, err)

	rm := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "node",
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
							Data: &otlpmetrics.Metric_Sum{
								Sum: &otlpmetrics.Sum{
									AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									IsMonotonic:            true,
									DataPoints: []*otlpmetrics.NumberDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "method", Value: "post"},
												{Key: "code", Value: "200"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        &otlpmetrics.NumberDataPoint_AsDouble{AsDouble: 1027},
										},
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "method", Value: "post"},
												{Key: "code", Value: "400"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        &otlpmetrics.NumberDataPoint_AsDouble{AsDouble: 3},
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
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
			},
			fields: map[string]interface{}{
				"http_requests_total":                    float64(1027),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
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
				"http_requests_total":                    float64(3),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
		},
	}

	assert.EqualValues(t, expected, w.points)
}

func TestWriteMetric_v2_histogram(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), otel2influx.MetricsSchemaTelegrafPrometheusV2)
	require.NoError(t, err)

	rm := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "node",
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
							Data: &otlpmetrics.Metric_Histogram{
								Histogram: &otlpmetrics.Histogram{
									AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
									DataPoints: []*otlpmetrics.HistogramDataPoint{
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
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
			},
			fields: map[string]interface{}{
				"http_request_duration_seconds_count":    float64(144320),
				"http_request_duration_seconds_sum":      float64(53423),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
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
				"http_request_duration_seconds_bucket":   float64(24054),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
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
				"http_request_duration_seconds_bucket":   float64(33444),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
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
				"http_request_duration_seconds_bucket":   float64(100392),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
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
				"http_request_duration_seconds_bucket":   float64(129389),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
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
				"http_request_duration_seconds_bucket":   float64(133988),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
		},
	}

	assert.Equal(t, expected, w.points)
}

func TestWriteMetric_v2_summary(t *testing.T) {
	c, err := otel2influx.NewOtelMetricsToLineProtocol(new(common.NoopLogger), otel2influx.MetricsSchemaTelegrafPrometheusV2)
	require.NoError(t, err)

	rm := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{
				Attributes: []*otlpcommon.KeyValue{
					{
						Key:   "node",
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
							Data: &otlpmetrics.Metric_Summary{
								Summary: &otlpmetrics.Summary{
									DataPoints: []*otlpmetrics.SummaryDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "method", Value: "post"},
												{Key: "code", Value: "200"},
											},
											TimeUnixNano: 1395066363000000123,
											Count:        2693,
											Sum:          17560473,
											QuantileValues: []*otlpmetrics.SummaryDataPoint_ValueAtQuantile{
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
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
			},
			fields: map[string]interface{}{
				"rpc_duration_seconds_count":             float64(2693),
				"rpc_duration_seconds_sum":               float64(17560473),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
				"quantile":                   "0.01",
			},
			fields: map[string]interface{}{
				"rpc_duration_seconds":                   float64(3102),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
				"quantile":                   "0.05",
			},
			fields: map[string]interface{}{
				"rpc_duration_seconds":                   float64(3272),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
				"quantile":                   "0.5",
			},
			fields: map[string]interface{}{
				"rpc_duration_seconds":                   float64(4773),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
				"quantile":                   "0.9",
			},
			fields: map[string]interface{}{
				"rpc_duration_seconds":                   float64(9001),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
		},
		{
			measurement: "prometheus",
			tags: map[string]string{
				"node":                 "42",
				"otel.library.name":    "My Library",
				"otel.library.version": "latest",
				"method":               "post",
				"code":                 "200",
				"quantile":                   "0.99",
			},
			fields: map[string]interface{}{
				"rpc_duration_seconds":                   float64(76656),
				"otel.resource.dropped_attributes_count": float64(1),
			},
			ts: time.Unix(0, 1395066363000000123),
		},
	}

	assert.Equal(t, expected, w.points)
}
