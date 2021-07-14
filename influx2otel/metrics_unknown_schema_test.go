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

func TestUnknownSchema(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	// cpu,cpu=cpu4,host=777348dc6343 usage_user=0.10090817356207936,usage_system=0.3027245206862381,usage_iowait=0 1395066363000000123
	b := c.NewBatch()
	err = b.AddPoint("cpu",
		map[string]string{
			"cpu":  "cpu4",
			"host": "777348dc6343",
		},
		map[string]interface{}{
			"usage_user":   0.10090817356207936,
			"usage_system": 0.3027245206862381,
			"usage_iowait": 0.0,
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{},
					Metrics: []*otlpmetrics.Metric{
						{
							Name: "cpu:usage_user",
							Data: &otlpmetrics.Metric_DoubleGauge{
								DoubleGauge: &otlpmetrics.DoubleGauge{
									DataPoints: []*otlpmetrics.DoubleDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "cpu", Value: "cpu4"},
												{Key: "host", Value: "777348dc6343"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        0.10090817356207936,
										},
									},
								},
							},
						},
						{
							Name: "cpu:usage_system",
							Data: &otlpmetrics.Metric_DoubleGauge{
								DoubleGauge: &otlpmetrics.DoubleGauge{
									DataPoints: []*otlpmetrics.DoubleDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "cpu", Value: "cpu4"},
												{Key: "host", Value: "777348dc6343"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        0.3027245206862381,
										},
									},
								},
							},
						},
						{
							Name: "cpu:usage_iowait",
							Data: &otlpmetrics.Metric_DoubleGauge{
								DoubleGauge: &otlpmetrics.DoubleGauge{
									DataPoints: []*otlpmetrics.DoubleDataPoint{
										{
											Labels: []*otlpcommon.StringKeyValue{
												{Key: "cpu", Value: "cpu4"},
												{Key: "host", Value: "777348dc6343"},
											},
											TimeUnixNano: 1395066363000000123,
											Value:        0,
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
