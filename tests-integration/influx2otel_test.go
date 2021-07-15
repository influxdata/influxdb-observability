package tests

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	"github.com/influxdata/telegraf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestInflux2Otel(t *testing.T) {
	for i, mt := range metricTests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Run("otelcol", func(t *testing.T) {
				otelcolReceiverAddress, mockExporterFactory := setupOtelcolInfluxDBReceiver(t)

				response, err := http.Post(fmt.Sprintf("http://%s/write", otelcolReceiverAddress), "", strings.NewReader(mt.lp))
				require.NoError(t, err)
				require.Equal(t, 2, response.StatusCode/100)

				got := new(otlpcollectormetrics.ExportMetricsServiceRequest)
				for _, rm := range mockExporterFactory.resourceMetrics {
					got.ResourceMetrics = append(got.ResourceMetrics, proto.Clone(rm).(*otlpmetrics.ResourceMetrics))
				}
				common.SortResourceMetrics(got.ResourceMetrics)

				expect := new(otlpcollectormetrics.ExportMetricsServiceRequest)
				for _, rm := range mt.otel {
					expect.ResourceMetrics = append(expect.ResourceMetrics, proto.Clone(rm).(*otlpmetrics.ResourceMetrics))
				}
				common.SortResourceMetrics(expect.ResourceMetrics)

				assertProtosEqual(t, expect, got)
			})

			t.Run("telegraf", func(t *testing.T) {
				expect := new(otlpcollectormetrics.ExportMetricsServiceRequest)
				for _, rm := range mt.otel {
					expect.ResourceMetrics = append(expect.ResourceMetrics, proto.Clone(rm).(*otlpmetrics.ResourceMetrics))
				}
				assertOtel2InfluxTelegraf(t, mt.lp, telegraf.Untyped, expect)
			})
		})
	}
}

func TestInflux2Otel_nowtime(t *testing.T) {
	t.Run("otelcol", func(t *testing.T) {
		otelcolReceiverAddress, mockExporterFactory := setupOtelcolInfluxDBReceiver(t)

		lp := `
cpu_temp,foo=bar gauge=87.332
`

		response, err := http.Post(fmt.Sprintf("http://%s/write", otelcolReceiverAddress), "", strings.NewReader(lp))
		require.NoError(t, err)
		assert.Equal(t, 2, response.StatusCode/100)

		gotTime := time.Unix(0, int64(mockExporterFactory.resourceMetrics[0].InstrumentationLibraryMetrics[0].Metrics[0].Data.(*otlpmetrics.Metric_DoubleGauge).DoubleGauge.DataPoints[0].TimeUnixNano))
		assert.WithinDuration(t, time.Now(), gotTime, time.Second)
	})
}

func TestInflux2Otel_unknownSchema(t *testing.T) {
	t.Run("telegraf", func(t *testing.T) {
		lp := `
cpu,cpu=cpu4,host=777348dc6343 usage_user=0.10090817356207936,usage_system=0.3027245206862381,usage_iowait=0,invalid="ignored" 1395066363000000123
`
		expect := &otlpcollectormetrics.ExportMetricsServiceRequest{
			ResourceMetrics: []*otlpmetrics.ResourceMetrics{
				{
					Resource: &otlpresource.Resource{},
					InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
						{
							InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{},
							Metrics: []*otlpmetrics.Metric{
								{
									Name: "cpu_usage_user",
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
									Name: "cpu_usage_system",
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
									Name: "cpu_usage_iowait",
									Data: &otlpmetrics.Metric_DoubleGauge{
										DoubleGauge: &otlpmetrics.DoubleGauge{
											DataPoints: []*otlpmetrics.DoubleDataPoint{
												{
													Labels: []*otlpcommon.StringKeyValue{
														{Key: "cpu", Value: "cpu4"},
														{Key: "host", Value: "777348dc6343"},
													},
													TimeUnixNano: 1395066363000000123,
													Value:        0.0,
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
		assertOtel2InfluxTelegraf(t, lp, telegraf.Untyped, expect)
	})
}

func TestInflux2Otel_gaugeNonPrometheus(t *testing.T) {
	t.Run("telegraf", func(t *testing.T) {
		lp := `
swap,host=8eaaf6b73054 used_percent=1.5,total=1073737728i 1626302080000000000
`
		expect := &otlpcollectormetrics.ExportMetricsServiceRequest{
			ResourceMetrics: []*otlpmetrics.ResourceMetrics{
				{
					Resource: &otlpresource.Resource{},
					InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
						{
							InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{},
							Metrics: []*otlpmetrics.Metric{
								{
									Name: "swap_used_percent",
									Data: &otlpmetrics.Metric_DoubleGauge{
										DoubleGauge: &otlpmetrics.DoubleGauge{
											DataPoints: []*otlpmetrics.DoubleDataPoint{
												{
													Labels: []*otlpcommon.StringKeyValue{
														{Key: "host", Value: "8eaaf6b73054"},
													},
													TimeUnixNano: 1626302080000000000,
													Value:        1.5,
												},
											},
										},
									},
								},
								{
									Name: "swap_total",
									Data: &otlpmetrics.Metric_DoubleGauge{
										DoubleGauge: &otlpmetrics.DoubleGauge{
											DataPoints: []*otlpmetrics.DoubleDataPoint{
												{
													Labels: []*otlpcommon.StringKeyValue{
														{Key: "host", Value: "8eaaf6b73054"},
													},
													TimeUnixNano: 1626302080000000000,
													Value:        1073737728,
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
		assertOtel2InfluxTelegraf(t, lp, telegraf.Gauge, expect)
	})
}

func TestInflux2Otel_counterNonPrometheus(t *testing.T) {
	t.Run("telegraf", func(t *testing.T) {
		lp := `
swap,host=8eaaf6b73054 in=32768i,out=12021760i 1626302080000000000
`
		expect := &otlpcollectormetrics.ExportMetricsServiceRequest{
			ResourceMetrics: []*otlpmetrics.ResourceMetrics{
				{
					Resource: &otlpresource.Resource{},
					InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
						{
							InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{},
							Metrics: []*otlpmetrics.Metric{
								{
									Name: "swap_in",
									Data: &otlpmetrics.Metric_DoubleSum{
										DoubleSum: &otlpmetrics.DoubleSum{
											DataPoints: []*otlpmetrics.DoubleDataPoint{
												{
													Labels: []*otlpcommon.StringKeyValue{
														{Key: "host", Value: "8eaaf6b73054"},
													},
													TimeUnixNano: 1626302080000000000,
													Value:        32768.0,
												},
											},
											AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
											IsMonotonic:            true,
										},
									},
								},
								{
									Name: "swap_out",
									Data: &otlpmetrics.Metric_DoubleSum{
										DoubleSum: &otlpmetrics.DoubleSum{
											DataPoints: []*otlpmetrics.DoubleDataPoint{
												{
													Labels: []*otlpcommon.StringKeyValue{
														{Key: "host", Value: "8eaaf6b73054"},
													},
													TimeUnixNano: 1626302080000000000,
													Value:        12021760.0,
												},
											},
											AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
											IsMonotonic:            true,
										},
									},
								},
							},
						},
					},
				},
			},
		}
		assertOtel2InfluxTelegraf(t, lp, telegraf.Counter, expect)
	})
}
