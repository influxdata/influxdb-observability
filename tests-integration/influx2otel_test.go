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
	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
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
				mockInputPlugin, mockOtelService, stopTelegraf := setupTelegrafOpenTelemetryOutput(t)
				t.Cleanup(stopTelegraf)

				lpdec := lineprotocol.NewDecoder(strings.NewReader(mt.lp))
				for lpdec.Next() {
					name, err := lpdec.Measurement()
					require.NoError(t, err)
					tags := make(map[string]string)
					for k, v, _ := lpdec.NextTag(); k != nil; k, v, _ = lpdec.NextTag() {
						tags[string(k)] = string(v)
					}
					fields := make(map[string]interface{})
					for k, v, _ := lpdec.NextField(); k != nil; k, v, _ = lpdec.NextField() {
						fields[string(k)] = v.Interface()
					}
					ts, err := lpdec.Time(lineprotocol.Nanosecond, time.Now())
					require.NoError(t, err)
					mockInputPlugin.accumulator.AddFields(string(name), fields, tags, ts)
				}
				require.NoError(t, lpdec.Err())

				stopTelegraf() // flush telegraf buffers

				got := new(otlpcollectormetrics.ExportMetricsServiceRequest)
				select {
				case rm := <-mockOtelService.metrics:
					got.ResourceMetrics = rm
				case <-time.NewTimer(time.Second).C:
					t.Log("test timed out")
					t.Fail()
					return
				}
				common.SortResourceMetrics(got.ResourceMetrics)

				expect := new(otlpcollectormetrics.ExportMetricsServiceRequest)
				for _, rm := range mt.otel {
					expect.ResourceMetrics = append(expect.ResourceMetrics, proto.Clone(rm).(*otlpmetrics.ResourceMetrics))
				}
				common.SortResourceMetrics(expect.ResourceMetrics)

				assertProtosEqual(t, expect, got)
			})
		})
	}
}

func TestInflux2Otel_nowtime(t *testing.T) {
	t.Run("otelcol", func(t *testing.T) {
		otelcolReceiverAddress, mockExporterFactory := setupOtelcolInfluxDBReceiver(t)

		payload := `
cpu_temp,foo=bar gauge=87.332
`

		response, err := http.Post(fmt.Sprintf("http://%s/write", otelcolReceiverAddress), "", strings.NewReader(payload))
		require.NoError(t, err)
		assert.Equal(t, 2, response.StatusCode/100)

		gotTime := time.Unix(0, int64(mockExporterFactory.resourceMetrics[0].InstrumentationLibraryMetrics[0].Metrics[0].Data.(*otlpmetrics.Metric_DoubleGauge).DoubleGauge.DataPoints[0].TimeUnixNano))
		assert.WithinDuration(t, time.Now(), gotTime, time.Second)
	})
}

func TestInflux2Otel_unknownSchema(t *testing.T) {
	t.Run("telegraf", func(t *testing.T) {
		mockInputPlugin, mockOtelService, stopTelegraf := setupTelegrafOpenTelemetryOutput(t)
		t.Cleanup(stopTelegraf)

		payload := `
cpu,cpu=cpu4,host=777348dc6343 usage_user=0.10090817356207936,usage_system=0.3027245206862381,usage_iowait=0,invalid="ignored" 1395066363000000123
`

		lpdec := lineprotocol.NewDecoder(strings.NewReader(payload))
		for lpdec.Next() {
			name, err := lpdec.Measurement()
			require.NoError(t, err)
			tags := make(map[string]string)
			for k, v, _ := lpdec.NextTag(); k != nil; k, v, _ = lpdec.NextTag() {
				tags[string(k)] = string(v)
			}
			fields := make(map[string]interface{})
			for k, v, _ := lpdec.NextField(); k != nil; k, v, _ = lpdec.NextField() {
				fields[string(k)] = v.Interface()
			}
			ts, err := lpdec.Time(lineprotocol.Nanosecond, time.Now())
			require.NoError(t, err)
			mockInputPlugin.accumulator.AddFields(string(name), fields, tags, ts)
		}
		require.NoError(t, lpdec.Err())

		stopTelegraf()

		got := new(otlpcollectormetrics.ExportMetricsServiceRequest)
		select {
		case rm := <-mockOtelService.metrics:
			got.ResourceMetrics = rm
		case <-time.NewTimer(time.Second).C:
			t.Log("test timed out")
			t.Fail()
			return
		}
		common.SortResourceMetrics(got.ResourceMetrics)

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
		common.SortResourceMetrics(expect.ResourceMetrics)

		assertProtosEqual(t, expect, got)
	})
}
