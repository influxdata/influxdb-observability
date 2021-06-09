package tests

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"google.golang.org/protobuf/proto"
)

func TestInflux2Otel(t *testing.T) {
	t.Run("metrics", func(t *testing.T) {
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
					sortResourceMetrics(got.ResourceMetrics)

					expect := new(otlpcollectormetrics.ExportMetricsServiceRequest)
					for _, rm := range mt.metrics {
						expect.ResourceMetrics = append(expect.ResourceMetrics, proto.Clone(rm).(*otlpmetrics.ResourceMetrics))
					}
					sortResourceMetrics(expect.ResourceMetrics)

					assertProtosEqual(t, expect, got)
				})

				// t.Run("telegraf", func(t *testing.T) {
				// 	clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)
				//
				// 	request := &otlpcollectormetrics.ExportMetricsServiceRequest{
				// 		ResourceMetrics: mt.metrics,
				// 	}
				//
				// 	client := otlpcollectormetrics.NewMetricsServiceClient(clientConn)
				// 	_, err := client.Export(context.Background(), request)
				// 	require.NoError(t, err)
				//
				// 	stopTelegraf() // flush telegraf buffers
				// 	got := mockOutputPlugin.lineprotocol(t)
				//
				// 	assertLineprotocolEqual(t, mt.lp, got)
				// })
			})
		}
	})
}

func TestInflux2Otel_nowtime(t *testing.T) {
	t.Run("metrics", func(t *testing.T) {
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
	})
}
