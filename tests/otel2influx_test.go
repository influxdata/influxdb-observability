package tests

import (
	"context"
	"fmt"
	"testing"

	otlpcollectorlogs "github.com/influxdata/influxdb-observability/otlp/collector/logs/v1"
	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpcollectortrace "github.com/influxdata/influxdb-observability/otlp/collector/trace/v1"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/pdata"
	"google.golang.org/protobuf/proto"
)

func TestOtel2Influx(t *testing.T) {
	t.Run("metrics", func(t *testing.T) {
		for i, mt := range metricTests {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Run("otelcol", func(t *testing.T) {
					mockDestination, mockReceiverFactory := setupOtelcolInfluxDBExporter(t)
					t.Cleanup(mockDestination.Close)

					request := &otlpcollectormetrics.ExportMetricsServiceRequest{
						ResourceMetrics: mt.metrics,
					}
					requestBytes, err := proto.Marshal(request)
					require.NoError(t, err)
					requestPdata, err := pdata.MetricsFromOtlpProtoBytes(requestBytes)
					require.NoError(t, err)

					err = mockReceiverFactory.nextMetricsConsumer.ConsumeMetrics(context.Background(), requestPdata)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, mt.lp, got)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)

					request := &otlpcollectormetrics.ExportMetricsServiceRequest{
						ResourceMetrics: mt.metrics,
					}

					client := otlpcollectormetrics.NewMetricsServiceClient(clientConn)
					_, err := client.Export(context.Background(), request)
					require.NoError(t, err)

					stopTelegraf() // flush telegraf buffers
					got := mockOutputPlugin.lineprotocol(t)

					assertLineprotocolEqual(t, mt.lp, got)
				})
			})
		}
	})

	t.Run("traces", func(t *testing.T) {
		for i, tt := range traceTests {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Run("otelcol", func(t *testing.T) {
					mockDestination, mockReceiverFactory := setupOtelcolInfluxDBExporter(t)
					t.Cleanup(mockDestination.Close)

					request := &otlpcollectortrace.ExportTraceServiceRequest{
						ResourceSpans: tt.spans,
					}
					requestBytes, err := proto.Marshal(request)
					require.NoError(t, err)
					requestPdata, err := pdata.TracesFromOtlpProtoBytes(requestBytes)
					require.NoError(t, err)

					err = mockReceiverFactory.nextTracesConsumer.ConsumeTraces(context.Background(), requestPdata)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, tt.lp, got)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)

					request := &otlpcollectortrace.ExportTraceServiceRequest{
						ResourceSpans: tt.spans,
					}

					client := otlpcollectortrace.NewTraceServiceClient(clientConn)
					_, err := client.Export(context.Background(), request)
					require.NoError(t, err)

					stopTelegraf() // flush telegraf buffers
					got := mockOutputPlugin.lineprotocol(t)

					assertLineprotocolEqual(t, tt.lp, got)
				})
			})
		}
	})

	t.Run("logs", func(t *testing.T) {
		for i, lt := range logTests {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Run("otelcol", func(t *testing.T) {
					mockDestination, mockReceiverFactory := setupOtelcolInfluxDBExporter(t)
					t.Cleanup(mockDestination.Close)

					request := &otlpcollectorlogs.ExportLogsServiceRequest{
						ResourceLogs: lt.logRecords,
					}
					requestBytes, err := proto.Marshal(request)
					require.NoError(t, err)
					requestPdata, err := pdata.LogsFromOtlpProtoBytes(requestBytes)
					require.NoError(t, err)

					err = mockReceiverFactory.nextLogsConsumer.ConsumeLogs(context.Background(), requestPdata)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, lt.lp, got)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)

					request := &otlpcollectorlogs.ExportLogsServiceRequest{
						ResourceLogs: lt.logRecords,
					}

					client := otlpcollectorlogs.NewLogsServiceClient(clientConn)
					_, err := client.Export(context.Background(), request)
					require.NoError(t, err)

					stopTelegraf() // flush telegraf buffers
					got := mockOutputPlugin.lineprotocol(t)

					assertLineprotocolEqual(t, lt.lp, got)
				})
			})
		}
	})
}
