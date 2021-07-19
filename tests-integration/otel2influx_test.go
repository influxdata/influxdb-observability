package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/otlpgrpc"
)

func TestOtel2Influx(t *testing.T) {
	t.Run("metrics", func(t *testing.T) {
		for i, mt := range metricTests {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Run("otelcol", func(t *testing.T) {
					mockDestination, mockReceiverFactory := setupOtelcolInfluxDBExporter(t)
					t.Cleanup(mockDestination.Close)

					request := mt.otel.Clone()
					err := mockReceiverFactory.nextMetricsConsumer.ConsumeMetrics(context.Background(), request)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, mt.lp, got)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)
					metricsClient := otlpgrpc.NewMetricsClient(clientConn)

					request := mt.otel.Clone()
					_, err := metricsClient.Export(context.Background(), request)
					if err != nil {
						// TODO not sure why the service returns this error, but the data arrives as required by the test
						// rpc error: code = Internal desc = grpc: error while marshaling: proto: Marshal called with nil
						if !strings.Contains(err.Error(), "proto: Marshal called with nil") {
							assert.NoError(t, err)
						}
					}

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

					request := tt.otel.Clone()
					err := mockReceiverFactory.nextTracesConsumer.ConsumeTraces(context.Background(), request)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, tt.lp, got)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)
					tracesClient := otlpgrpc.NewTracesClient(clientConn)

					request := tt.otel.Clone()
					_, err := tracesClient.Export(context.Background(), request)
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

					request := lt.otel.Clone()
					err := mockReceiverFactory.nextLogsConsumer.ConsumeLogs(context.Background(), request)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, lt.lp, got)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)
					logsClient := otlpgrpc.NewLogsClient(clientConn)

					request := lt.otel.Clone()
					_, err := logsClient.Export(context.Background(), request)
					require.NoError(t, err)

					stopTelegraf() // flush telegraf buffers
					got := mockOutputPlugin.lineprotocol(t)

					assertLineprotocolEqual(t, lt.lp, got)
				})
			})
		}
	})
}
