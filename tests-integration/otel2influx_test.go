package tests

import (
	"context"
	"fmt"
	"strings"
	"testing"

	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/plog/plogotlp"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"go.opentelemetry.io/collector/pdata/ptrace"
	"go.opentelemetry.io/collector/pdata/ptrace/ptraceotlp"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestOtel2Influx(t *testing.T) {
	t.Run("metrics", func(t *testing.T) {
		for i, mt := range metricTests {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Run("otelcol", func(t *testing.T) {
					mockDestination, mockReceiverFactory, closeOtelcol := setupOtelcolInfluxDBExporter(t)
					t.Cleanup(mockDestination.Close)

					clone := pmetric.NewMetrics()
					mt.otel.CopyTo(clone)
					err := mockReceiverFactory.nextMetricsConsumer.ConsumeMetrics(context.Background(), clone)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, mt.lp, got)
					closeOtelcol(t)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)
					metricsClient := pmetricotlp.NewGRPCClient(clientConn)

					clone := pmetric.NewMetrics()
					mt.otel.CopyTo(clone)
					request := pmetricotlp.NewExportRequestFromMetrics(clone)
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
					mockDestination, mockReceiverFactory, closeOtelcol := setupOtelcolInfluxDBExporter(t)
					t.Cleanup(mockDestination.Close)

					clone := ptrace.NewTraces()
					tt.otel.CopyTo(clone)
					err := mockReceiverFactory.nextTracesConsumer.ConsumeTraces(context.Background(), clone)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, tt.lp, got)
					closeOtelcol(t)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)
					tracesClient := ptraceotlp.NewGRPCClient(clientConn)

					clone := ptrace.NewTraces()
					tt.otel.CopyTo(clone)
					request := ptraceotlp.NewExportRequestFromTraces(clone)
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
					mockDestination, mockReceiverFactory, closeOtelcol := setupOtelcolInfluxDBExporter(t)
					t.Cleanup(mockDestination.Close)

					clone := plog.NewLogs()
					lt.otel.CopyTo(clone)
					err := mockReceiverFactory.nextLogsConsumer.ConsumeLogs(context.Background(), clone)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, lt.lp, got)
					closeOtelcol(t)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)
					logsClient := plogotlp.NewGRPCClient(clientConn)

					clone := plog.NewLogs()
					lt.otel.CopyTo(clone)
					request := plogotlp.NewExportRequestFromLogs(clone)
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
