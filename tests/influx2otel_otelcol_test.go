package tests

import (
	"bytes"
	"context"
	"fmt"
	"net"
	"net/http"
	"strings"
	"testing"
	"time"

	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.opentelemetry.io/collector/extension/healthcheckextension"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/parserprovider"
	"go.uber.org/zap"
	"google.golang.org/protobuf/encoding/protojson"
	"google.golang.org/protobuf/proto"
)

func findOpenTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func TestInflux2Otel_otelcol(t *testing.T) {
	otelcolReceiverAddress, mockExporterFactory := setupOtelcolInfluxDBReceiver(t)

	payload := `
cpu_temp,foo=bar gauge=87.332 1622848686000000000
http_requests_total,method=post,code=200 counter=1027 1622848686000000000
http_requests_total,method=post,code=400 counter=3 1622848686000000000
http_request_duration_seconds,region=eu 0.05=24054,0.1=33444,0.2=100392,0.5=129389,1=133988,sum=53423,count=144320 1622848686000000000
`

	response, err := http.Post(fmt.Sprintf("http://%s/write", otelcolReceiverAddress), "", strings.NewReader(payload))
	require.NoError(t, err)
	assert.Equal(t, 2, response.StatusCode/100)

	if !assert.Len(t, mockExporterFactory.resourceMetrics, 1) {
		return
	}
	sortResourceMetrics(mockExporterFactory.resourceMetrics)

	expect := []*otlpmetrics.ResourceMetrics{
		{
			Resource: &otlpresource.Resource{},
			InstrumentationLibraryMetrics: []*otlpmetrics.InstrumentationLibraryMetrics{
				{
					InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{},
					Metrics: []*otlpmetrics.Metric{
						{
							Name: "cpu_temp",
							Data: &otlpmetrics.Metric_DoubleGauge{DoubleGauge: &otlpmetrics.DoubleGauge{DataPoints: []*otlpmetrics.DoubleDataPoint{
								{
									Labels:       []*otlpcommon.StringKeyValue{{Key: "foo", Value: "bar"}},
									TimeUnixNano: 1622848686000000000,
									Value:        87.332,
								},
							}}},
						},
						{
							Name: "http_requests_total",
							Data: &otlpmetrics.Metric_DoubleSum{DoubleSum: &otlpmetrics.DoubleSum{
								AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
								IsMonotonic:            true,
								DataPoints: []*otlpmetrics.DoubleDataPoint{
									{
										Labels:       []*otlpcommon.StringKeyValue{{Key: "method", Value: "post"}, {Key: "code", Value: "200"}},
										TimeUnixNano: 1622848686000000000,
										Value:        1027,
									},
									{
										Labels:       []*otlpcommon.StringKeyValue{{Key: "method", Value: "post"}, {Key: "code", Value: "400"}},
										TimeUnixNano: 1622848686000000000,
										Value:        3,
									},
								},
							}},
						},
						{
							Name: "http_request_duration_seconds",
							Data: &otlpmetrics.Metric_DoubleHistogram{DoubleHistogram: &otlpmetrics.DoubleHistogram{
								AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
								DataPoints: []*otlpmetrics.DoubleHistogramDataPoint{
									{
										Labels:         []*otlpcommon.StringKeyValue{{Key: "region", Value: "eu"}},
										TimeUnixNano:   1622848686000000000,
										Count:          144320,
										Sum:            53423,
										ExplicitBounds: []float64{0.05, 0.1, 0.2, 0.5, 1},
										BucketCounts:   []uint64{24054, 33444, 100392, 129389, 133988, 144320},
									},
								},
							}},
						},
					},
				},
			},
		},
	}
	sortResourceMetrics(expect)

	assert.JSONEq(t, protojson.Format(expect[0]), protojson.Format(mockExporterFactory.resourceMetrics[0]))
}

func TestInflux2Otel_otelcol_nowtime(t *testing.T) {
	otelcolReceiverAddress, mockExporterFactory := setupOtelcolInfluxDBReceiver(t)

	payload := `
cpu_temp,foo=bar gauge=87.332
`

	response, err := http.Post(fmt.Sprintf("http://%s/write", otelcolReceiverAddress), "", strings.NewReader(payload))
	require.NoError(t, err)
	assert.Equal(t, 2, response.StatusCode/100)

	gotTime := time.Unix(0, int64(mockExporterFactory.resourceMetrics[0].InstrumentationLibraryMetrics[0].Metrics[0].Data.(*otlpmetrics.Metric_DoubleGauge).DoubleGauge.DataPoints[0].TimeUnixNano))
	assert.WithinDuration(t, time.Now(), gotTime, time.Second)
}

func setupOtelcolInfluxDBReceiver(t *testing.T) (string, *mockExporterFactory) {
	t.Helper()

	const otelcolConfigTemplate = `
receivers:
  influxdb:
    endpoint: ADDRESS_INFLUXDB
    metrics_schema: SCHEMA

exporters:
  mock:

extensions:
  health_check:
    endpoint: ADDRESS_HEALTH_CHECK

service:
  extensions: [health_check]
  pipelines:
    metrics:
      receivers: [influxdb]
      exporters: [mock]
`

	otelcolReceiverAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))
	otelcolHealthCheckAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))
	otelcolConfig := strings.ReplaceAll(otelcolConfigTemplate, "ADDRESS_INFLUXDB", otelcolReceiverAddress)
	otelcolConfig = strings.ReplaceAll(otelcolConfig, "SCHEMA", "telegraf-prometheus-v1")
	otelcolConfig = strings.ReplaceAll(otelcolConfig, "ADDRESS_HEALTH_CHECK", otelcolHealthCheckAddress)

	receiverFactories, err := component.MakeReceiverFactoryMap(influxdbreceiver.NewFactory())
	require.NoError(t, err)
	mockExporterFactory := new(mockExporterFactory)
	exporterFactories, err := component.MakeExporterFactoryMap(mockExporterFactory)
	require.NoError(t, err)
	extensionFactories, err := component.MakeExtensionFactoryMap(healthcheckextension.NewFactory())
	require.NoError(t, err)
	appSettings := service.AppSettings{
		Factories: component.Factories{
			Receivers:  receiverFactories,
			Exporters:  exporterFactories,
			Extensions: extensionFactories,
		},
		BuildInfo: component.BuildInfo{
			Command:     "test",
			Description: "test",
			Version:     "test",
		},
		LoggingOptions: []zap.Option{
			zap.ErrorOutput(&testingLogger{t}),
			zap.IncreaseLevel(zap.WarnLevel),
		},
		ParserProvider: parserprovider.NewInMemory(strings.NewReader(otelcolConfig)),
	}
	otelcol, err := service.New(appSettings)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		err := otelcol.Run()
		assert.NoError(t, err)
		close(done)
	}()
	t.Cleanup(otelcol.Shutdown)

	go func() {
		select {
		case <-done:
			return
		case <-time.NewTimer(time.Second).C:
			t.Log("test timed out")
			t.Fail()
		}
	}()

	for { // Wait for health check to be green
		response, _ := http.Get(fmt.Sprintf("http://%s", otelcolHealthCheckAddress))
		if response != nil && response.StatusCode/100 == 2 {
			break
		}

		time.Sleep(10 * time.Millisecond)
		select {
		case <-done:
			return "", nil
		default:
		}
	}

	return otelcolReceiverAddress, mockExporterFactory
}

type testingLogger struct {
	tb testing.TB
}

func (w *testingLogger) Write(p []byte) (int, error) {
	w.tb.Logf("%s", bytes.TrimSpace(p))
	return len(p), nil
}

func (w testingLogger) Sync() error {
	return nil
}

var _ component.ExporterFactory = (*mockExporterFactory)(nil)

type mockExporterFactory struct {
	*mockMetricsExporter
}

func (m mockExporterFactory) Type() config.Type {
	return "mock"
}

type mockExporterConfig struct {
	config.ExporterSettings `mapstructure:",squash"`
}

func (m mockExporterFactory) CreateDefaultConfig() config.Exporter {
	return &mockExporterConfig{
		ExporterSettings: config.NewExporterSettings(config.NewID("mock")),
	}
}

func (m *mockExporterFactory) CreateMetricsExporter(ctx context.Context, params component.ExporterCreateSettings, cfg config.Exporter) (component.MetricsExporter, error) {
	if m.mockMetricsExporter == nil {
		m.mockMetricsExporter = new(mockMetricsExporter)
	}
	return m.mockMetricsExporter, nil
}

func (m mockExporterFactory) CreateLogsExporter(ctx context.Context, params component.ExporterCreateSettings, cfg config.Exporter) (component.LogsExporter, error) {
	panic("not implemented")
}

func (m mockExporterFactory) CreateTracesExporter(ctx context.Context, params component.ExporterCreateSettings, cfg config.Exporter) (component.TracesExporter, error) {
	panic("not implemented")
}

var _ component.MetricsExporter = (*mockMetricsExporter)(nil)

type mockMetricsExporter struct {
	resourceMetrics []*otlpmetrics.ResourceMetrics
}

func (m mockMetricsExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (m mockMetricsExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (m mockMetricsExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: false,
	}
}

func (m *mockMetricsExporter) ConsumeMetrics(ctx context.Context, md pdata.Metrics) error {
	b, err := md.ToOtlpProtoBytes()
	if err != nil {
		return err
	}
	var req otlpcollectormetrics.ExportMetricsServiceRequest
	err = proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	m.resourceMetrics = append(m.resourceMetrics, req.ResourceMetrics...)

	return nil
}
