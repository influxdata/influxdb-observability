package tests

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/converter/expandconverter"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/extension"
	"go.opentelemetry.io/collector/otelcol"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/processor"
	"go.opentelemetry.io/collector/receiver"
	"go.uber.org/zap"
)

func setupOtelcolInfluxDBExporter(t *testing.T) (*httptest.Server, *mockReceiverFactory, func(*testing.T)) {
	t.Helper()

	const otelcolConfigTemplate = `
receivers:
  mock:

exporters:
  influxdb:
    endpoint: ENDPOINT_DESTINATION
    span_dimensions: SPAN_DIMENSIONS
    metrics_schema: METRICS_SCHEMA
    org: myorg
    bucket: mybucket

extensions:
  health_check:
    endpoint: ADDRESS_HEALTH_CHECK

service:
  extensions: [health_check]
  pipelines:
    metrics:
      receivers: [mock]
      exporters: [influxdb]
    logs:
      receivers: [mock]
      exporters: [influxdb]
    traces:
      receivers: [mock]
      exporters: [influxdb]
`

	mockReceiverFactory := newMockReceiverFactory()
	mockDestination := httptest.NewServer(mockReceiverFactory)
	otelcolHealthCheckAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))

	otelcolConfigProvider := func() otelcol.ConfigProvider {
		mockDestinationEndpoint := mockDestination.URL
		configString := strings.ReplaceAll(otelcolConfigTemplate, "ENDPOINT_DESTINATION", mockDestinationEndpoint)
		configString = strings.ReplaceAll(configString, "SPAN_DIMENSIONS", "\n    - service.name\n    - span.name")
		configString = strings.ReplaceAll(configString, "METRICS_SCHEMA", "telegraf-prometheus-v1")
		configString = strings.ReplaceAll(configString, "ADDRESS_HEALTH_CHECK", otelcolHealthCheckAddress)
		t.Setenv("test-env", configString)
		configMapProvider := envprovider.New()
		configProviderSettings := otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:       []string{"env:test-env"},
				Providers:  map[string]confmap.Provider{configMapProvider.Scheme(): configMapProvider},
				Converters: []confmap.Converter{expandconverter.New()},
			},
		}
		configProvider, err := otelcol.NewConfigProvider(configProviderSettings)
		require.NoError(t, err)
		return configProvider
	}()

	receiverFactories, err := receiver.MakeFactoryMap(mockReceiverFactory)
	require.NoError(t, err)
	processorFactories, err := processor.MakeFactoryMap()
	require.NoError(t, err)
	exporterFactories, err := exporter.MakeFactoryMap(influxdbexporter.NewFactory())
	require.NoError(t, err)
	extensionFactories, err := extension.MakeFactoryMap(healthcheckextension.NewFactory())
	require.NoError(t, err)
	appSettings := otelcol.CollectorSettings{
		Factories: otelcol.Factories{
			Receivers:  receiverFactories,
			Processors: processorFactories,
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
		ConfigProvider:        otelcolConfigProvider,
		SkipSettingGRPCLogger: true,
	}
	envprovider.New()
	fileprovider.New()
	collector, err := otelcol.NewCollector(appSettings)
	require.NoError(t, err)

	done := make(chan struct{})
	var runErr error
	go func() {
		runErr = collector.Run(context.Background())
		close(done)
	}()
	t.Cleanup(collector.Shutdown)

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
			return nil, nil, nil
		default:
		}
	}

	return mockDestination, mockReceiverFactory, func(t *testing.T) { collector.Shutdown(); <-done; assert.NoError(t, runErr) }
}

var (
	_ receiver.Factory = (*mockReceiverFactory)(nil)
	_ http.Handler     = (*mockReceiverFactory)(nil)
)

type mockReceiverFactory struct {
	receiver.Factory
	nextMetricsConsumer consumer.Metrics
	nextTracesConsumer  consumer.Traces
	nextLogsConsumer    consumer.Logs
	payloads            chan []byte
}

func newMockReceiverFactory() *mockReceiverFactory {
	return &mockReceiverFactory{
		payloads: make(chan []byte, 10),
	}
}

func (m *mockReceiverFactory) Type() component.Type {
	return "mock"
}

func (m *mockReceiverFactory) TracesReceiverStability() component.StabilityLevel {
	return component.StabilityLevelDevelopment
}

func (m *mockReceiverFactory) MetricsReceiverStability() component.StabilityLevel {
	return component.StabilityLevelDevelopment
}

func (m *mockReceiverFactory) LogsReceiverStability() component.StabilityLevel {
	return component.StabilityLevelDevelopment
}

func (m *mockReceiverFactory) CreateDefaultConfig() component.Config {
	return &struct{}{}
}

func (m *mockReceiverFactory) CreateMetricsReceiver(ctx context.Context, params receiver.CreateSettings, cfg component.Config, nextConsumer consumer.Metrics) (receiver.Metrics, error) {
	if m.nextMetricsConsumer == nil {
		m.nextMetricsConsumer = nextConsumer
	} else if m.nextMetricsConsumer != nextConsumer {
		return nil, errors.New("only one metrics consumer allowed in this mock")
	}
	return new(mockReceiver), nil
}

func (m *mockReceiverFactory) CreateTracesReceiver(ctx context.Context, params receiver.CreateSettings, cfg component.Config, nextConsumer consumer.Traces) (receiver.Traces, error) {
	if m.nextTracesConsumer == nil {
		m.nextTracesConsumer = nextConsumer
	} else if m.nextTracesConsumer != nextConsumer {
		return nil, errors.New("only one traces consumer allowed in this mock")
	}
	return new(mockReceiver), nil
}

func (m *mockReceiverFactory) CreateLogsReceiver(ctx context.Context, params receiver.CreateSettings, cfg component.Config, nextConsumer consumer.Logs) (receiver.Logs, error) {
	if m.nextLogsConsumer == nil {
		m.nextLogsConsumer = nextConsumer
	} else if m.nextLogsConsumer != nextConsumer {
		return nil, errors.New("only one logs consumer allowed in this mock")
	}
	return new(mockReceiver), nil
}

func (m *mockReceiverFactory) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	payload, err := io.ReadAll(request.Body)
	if err != nil {
		writer.WriteHeader(http.StatusBadRequest)
		_, err = writer.Write([]byte(err.Error()))
		if err != nil {
			panic(err)
		}
	}

	m.payloads <- payload
}

func (m *mockReceiverFactory) lineprotocol(t *testing.T) string {
	var gotBytes []byte
	select {
	case gotBytes = <-m.payloads:
	case <-time.NewTimer(time.Second).C:
		t.Log("test timed out")
		t.Fail()
		return ""
	}
	return string(gotBytes)
}

var (
	_ receiver.Metrics = (*mockReceiver)(nil)
	_ receiver.Traces  = (*mockReceiver)(nil)
	_ receiver.Logs    = (*mockReceiver)(nil)
)

type mockReceiver struct {
}

func (m mockReceiver) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (m mockReceiver) Shutdown(ctx context.Context) error {
	return nil
}

func setupOtelcolInfluxDBReceiver(t *testing.T) (string, *mockExporterFactory) {
	t.Helper()

	const otelcolConfigTemplate = `
receivers:
  influxdb:
    endpoint: ADDRESS_INFLUXDB

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

	otelcolConfigProvider := func() otelcol.ConfigProvider {
		configString := strings.ReplaceAll(otelcolConfigTemplate, "ADDRESS_INFLUXDB", otelcolReceiverAddress)
		configString = strings.ReplaceAll(configString, "ADDRESS_HEALTH_CHECK", otelcolHealthCheckAddress)
		t.Setenv("test-env", configString)
		configMapProvider := envprovider.New()
		configProviderSettings := otelcol.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:       []string{"env:test-env"},
				Providers:  map[string]confmap.Provider{configMapProvider.Scheme(): configMapProvider},
				Converters: []confmap.Converter{expandconverter.New()},
			},
		}
		configProvider, err := otelcol.NewConfigProvider(configProviderSettings)
		require.NoError(t, err)
		return configProvider
	}()

	receiverFactories, err := receiver.MakeFactoryMap(influxdbreceiver.NewFactory())
	require.NoError(t, err)
	mockExporterFactory := new(mockExporterFactory)
	exporterFactories, err := exporter.MakeFactoryMap(mockExporterFactory)
	require.NoError(t, err)
	extensionFactories, err := extension.MakeFactoryMap(healthcheckextension.NewFactory())
	require.NoError(t, err)
	appSettings := otelcol.CollectorSettings{
		Factories: otelcol.Factories{
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
		ConfigProvider: otelcolConfigProvider,
	}
	collector, err := otelcol.NewCollector(appSettings)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		err := collector.Run(context.Background())
		assert.NoError(t, err)
		close(done)
	}()
	t.Cleanup(collector.Shutdown)

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

var _ exporter.Factory = (*mockExporterFactory)(nil)

type mockExporterFactory struct {
	exporter.Factory
	*mockMetricsExporter
}

func (m *mockExporterFactory) Type() component.Type {
	return "mock"
}

func (m *mockExporterFactory) TracesExporterStability() component.StabilityLevel {
	return component.StabilityLevelDevelopment
}

func (m *mockExporterFactory) MetricsExporterStability() component.StabilityLevel {
	return component.StabilityLevelDevelopment
}

func (m *mockExporterFactory) LogsExporterStability() component.StabilityLevel {
	return component.StabilityLevelDevelopment
}

func (m *mockExporterFactory) CreateDefaultConfig() component.Config {
	return &struct{}{}
}

func (m *mockExporterFactory) CreateMetricsExporter(ctx context.Context, params exporter.CreateSettings, cfg component.Config) (exporter.Metrics, error) {
	if m.mockMetricsExporter == nil {
		m.mockMetricsExporter = &mockMetricsExporter{}
	}
	return m.mockMetricsExporter, nil
}

func (m *mockExporterFactory) CreateLogsExporter(ctx context.Context, params exporter.CreateSettings, cfg component.Config) (exporter.Logs, error) {
	panic("not implemented")
}

func (m *mockExporterFactory) CreateTracesExporter(ctx context.Context, params exporter.CreateSettings, cfg component.Config) (exporter.Traces, error) {
	panic("not implemented")
}

var _ exporter.Metrics = (*mockMetricsExporter)(nil)

type mockMetricsExporter struct {
	consumedMetrics pmetric.Metrics
}

func (m *mockMetricsExporter) Start(ctx context.Context, host component.Host) error {
	return nil
}

func (m *mockMetricsExporter) Shutdown(ctx context.Context) error {
	return nil
}

func (m *mockMetricsExporter) Capabilities() consumer.Capabilities {
	return consumer.Capabilities{
		MutatesData: false,
	}
}

func (m *mockMetricsExporter) ConsumeMetrics(ctx context.Context, md pmetric.Metrics) error {
	clone := pmetric.NewMetrics()
	md.CopyTo(clone)
	m.consumedMetrics = clone
	return nil
}
