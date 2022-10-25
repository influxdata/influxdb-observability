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

	"go.opentelemetry.io/collector/confmap"
	"go.opentelemetry.io/collector/confmap/converter/expandconverter"
	"go.opentelemetry.io/collector/confmap/provider/envprovider"
	"go.opentelemetry.io/collector/confmap/provider/fileprovider"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbexporter"
	"github.com/open-telemetry/opentelemetry-collector-contrib/extension/healthcheckextension"
	"github.com/open-telemetry/opentelemetry-collector-contrib/receiver/influxdbreceiver"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/service"
	"go.uber.org/zap"
)

func setupOtelcolInfluxDBExporter(t *testing.T) (*httptest.Server, *mockReceiverFactory) {
	t.Helper()

	const otelcolConfigTemplate = `
receivers:
  mock:

exporters:
  influxdb:
    endpoint: ENDPOINT_DESTINATION
    metrics_schema: SCHEMA
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

	otelcolConfigProvider := func() service.ConfigProvider {
		mockDestinationEndpoint := mockDestination.URL
		configString := strings.ReplaceAll(otelcolConfigTemplate, "ENDPOINT_DESTINATION", mockDestinationEndpoint)
		configString = strings.ReplaceAll(configString, "SCHEMA", "telegraf-prometheus-v1")
		configString = strings.ReplaceAll(configString, "ADDRESS_HEALTH_CHECK", otelcolHealthCheckAddress)
		t.Setenv("test-env", configString)
		configMapProvider := envprovider.New()
		configProviderSettings := service.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:       []string{"env:test-env"},
				Providers:  map[string]confmap.Provider{configMapProvider.Scheme(): configMapProvider},
				Converters: []confmap.Converter{expandconverter.New()},
			},
		}
		configProvider, err := service.NewConfigProvider(configProviderSettings)
		require.NoError(t, err)
		return configProvider
	}()

	receiverFactories, err := component.MakeReceiverFactoryMap(mockReceiverFactory)
	require.NoError(t, err)
	exporterFactories, err := component.MakeExporterFactoryMap(influxdbexporter.NewFactory())
	require.NoError(t, err)
	extensionFactories, err := component.MakeExtensionFactoryMap(healthcheckextension.NewFactory())
	require.NoError(t, err)
	appSettings := service.CollectorSettings{
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
		ConfigProvider: otelcolConfigProvider,
	}
	envprovider.New()
	fileprovider.New()
	otelcol, err := service.New(appSettings)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		err := otelcol.Run(context.Background())
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
			return nil, nil
		default:
		}
	}

	return mockDestination, mockReceiverFactory
}

var (
	_ component.ReceiverFactory = (*mockReceiverFactory)(nil)
	_ http.Handler              = (*mockReceiverFactory)(nil)
)

type mockReceiverFactory struct {
	component.ReceiverFactory
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

func (m *mockReceiverFactory) Type() config.Type {
	return "mock"
}

func (m *mockReceiverFactory) TracesReceiverStability() component.StabilityLevel {
	return component.StabilityLevelInDevelopment
}

func (m *mockReceiverFactory) MetricsReceiverStability() component.StabilityLevel {
	return component.StabilityLevelInDevelopment
}

func (m *mockReceiverFactory) LogsReceiverStability() component.StabilityLevel {
	return component.StabilityLevelInDevelopment
}

type mockReceiverConfig struct {
	config.ReceiverSettings `mapstructure:",squash"`
}

func (m *mockReceiverFactory) CreateDefaultConfig() config.Receiver {
	return &mockReceiverConfig{
		ReceiverSettings: config.NewReceiverSettings(config.NewComponentID("mock")),
	}
}

func (m *mockReceiverFactory) CreateMetricsReceiver(ctx context.Context, params component.ReceiverCreateSettings, cfg config.Receiver, nextConsumer consumer.Metrics) (component.MetricsReceiver, error) {
	if m.nextMetricsConsumer == nil {
		m.nextMetricsConsumer = nextConsumer
	} else if m.nextMetricsConsumer != nextConsumer {
		return nil, errors.New("only one metrics consumer allowed in this mock")
	}
	return new(mockReceiver), nil
}

func (m *mockReceiverFactory) CreateTracesReceiver(ctx context.Context, params component.ReceiverCreateSettings, cfg config.Receiver, nextConsumer consumer.Traces) (component.TracesReceiver, error) {
	if m.nextTracesConsumer == nil {
		m.nextTracesConsumer = nextConsumer
	} else if m.nextTracesConsumer != nextConsumer {
		return nil, errors.New("only one traces consumer allowed in this mock")
	}
	return new(mockReceiver), nil
}

func (m *mockReceiverFactory) CreateLogsReceiver(ctx context.Context, params component.ReceiverCreateSettings, cfg config.Receiver, nextConsumer consumer.Logs) (component.LogsReceiver, error) {
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
	_ component.MetricsReceiver = (*mockReceiver)(nil)
	_ component.TracesReceiver  = (*mockReceiver)(nil)
	_ component.LogsReceiver    = (*mockReceiver)(nil)
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

	otelcolConfigProvider := func() service.ConfigProvider {
		configString := strings.ReplaceAll(otelcolConfigTemplate, "ADDRESS_INFLUXDB", otelcolReceiverAddress)
		configString = strings.ReplaceAll(configString, "ADDRESS_HEALTH_CHECK", otelcolHealthCheckAddress)
		t.Setenv("test-env", configString)
		configMapProvider := envprovider.New()
		configProviderSettings := service.ConfigProviderSettings{
			ResolverSettings: confmap.ResolverSettings{
				URIs:       []string{"env:test-env"},
				Providers:  map[string]confmap.Provider{configMapProvider.Scheme(): configMapProvider},
				Converters: []confmap.Converter{expandconverter.New()},
			},
		}
		configProvider, err := service.NewConfigProvider(configProviderSettings)
		require.NoError(t, err)
		return configProvider
	}()

	receiverFactories, err := component.MakeReceiverFactoryMap(influxdbreceiver.NewFactory())
	require.NoError(t, err)
	mockExporterFactory := new(mockExporterFactory)
	exporterFactories, err := component.MakeExporterFactoryMap(mockExporterFactory)
	require.NoError(t, err)
	extensionFactories, err := component.MakeExtensionFactoryMap(healthcheckextension.NewFactory())
	require.NoError(t, err)
	appSettings := service.CollectorSettings{
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
		ConfigProvider: otelcolConfigProvider,
	}
	otelcol, err := service.New(appSettings)
	require.NoError(t, err)

	done := make(chan struct{})
	go func() {
		err := otelcol.Run(context.Background())
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
	component.ExporterFactory
	*mockMetricsExporter
}

func (m *mockExporterFactory) Type() config.Type {
	return "mock"
}

func (m *mockExporterFactory) TracesExporterStability() component.StabilityLevel {
	return component.StabilityLevelInDevelopment
}

func (m *mockExporterFactory) MetricsExporterStability() component.StabilityLevel {
	return component.StabilityLevelInDevelopment
}

func (m *mockExporterFactory) LogsExporterStability() component.StabilityLevel {
	return component.StabilityLevelInDevelopment
}

type mockExporterConfig struct {
	config.ExporterSettings `mapstructure:",squash"`
}

func (m *mockExporterFactory) CreateDefaultConfig() config.Exporter {
	return &mockExporterConfig{
		ExporterSettings: config.NewExporterSettings(config.NewComponentID("mock")),
	}
}

func (m *mockExporterFactory) CreateMetricsExporter(ctx context.Context, params component.ExporterCreateSettings, cfg config.Exporter) (component.MetricsExporter, error) {
	if m.mockMetricsExporter == nil {
		m.mockMetricsExporter = &mockMetricsExporter{}
	}
	return m.mockMetricsExporter, nil
}

func (m *mockExporterFactory) CreateLogsExporter(ctx context.Context, params component.ExporterCreateSettings, cfg config.Exporter) (component.LogsExporter, error) {
	panic("not implemented")
}

func (m *mockExporterFactory) CreateTracesExporter(ctx context.Context, params component.ExporterCreateSettings, cfg config.Exporter) (component.TracesExporter, error) {
	panic("not implemented")
}

var _ component.MetricsExporter = (*mockMetricsExporter)(nil)

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
