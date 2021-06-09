package tests

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbexporter"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/extension/healthcheckextension"
	"go.opentelemetry.io/collector/service"
	"go.opentelemetry.io/collector/service/parserprovider"
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
	mockDestinationEndpoint := mockDestination.URL
	otelcolHealthCheckAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))
	otelcolConfig := strings.ReplaceAll(otelcolConfigTemplate, "ENDPOINT_DESTINATION", mockDestinationEndpoint)
	otelcolConfig = strings.ReplaceAll(otelcolConfig, "SCHEMA", "telegraf-prometheus-v1")
	otelcolConfig = strings.ReplaceAll(otelcolConfig, "ADDRESS_HEALTH_CHECK", otelcolHealthCheckAddress)

	receiverFactories, err := component.MakeReceiverFactoryMap(mockReceiverFactory)
	require.NoError(t, err)
	exporterFactories, err := component.MakeExporterFactoryMap(influxdbexporter.NewFactory())
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

func (m mockReceiverFactory) Type() config.Type {
	return "mock"
}

type mockReceiverConfig struct {
	config.ReceiverSettings `mapstructure:",squash"`
}

func (m mockReceiverFactory) CreateDefaultConfig() config.Receiver {
	return &mockReceiverConfig{
		ReceiverSettings: config.NewReceiverSettings(config.NewID("mock")),
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
	payload, err := ioutil.ReadAll(request.Body)
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
