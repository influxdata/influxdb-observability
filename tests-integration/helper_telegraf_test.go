package tests

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/agent"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/models"
	otelinput "github.com/influxdata/telegraf/plugins/inputs/opentelemetry"
	"github.com/influxdata/telegraf/plugins/outputs/health"
	oteloutput "github.com/influxdata/telegraf/plugins/outputs/opentelemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
)

func assertOtel2InfluxTelegraf(t *testing.T, lp string, telegrafValueType telegraf.ValueType, expect *otlpcollectormetrics.ExportMetricsServiceRequest) {
	mockInputPlugin, mockOtelService, stopTelegraf := setupTelegrafOpenTelemetryOutput(t)
	t.Cleanup(stopTelegraf)

	lpdec := lineprotocol.NewDecoder(strings.NewReader(lp))
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

		m := metric.New(string(name), tags, fields, ts, telegrafValueType)
		mockInputPlugin.accumulator.AddMetric(m)
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
	common.SortResourceMetrics(expect.ResourceMetrics)
	common.SortResourceMetrics(got.ResourceMetrics)

	assertProtosEqual(t, expect, got)
}

func setupTelegrafOpenTelemetryInput(t *testing.T) (*grpc.ClientConn, *mockOutputPlugin, context.CancelFunc) {
	t.Helper()

	telegrafConfig := config.NewConfig()

	otelInputAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))
	inputPlugin := &otelinput.OpenTelemetry{
		ServiceAddress: otelInputAddress,
		Timeout:        config.Duration(time.Second),
		MetricsSchema:  "prometheus-v1",
		Log:            zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel)).Sugar(),
	}
	otelInputConfig := &models.InputConfig{
		Name: "opentelemetry",
	}
	telegrafConfig.Inputs = append(telegrafConfig.Inputs, models.NewRunningInput(inputPlugin, otelInputConfig))

	mockOutputPlugin := newMockOutputPlugin()
	mockOutputConfig := &models.OutputConfig{
		Name: "mock",
	}
	healthOutputAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))
	healthOutputPlugin := health.NewHealth()
	healthOutputPlugin.ServiceAddress = "http://" + healthOutputAddress
	healthOutputConfig := &models.OutputConfig{
		Name: "health",
	}
	telegrafConfig.Outputs = append(telegrafConfig.Outputs,
		models.NewRunningOutput(mockOutputPlugin, mockOutputConfig, 0, 0),
		models.NewRunningOutput(healthOutputPlugin, healthOutputConfig, 0, 0))

	ag, err := agent.NewAgent(telegrafConfig)
	require.NoError(t, err)
	ctx, stopAgent := context.WithCancel(context.Background())

	agentDone := make(chan struct{})
	go func(ctx context.Context) {
		err := ag.Run(ctx)
		assert.NoError(t, err)
		close(agentDone)
	}(ctx)
	t.Cleanup(stopAgent)

	go func() {
		select {
		case <-agentDone:
			return
		case <-time.NewTimer(time.Second).C:
			t.Log("test timed out")
			t.Fail()
			stopAgent()
		}
	}()

	for { // Wait for health check to be green
		response, _ := http.Get(fmt.Sprintf("http://%s", healthOutputAddress))
		if response != nil && response.StatusCode/100 == 2 {
			break
		}

		time.Sleep(10 * time.Millisecond)
		select {
		case <-agentDone:
			return nil, nil, nil
		default:
		}
	}

	clientConn, err := grpc.Dial(otelInputAddress, grpc.WithInsecure())
	require.NoError(t, err)

	return clientConn, mockOutputPlugin, stopAgent
}

var _ telegraf.Output = (*mockOutputPlugin)(nil)

type mockOutputPlugin struct {
	metrics chan []telegraf.Metric
}

func newMockOutputPlugin() *mockOutputPlugin {
	return &mockOutputPlugin{
		metrics: make(chan []telegraf.Metric, 10),
	}
}

func (m mockOutputPlugin) SampleConfig() string {
	return ""
}

func (m mockOutputPlugin) Description() string {
	return ""
}

func (m mockOutputPlugin) Connect() error {
	return nil
}

func (m mockOutputPlugin) Close() error {
	return nil
}

func (m *mockOutputPlugin) Write(metrics []telegraf.Metric) error {
	m.metrics <- metrics
	return nil
}

func (m *mockOutputPlugin) lineprotocol(t *testing.T) string {
	t.Helper()

	encoder := new(lineprotocol.Encoder)

	select {
	case metrics := <-m.metrics:
		for _, metric := range metrics {
			encoder.StartLine(metric.Name())

			tagNames := make([]string, 0, len(metric.Tags()))
			for k := range metric.Tags() {
				tagNames = append(tagNames, k)
			}
			sort.Strings(tagNames)
			for _, k := range tagNames {
				encoder.AddTag(k, metric.Tags()[k])
			}

			fieldNames := make([]string, 0, len(metric.Fields()))
			for k := range metric.Fields() {
				fieldNames = append(fieldNames, k)
			}
			sort.Strings(fieldNames)
			for _, k := range fieldNames {
				encoder.AddField(k, lineprotocol.MustNewValue(metric.Fields()[k]))
			}

			encoder.EndLine(metric.Time())
		}
	case <-time.NewTimer(time.Second).C:
		t.Log("test timed out")
		t.Fail()
		return ""
	}

	require.NoError(t, encoder.Err())
	return string(encoder.Bytes())
}

func setupTelegrafOpenTelemetryOutput(t *testing.T) (*mockInputPlugin, *mockOtelService, context.CancelFunc) {
	t.Helper()

	telegrafConfig := config.NewConfig()

	mockInputPlugin := new(mockInputPlugin)
	mockInputConfig := &models.InputConfig{
		Name: "mock",
	}
	telegrafConfig.Inputs = append(telegrafConfig.Inputs, models.NewRunningInput(mockInputPlugin, mockInputConfig))

	otelOutputAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))
	otelOutputPlugin := &oteloutput.OpenTelemetry{
		ServiceAddress: otelOutputAddress,
		Log:            zaptest.NewLogger(t, zaptest.Level(zapcore.InfoLevel)).Sugar(),
	}
	otelOutputConfig := &models.OutputConfig{
		Name: "opentelemetry",
	}
	healthOutputAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))
	healthOutputPlugin := health.NewHealth()
	healthOutputPlugin.ServiceAddress = "http://" + healthOutputAddress
	healthOutputConfig := &models.OutputConfig{
		Name: "health",
	}
	telegrafConfig.Outputs = append(telegrafConfig.Outputs,
		models.NewRunningOutput(otelOutputPlugin, otelOutputConfig, 0, 0),
		models.NewRunningOutput(healthOutputPlugin, healthOutputConfig, 0, 0))

	ag, err := agent.NewAgent(telegrafConfig)
	require.NoError(t, err)
	ctx, stopAgent := context.WithCancel(context.Background())

	mockOtelServiceListener, err := net.Listen("tcp", otelOutputAddress)
	require.NoError(t, err)
	mockOtelService := newMockOtelService()
	mockOtelServiceGrpcServer := grpc.NewServer()
	otlpcollectormetrics.RegisterMetricsServiceServer(mockOtelServiceGrpcServer, mockOtelService)

	go func() {
		err := mockOtelServiceGrpcServer.Serve(mockOtelServiceListener)
		assert.NoError(t, err)
	}()
	t.Cleanup(mockOtelServiceGrpcServer.Stop)

	agentDone := make(chan struct{})
	go func(ctx context.Context) {
		err := ag.Run(ctx)
		assert.NoError(t, err)
		close(agentDone)
	}(ctx)
	t.Cleanup(stopAgent)

	go func() {
		select {
		case <-agentDone:
			return
		case <-time.NewTimer(time.Second).C:
			t.Log("test timed out")
			t.Fail()
			stopAgent()
		}
	}()

	for { // Wait for health check to be green
		response, _ := http.Get(fmt.Sprintf("http://%s", healthOutputAddress))
		if response != nil && response.StatusCode/100 == 2 {
			break
		}

		time.Sleep(10 * time.Millisecond)
		select {
		case <-agentDone:
			return nil, nil, nil
		default:
		}
	}

	return mockInputPlugin, mockOtelService, stopAgent
}

var _ telegraf.ServiceInput = (*mockInputPlugin)(nil)

type mockInputPlugin struct {
	accumulator telegraf.Accumulator
}

func (m *mockInputPlugin) Start(accumulator telegraf.Accumulator) error {
	m.accumulator = accumulator
	return nil
}

func (m mockInputPlugin) Stop() {
}

func (m mockInputPlugin) SampleConfig() string {
	return ""
}

func (m mockInputPlugin) Description() string {
	return ""
}

func (m mockInputPlugin) Gather(accumulator telegraf.Accumulator) error {
	return nil
}

var _ otlpcollectormetrics.MetricsServiceServer = (*mockOtelService)(nil)

type mockOtelService struct {
	otlpcollectormetrics.UnimplementedMetricsServiceServer

	metrics chan []*otlpmetrics.ResourceMetrics
}

func newMockOtelService() *mockOtelService {
	return &mockOtelService{
		metrics: make(chan []*otlpmetrics.ResourceMetrics),
	}
}

func (m *mockOtelService) Export(ctx context.Context, request *otlpcollectormetrics.ExportMetricsServiceRequest) (*otlpcollectormetrics.ExportMetricsServiceResponse, error) {
	m.metrics <- request.ResourceMetrics
	return &otlpcollectormetrics.ExportMetricsServiceResponse{}, nil
}
