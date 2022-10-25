package tests

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"sort"
	"strings"
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/pmetric/pmetricotlp"
	"google.golang.org/grpc/credentials/insecure"

	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/agent"
	"github.com/influxdata/telegraf/config"
	telegrafmetric "github.com/influxdata/telegraf/metric"
	"github.com/influxdata/telegraf/models"
	otelinput "github.com/influxdata/telegraf/plugins/inputs/opentelemetry"
	"github.com/influxdata/telegraf/plugins/outputs/health"
	oteloutput "github.com/influxdata/telegraf/plugins/outputs/opentelemetry"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"

	"github.com/influxdata/influxdb-observability/common"
)

func assertOtel2InfluxTelegraf(t *testing.T, lp string, telegrafValueType telegraf.ValueType, expect pmetric.Metrics) {
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

		m := telegrafmetric.New(string(name), tags, fields, ts, telegrafValueType)
		mockInputPlugin.accumulator.AddMetric(m)
	}
	require.NoError(t, lpdec.Err())

	stopTelegraf()

	var got pmetric.Metrics
	select {
	case got = <-mockOtelService.metricss:
	case <-time.NewTimer(time.Second).C:
		t.Log("test timed out")
		t.Fail()
		return
	}

	common.SortResourceMetrics(expect.ResourceMetrics())
	expectJSON, err := pmetric.NewJSONMarshaler().MarshalMetrics(expect)
	require.NoError(t, err)

	common.SortResourceMetrics(got.ResourceMetrics())
	gotJSON, err := pmetric.NewJSONMarshaler().MarshalMetrics(got)
	require.NoError(t, err)

	assert.JSONEq(t, string(expectJSON), string(gotJSON))
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

	clientConn, err := grpc.Dial(otelInputAddress, grpc.WithTransportCredentials(insecure.NewCredentials()))
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

func (m *mockOutputPlugin) SampleConfig() string {
	return ""
}

func (m *mockOutputPlugin) Description() string {
	return ""
}

func (m *mockOutputPlugin) Connect() error {
	return nil
}

func (m *mockOutputPlugin) Close() error {
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

	logWriterToRestore := log.Writer()
	log.SetOutput(io.Discard)
	t.Cleanup(func() {
		log.SetOutput(logWriterToRestore)
	})
	telegrafConfig := config.NewConfig()
	// telegrafConfig.Agent.Quiet = false
	// telegrafConfig.Agent.Debug = true
	// telegrafConfig.Agent.LogTarget = "file"
	// telegrafConfig.Agent.Logfile = "/dev/null"

	mockInputPlugin := new(mockInputPlugin)
	mockInputConfig := &models.InputConfig{
		Name: "mock",
	}
	telegrafConfig.Inputs = append(telegrafConfig.Inputs, models.NewRunningInput(mockInputPlugin, mockInputConfig))

	otelOutputAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))
	otelOutputPlugin := &oteloutput.OpenTelemetry{
		ServiceAddress: otelOutputAddress,
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
	t.Cleanup(stopAgent)

	mockOtelServiceListener, err := net.Listen("tcp", otelOutputAddress)
	require.NoError(t, err)
	mockOtelService := newMockOtelService()
	mockOtelServiceGrpcServer := grpc.NewServer()
	pmetricotlp.RegisterGRPCServer(mockOtelServiceGrpcServer, mockOtelService)

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

func (m *mockInputPlugin) Stop() {
}

func (m *mockInputPlugin) SampleConfig() string {
	return ""
}

func (m *mockInputPlugin) Description() string {
	return ""
}

func (m *mockInputPlugin) Gather(accumulator telegraf.Accumulator) error {
	return nil
}

var _ pmetricotlp.GRPCServer = (*mockOtelService)(nil)

type mockOtelService struct {
	metricss chan pmetric.Metrics
}

func newMockOtelService() *mockOtelService {
	return &mockOtelService{
		metricss: make(chan pmetric.Metrics),
	}
}

func (m *mockOtelService) Export(ctx context.Context, request pmetricotlp.Request) (pmetricotlp.Response, error) {
	clone := pmetric.NewMetrics()
	request.Metrics().CopyTo(clone)
	m.metricss <- clone
	return pmetricotlp.NewResponse(), nil
}
