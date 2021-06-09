package tests

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
	"github.com/influxdata/telegraf"
	"github.com/influxdata/telegraf/agent"
	"github.com/influxdata/telegraf/config"
	"github.com/influxdata/telegraf/models"
	"github.com/influxdata/telegraf/plugins/inputs/opentelemetry"
	"github.com/influxdata/telegraf/plugins/outputs/health"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.uber.org/zap/zapcore"
	"go.uber.org/zap/zaptest"
	"google.golang.org/grpc"
)

func setupTelegrafOpenTelemetryInput(t *testing.T) (*grpc.ClientConn, *mockOutputPlugin, context.CancelFunc) {
	t.Helper()

	telegrafConfig := config.NewConfig()

	otelInputAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))
	inputPlugin := &opentelemetry.OpenTelemetry{
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
