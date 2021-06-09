package tests

import (
	"context"
	"fmt"
	"net/http"
	"sort"
	"testing"
	"time"

	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
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

func TestOtel2Influx_telegraf(t *testing.T) {
	otelReceiverAddress, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)

	request := &otlpcollectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: []*otlpmetrics.ResourceMetrics{
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
		},
	}
	cc, err := grpc.Dial(otelReceiverAddress, grpc.WithInsecure())
	require.NoError(t, err)
	client := otlpcollectormetrics.NewMetricsServiceClient(cc)

	_, err = client.Export(context.Background(), request)
	require.NoError(t, err)

	stopTelegraf() // flush telegraf buffers
	got := mockOutputPlugin.lineprotocol(t)

	expect := `
cpu_temp,foo=bar gauge=87.332 1622848686000000000
http_request_duration_seconds,region=eu count=144320,sum=53423,0.05=24054,0.1=33444,0.2=100392,0.5=129389,1=133988 1622848686000000000
http_requests_total,code=200,method=post counter=1027 1622848686000000000
http_requests_total,code=400,method=post counter=3 1622848686000000000
`

	assertLineprotocolEqual(t, expect, got)
}

func setupTelegrafOpenTelemetryInput(t *testing.T) (string, *mockOutputPlugin, context.CancelFunc) {
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
			return "", nil, nil
		default:
		}
	}

	return otelInputAddress, mockOutputPlugin, stopAgent
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
