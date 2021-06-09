package tests

import (
	"context"
	"errors"
	"fmt"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"regexp"
	"sort"
	"strings"
	"testing"
	"time"

	otlpcollectorlogs "github.com/influxdata/influxdb-observability/otlp/collector/logs/v1"
	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpcollectortrace "github.com/influxdata/influxdb-observability/otlp/collector/trace/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlplogs "github.com/influxdata/influxdb-observability/otlp/logs/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	otlptrace "github.com/influxdata/influxdb-observability/otlp/trace/v1"
	"github.com/open-telemetry/opentelemetry-collector-contrib/exporter/influxdbexporter"
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
	"google.golang.org/protobuf/proto"
)

func TestOtel2Influx_otelcol_metrics(t *testing.T) {
	telegrafService, mockReceiverFactory := setupOtelcolInfluxDBExporter(t)
	t.Cleanup(telegrafService.Close)

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
	requestBytes, err := proto.Marshal(request)
	require.NoError(t, err)
	requestPdata, err := pdata.MetricsFromOtlpProtoBytes(requestBytes)
	require.NoError(t, err)

	err = mockReceiverFactory.nextMetricsConsumer.ConsumeMetrics(context.Background(), requestPdata)
	require.NoError(t, err)

	var gotBytes []byte
	select {
	case gotBytes = <-mockReceiverFactory.payloads:
	case <-time.NewTimer(time.Second).C:
		t.Log("test timed out")
		t.Fail()
		return
	}

	expect := `
cpu_temp,foo=bar gauge=87.332 1622848686000000000
http_request_duration_seconds,region=eu count=144320,sum=53423,0.05=24054,0.1=33444,0.2=100392,0.5=129389,1=133988 1622848686000000000
http_requests_total,code=200,method=post counter=1027 1622848686000000000
http_requests_total,code=400,method=post counter=3 1622848686000000000
`

	assertLineprotocolEqual(t, expect, string(gotBytes))
}

func TestOtel2Influx_otelcol_traces(t *testing.T) {
	telegrafService, mockReceiverFactory := setupOtelcolInfluxDBExporter(t)
	t.Cleanup(telegrafService.Close)

	request := &otlpcollectortrace.ExportTraceServiceRequest{
		ResourceSpans: []*otlptrace.ResourceSpans{
			{
				Resource: &otlpresource.Resource{},
				InstrumentationLibrarySpans: []*otlptrace.InstrumentationLibrarySpans{
					{
						InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{},
						Spans: []*otlptrace.Span{
							{
								Name:              "cpu_temp",
								TraceId:           []byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1},
								SpanId:            []byte{0, 0, 0, 0, 0, 0, 0, 3},
								TraceState:        "",
								ParentSpanId:      nil,
								Kind:              otlptrace.Span_SPAN_KIND_INTERNAL,
								StartTimeUnixNano: 1622848000000000000,
								EndTimeUnixNano:   1622848100000000000,
								Attributes: []*otlpcommon.KeyValue{
									{Key: "k", Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_BoolValue{BoolValue: true}}},
								},
								DroppedAttributesCount: 7,
								Events: []*otlptrace.Span_Event{
									{
										TimeUnixNano: 1622848000000000001,
										Name:         "yay-event",
										Attributes: []*otlpcommon.KeyValue{
											{Key: "foo", Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "bar"}}},
										},
										DroppedAttributesCount: 5,
									},
								},
								DroppedEventsCount: 13,
								Links: []*otlptrace.Span_Link{
									{
										TraceId:    []byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2},
										SpanId:     []byte{0, 0, 0, 0, 0, 0, 0, 3},
										TraceState: "",
										Attributes: []*otlpcommon.KeyValue{
											{Key: "yay-link", Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_IntValue{IntValue: 123}}},
										},
										DroppedAttributesCount: 19,
									},
								},
								DroppedLinksCount: 17,
								Status:            nil,
							},
							{
								Name:                   "http_request",
								TraceId:                []byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1},
								SpanId:                 []byte{0, 0, 0, 0, 0, 0, 0, 4},
								TraceState:             "",
								ParentSpanId:           []byte{0, 0, 0, 0, 0, 0, 0, 3},
								Kind:                   otlptrace.Span_SPAN_KIND_CLIENT,
								StartTimeUnixNano:      1622848000000000002,
								EndTimeUnixNano:        1622848000000000005,
								Attributes:             nil,
								DroppedAttributesCount: 0,
								Events:                 nil,
								DroppedEventsCount:     0,
								Links:                  nil,
								DroppedLinksCount:      0,
								Status:                 nil,
							},
							{
								Name:                   "process_batch",
								TraceId:                []byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2},
								SpanId:                 []byte{0, 0, 0, 0, 0, 0, 0, 5},
								TraceState:             "",
								ParentSpanId:           nil,
								Kind:                   otlptrace.Span_SPAN_KIND_CONSUMER,
								StartTimeUnixNano:      1622848000000000010,
								EndTimeUnixNano:        1622848000000000012,
								Attributes:             nil,
								DroppedAttributesCount: 0,
								Events:                 nil,
								DroppedEventsCount:     0,
								Links:                  nil,
								DroppedLinksCount:      0,
								Status:                 nil,
							},
						},
					},
				},
			},
		},
	}
	requestBytes, err := proto.Marshal(request)
	require.NoError(t, err)
	requestPdata, err := pdata.TracesFromOtlpProtoBytes(requestBytes)
	require.NoError(t, err)

	err = mockReceiverFactory.nextTracesConsumer.ConsumeTraces(context.Background(), requestPdata)
	require.NoError(t, err)

	var gotBytes []byte
	select {
	case gotBytes = <-mockReceiverFactory.payloads:
	case <-time.NewTimer(time.Second).C:
		t.Log("test timed out")
		t.Fail()
		return
	}

	expect := `
spans,kind=SPAN_KIND_INTERNAL,name=cpu_temp,span_id=0000000000000003,trace_id=00000000000000020000000000000001 duration_nano=100000000000i,end_time_unix_nano=1622848100000000000i,k=true,otel.span.dropped_attributes_count=7u,otel.span.dropped_events_count=13u,otel.span.dropped_links_count=17u 1622848000000000000
logs,name=yay-event,span_id=0000000000000003,trace_id=00000000000000020000000000000001 foo="bar",otel.event.dropped_attributes_count=5u 1622848000000000001
span-links,linked_span_id=0000000000000003,linked_trace_id=00000000000000020000000000000002,span_id=0000000000000003,trace_id=00000000000000020000000000000001 otel.link.dropped_attributes_count=19u,yay-link=123i 1622848000000000000
spans,kind=SPAN_KIND_CLIENT,name=http_request,parent_span_id=0000000000000003,span_id=0000000000000004,trace_id=00000000000000020000000000000001 duration_nano=3i,end_time_unix_nano=1622848000000000005i 1622848000000000002
spans,kind=SPAN_KIND_CONSUMER,name=process_batch,span_id=0000000000000005,trace_id=00000000000000020000000000000002 duration_nano=2i,end_time_unix_nano=1622848000000000012i 1622848000000000010
`

	assertLineprotocolEqual(t, expect, string(gotBytes))
}

func TestOtel2Influx_otelcol_logs(t *testing.T) {
	telegrafService, mockReceiverFactory := setupOtelcolInfluxDBExporter(t)
	t.Cleanup(telegrafService.Close)

	request := &otlpcollectorlogs.ExportLogsServiceRequest{
		ResourceLogs: []*otlplogs.ResourceLogs{
			{
				Resource: &otlpresource.Resource{},
				InstrumentationLibraryLogs: []*otlplogs.InstrumentationLibraryLogs{
					{
						InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{},
						Logs: []*otlplogs.LogRecord{
							{
								Name:           "cpu_temp",
								TimeUnixNano:   1622848686000000000,
								SeverityNumber: otlplogs.SeverityNumber_SEVERITY_NUMBER_INFO,
								SeverityText:   "info",
								Body:           &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: "something-happened"}},
								Attributes: []*otlpcommon.KeyValue{
									{Key: "k", Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_BoolValue{BoolValue: true}}},
								},
								DroppedAttributesCount: 5,
								Flags:                  0,
								TraceId:                []byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1},
								SpanId:                 []byte{0, 0, 0, 0, 0, 0, 0, 3},
							},
						},
					},
				},
			},
		},
	}
	requestBytes, err := proto.Marshal(request)
	require.NoError(t, err)
	requestPdata, err := pdata.LogsFromOtlpProtoBytes(requestBytes)
	require.NoError(t, err)

	err = mockReceiverFactory.nextLogsConsumer.ConsumeLogs(context.Background(), requestPdata)
	require.NoError(t, err)

	got := mockReceiverFactory.lineprotocol(t)

	expect := `
logs,span_id=0000000000000003,trace_id=00000000000000020000000000000001 body="something-happened",k=true,otel.span.dropped_attributes_count=5u,name="cpu_temp",severity_number=9i,severity_text="info" 1622848686000000000
`

	assertLineprotocolEqual(t, expect, got)
}

func assertLineprotocolEqual(t *testing.T, expect, got string) bool {
	t.Helper()
	return assert.Equal(t, cleanupLP(expect), cleanupLP(got))
}

func cleanupLP(s string) []string {
	lines := strings.Split(s, "\n")
	var cleanLines []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		cleanLines = append(cleanLines, sortFields(line))
	}
	sort.Strings(cleanLines)
	return cleanLines
}

func sortFields(line string) string {
	fieldsIndexes := regexp.MustCompile(`^\s*(\S+)\s+(\S+)\s*(\d*)\s*$`).FindStringSubmatchIndex(line)
	if len(fieldsIndexes) != 8 {
		panic(fmt.Sprint(len(fieldsIndexes), line))
	}
	fieldsSlice := strings.Split(line[fieldsIndexes[4]:fieldsIndexes[5]], ",")
	sort.Strings(fieldsSlice)
	return line[fieldsIndexes[2]:fieldsIndexes[3]] + " " + strings.Join(fieldsSlice, ",") + " " + line[fieldsIndexes[6]:fieldsIndexes[7]]
}

func setupOtelcolInfluxDBExporter(t *testing.T) (*httptest.Server, *mockReceiverFactory) {
	t.Helper()

	const otelcolConfigTemplate = `
receivers:
  mock:

exporters:
  influxdb:
    endpoint: ENDPOINT_TELEGRAF
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
	mockTelegrafService := httptest.NewServer(mockReceiverFactory)
	mockTelegrafEndpoint := mockTelegrafService.URL
	otelcolHealthCheckAddress := fmt.Sprintf("127.0.0.1:%d", findOpenTCPPort(t))
	otelcolConfig := strings.ReplaceAll(otelcolConfigTemplate, "ENDPOINT_TELEGRAF", mockTelegrafEndpoint)
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

	return mockTelegrafService, mockReceiverFactory
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
