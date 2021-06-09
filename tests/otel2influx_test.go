package tests

import (
	"context"
	"fmt"
	"regexp"
	"sort"
	"strings"
	"testing"

	otlpcollectorlogs "github.com/influxdata/influxdb-observability/otlp/collector/logs/v1"
	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpcollectortrace "github.com/influxdata/influxdb-observability/otlp/collector/trace/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlplogs "github.com/influxdata/influxdb-observability/otlp/logs/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	otlptrace "github.com/influxdata/influxdb-observability/otlp/trace/v1"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/consumer/pdata"
	"google.golang.org/protobuf/proto"
)

var metricTests = []struct {
	metrics  []*otlpmetrics.ResourceMetrics
	expectLP string
}{
	{
		metrics: []*otlpmetrics.ResourceMetrics{
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
		expectLP: `
cpu_temp,foo=bar gauge=87.332 1622848686000000000
http_request_duration_seconds,region=eu count=144320,sum=53423,0.05=24054,0.1=33444,0.2=100392,0.5=129389,1=133988 1622848686000000000
http_requests_total,code=200,method=post counter=1027 1622848686000000000
http_requests_total,code=400,method=post counter=3 1622848686000000000
`,
	},
}

var traceTests = []struct {
	spans    []*otlptrace.ResourceSpans
	expectLP string
}{
	{
		spans: []*otlptrace.ResourceSpans{
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
		expectLP: `
spans,kind=SPAN_KIND_INTERNAL,name=cpu_temp,span_id=0000000000000003,trace_id=00000000000000020000000000000001 duration_nano=100000000000i,end_time_unix_nano=1622848100000000000i,k=true,otel.span.dropped_attributes_count=7u,otel.span.dropped_events_count=13u,otel.span.dropped_links_count=17u 1622848000000000000
logs,name=yay-event,span_id=0000000000000003,trace_id=00000000000000020000000000000001 foo="bar",otel.event.dropped_attributes_count=5u 1622848000000000001
span-links,linked_span_id=0000000000000003,linked_trace_id=00000000000000020000000000000002,span_id=0000000000000003,trace_id=00000000000000020000000000000001 otel.link.dropped_attributes_count=19u,yay-link=123i 1622848000000000000
spans,kind=SPAN_KIND_CLIENT,name=http_request,parent_span_id=0000000000000003,span_id=0000000000000004,trace_id=00000000000000020000000000000001 duration_nano=3i,end_time_unix_nano=1622848000000000005i 1622848000000000002
spans,kind=SPAN_KIND_CONSUMER,name=process_batch,span_id=0000000000000005,trace_id=00000000000000020000000000000002 duration_nano=2i,end_time_unix_nano=1622848000000000012i 1622848000000000010
`,
	},
}

var logTests = []struct {
	logRecords []*otlplogs.ResourceLogs
	expectLP   string
}{
	{
		logRecords: []*otlplogs.ResourceLogs{
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
		expectLP: `
logs,span_id=0000000000000003,trace_id=00000000000000020000000000000001 body="something-happened",k=true,otel.span.dropped_attributes_count=5u,name="cpu_temp",severity_number=9i,severity_text="info" 1622848686000000000
`,
	},
}

func TestOtel2Influx(t *testing.T) {
	t.Run("metrics", func(t *testing.T) {
		for i, mt := range metricTests {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Run("otelcol", func(t *testing.T) {
					mockDestination, mockReceiverFactory := setupOtelcolInfluxDBExporter(t)
					t.Cleanup(mockDestination.Close)

					request := &otlpcollectormetrics.ExportMetricsServiceRequest{
						ResourceMetrics: mt.metrics,
					}
					requestBytes, err := proto.Marshal(request)
					require.NoError(t, err)
					requestPdata, err := pdata.MetricsFromOtlpProtoBytes(requestBytes)
					require.NoError(t, err)

					err = mockReceiverFactory.nextMetricsConsumer.ConsumeMetrics(context.Background(), requestPdata)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, mt.expectLP, got)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)

					request := &otlpcollectormetrics.ExportMetricsServiceRequest{
						ResourceMetrics: mt.metrics,
					}

					client := otlpcollectormetrics.NewMetricsServiceClient(clientConn)
					_, err := client.Export(context.Background(), request)
					require.NoError(t, err)

					stopTelegraf() // flush telegraf buffers
					got := mockOutputPlugin.lineprotocol(t)

					assertLineprotocolEqual(t, mt.expectLP, got)
				})
			})
		}
	})

	t.Run("traces", func(t *testing.T) {
		for i, tt := range traceTests {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Run("otelcol", func(t *testing.T) {
					mockDestination, mockReceiverFactory := setupOtelcolInfluxDBExporter(t)
					t.Cleanup(mockDestination.Close)

					request := &otlpcollectortrace.ExportTraceServiceRequest{
						ResourceSpans: tt.spans,
					}
					requestBytes, err := proto.Marshal(request)
					require.NoError(t, err)
					requestPdata, err := pdata.TracesFromOtlpProtoBytes(requestBytes)
					require.NoError(t, err)

					err = mockReceiverFactory.nextTracesConsumer.ConsumeTraces(context.Background(), requestPdata)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, tt.expectLP, got)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)

					request := &otlpcollectortrace.ExportTraceServiceRequest{
						ResourceSpans: tt.spans,
					}

					client := otlpcollectortrace.NewTraceServiceClient(clientConn)
					_, err := client.Export(context.Background(), request)
					require.NoError(t, err)

					stopTelegraf() // flush telegraf buffers
					got := mockOutputPlugin.lineprotocol(t)

					assertLineprotocolEqual(t, tt.expectLP, got)
				})
			})
		}
	})

	t.Run("logs", func(t *testing.T) {
		for i, lt := range logTests {
			t.Run(fmt.Sprint(i), func(t *testing.T) {
				t.Run("otelcol", func(t *testing.T) {
					mockDestination, mockReceiverFactory := setupOtelcolInfluxDBExporter(t)
					t.Cleanup(mockDestination.Close)

					request := &otlpcollectorlogs.ExportLogsServiceRequest{
						ResourceLogs: lt.logRecords,
					}
					requestBytes, err := proto.Marshal(request)
					require.NoError(t, err)
					requestPdata, err := pdata.LogsFromOtlpProtoBytes(requestBytes)
					require.NoError(t, err)

					err = mockReceiverFactory.nextLogsConsumer.ConsumeLogs(context.Background(), requestPdata)
					require.NoError(t, err)

					got := mockReceiverFactory.lineprotocol(t)

					assertLineprotocolEqual(t, lt.expectLP, got)
				})

				t.Run("telegraf", func(t *testing.T) {
					clientConn, mockOutputPlugin, stopTelegraf := setupTelegrafOpenTelemetryInput(t)

					request := &otlpcollectorlogs.ExportLogsServiceRequest{
						ResourceLogs: lt.logRecords,
					}

					client := otlpcollectorlogs.NewLogsServiceClient(clientConn)
					_, err := client.Export(context.Background(), request)
					require.NoError(t, err)

					stopTelegraf() // flush telegraf buffers
					got := mockOutputPlugin.lineprotocol(t)

					assertLineprotocolEqual(t, lt.expectLP, got)
				})
			})
		}
	})
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
