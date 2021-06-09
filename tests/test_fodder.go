package tests

import (
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlplogs "github.com/influxdata/influxdb-observability/otlp/logs/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	otlptrace "github.com/influxdata/influxdb-observability/otlp/trace/v1"
)

var metricTests = []struct {
	metrics []*otlpmetrics.ResourceMetrics
	lp      string
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
		lp: `
cpu_temp,foo=bar gauge=87.332 1622848686000000000
http_request_duration_seconds,region=eu count=144320,sum=53423,0.05=24054,0.1=33444,0.2=100392,0.5=129389,1=133988 1622848686000000000
http_requests_total,code=200,method=post counter=1027 1622848686000000000
http_requests_total,code=400,method=post counter=3 1622848686000000000
`,
	},
}

var traceTests = []struct {
	spans []*otlptrace.ResourceSpans
	lp    string
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
		lp: `
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
	lp         string
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
		lp: `
logs,span_id=0000000000000003,trace_id=00000000000000020000000000000001 body="something-happened",k=true,otel.span.dropped_attributes_count=5u,name="cpu_temp",severity_number=9i,severity_text="info" 1622848686000000000
`,
	},
}
