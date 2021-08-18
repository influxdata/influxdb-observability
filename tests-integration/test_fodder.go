package tests

import (
	"go.opentelemetry.io/collector/model/pdata"
)

var (
	metricTests []struct {
		otel pdata.Metrics
		lp   string
	}

	traceTests []struct {
		otel pdata.Traces
		lp   string
	}

	logTests []struct {
		otel pdata.Logs
		lp   string
	}
)

func init() {
	{
		metrics := pdata.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		ilMetrics := rm.InstrumentationLibraryMetrics().AppendEmpty()
		m := ilMetrics.Metrics().AppendEmpty()
		m.SetName("cpu_temp")
		m.SetDataType(pdata.MetricDataTypeGauge)
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.Attributes().InsertString("foo", "bar")
		dp.SetTimestamp(pdata.Timestamp(1622848686000000000))
		dp.SetDoubleVal(87.332)
		m = ilMetrics.Metrics().AppendEmpty()
		m.SetName("http_request_duration_seconds")
		m.SetDataType(pdata.MetricDataTypeHistogram)
		m.Histogram().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
		dp2 := m.Histogram().DataPoints().AppendEmpty()
		dp2.Attributes().InsertString("region", "eu")
		dp2.SetTimestamp(pdata.Timestamp(1622848686000000000))
		dp2.SetCount(144320)
		dp2.SetSum(53423)
		dp2.SetExplicitBounds([]float64{0.05, 0.1, 0.2, 0.5, 1})
		dp2.SetBucketCounts([]uint64{24054, 33444, 100392, 129389, 133988, 144320})
		m = ilMetrics.Metrics().AppendEmpty()
		m.SetName("http_requests_total")
		m.SetDataType(pdata.MetricDataTypeSum)
		m.Sum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
		m.Sum().SetIsMonotonic(true)
		dp = m.Sum().DataPoints().AppendEmpty()
		dp.Attributes().InsertString("method", "post")
		dp.Attributes().InsertString("code", "200")
		dp.SetTimestamp(pdata.Timestamp(1622848686000000000))
		dp.SetDoubleVal(1027)
		dp = m.Sum().DataPoints().AppendEmpty()
		dp.Attributes().InsertString("method", "post")
		dp.Attributes().InsertString("code", "400")
		dp.SetTimestamp(pdata.Timestamp(1622848686000000000))
		dp.SetDoubleVal(3)

		metricTests = append(metricTests, struct {
			otel pdata.Metrics
			lp   string
		}{
			otel: metrics,
			lp: `
cpu_temp,foo=bar gauge=87.332 1622848686000000000
http_request_duration_seconds,region=eu count=144320,sum=53423,0.05=24054,0.1=33444,0.2=100392,0.5=129389,1=133988 1622848686000000000
http_requests_total,code=200,method=post counter=1027 1622848686000000000
http_requests_total,code=400,method=post counter=3 1622848686000000000
`,
		})
	}

	{
		traces := pdata.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		ilSpan := rs.InstrumentationLibrarySpans().AppendEmpty()
		span := ilSpan.Spans().AppendEmpty()
		span.SetName("cpu_temp")
		span.SetTraceID(pdata.NewTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1}))
		span.SetSpanID(pdata.NewSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 3}))
		span.SetKind(pdata.SpanKindInternal)
		span.SetStartTimestamp(pdata.Timestamp(1622848000000000000))
		span.SetEndTimestamp(pdata.Timestamp(1622848100000000000))
		span.Attributes().InsertBool("k", true)
		span.SetDroppedAttributesCount(7)
		event := span.Events().AppendEmpty()
		event.SetName("yay-event")
		event.SetTimestamp(pdata.Timestamp(1622848000000000001))
		event.Attributes().InsertString("foo", "bar")
		event.SetDroppedAttributesCount(5)
		span.SetDroppedEventsCount(13)
		link := span.Links().AppendEmpty()
		link.SetTraceID(pdata.NewTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2}))
		link.SetSpanID(pdata.NewSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 3}))
		link.Attributes().InsertInt("yay-link", 123)
		link.SetDroppedAttributesCount(19)
		span.SetDroppedLinksCount(17)
		span = ilSpan.Spans().AppendEmpty()
		span.SetName("http_request")
		span.SetTraceID(pdata.NewTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1}))
		span.SetSpanID(pdata.NewSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 4}))
		span.SetParentSpanID(pdata.NewSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 3}))
		span.SetKind(pdata.SpanKindClient)
		span.SetStartTimestamp(pdata.Timestamp(1622848000000000002))
		span.SetEndTimestamp(pdata.Timestamp(1622848000000000005))
		span = ilSpan.Spans().AppendEmpty()
		span.SetName("process_batch")
		span.SetTraceID(pdata.NewTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2}))
		span.SetSpanID(pdata.NewSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 5}))
		span.SetKind(pdata.SpanKindConsumer)
		span.SetStartTimestamp(pdata.Timestamp(1622848000000000010))
		span.SetEndTimestamp(pdata.Timestamp(1622848000000000012))

		traceTests = append(traceTests, struct {
			otel pdata.Traces
			lp   string
		}{
			otel: traces,
			lp: `
spans,kind=SPAN_KIND_INTERNAL,name=cpu_temp,span_id=0000000000000003,trace_id=00000000000000020000000000000001 duration_nano=100000000000i,end_time_unix_nano=1622848100000000000i,k=true,otel.span.dropped_attributes_count=7u,otel.span.dropped_events_count=13u,otel.span.dropped_links_count=17u 1622848000000000000
logs,name=yay-event,span_id=0000000000000003,trace_id=00000000000000020000000000000001 foo="bar",otel.event.dropped_attributes_count=5u 1622848000000000001
span-links,linked_span_id=0000000000000003,linked_trace_id=00000000000000020000000000000002,span_id=0000000000000003,trace_id=00000000000000020000000000000001 otel.link.dropped_attributes_count=19u,yay-link=123i 1622848000000000000
spans,kind=SPAN_KIND_CLIENT,name=http_request,parent_span_id=0000000000000003,span_id=0000000000000004,trace_id=00000000000000020000000000000001 duration_nano=3i,end_time_unix_nano=1622848000000000005i 1622848000000000002
spans,kind=SPAN_KIND_CONSUMER,name=process_batch,span_id=0000000000000005,trace_id=00000000000000020000000000000002 duration_nano=2i,end_time_unix_nano=1622848000000000012i 1622848000000000010
`,
		})
	}

	{
		logs := pdata.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		ilLog := rl.InstrumentationLibraryLogs().AppendEmpty()
		log := ilLog.Logs().AppendEmpty()
		log.SetName("cpu_temp")
		log.SetTimestamp(pdata.Timestamp(1622848686000000000))
		log.SetSeverityNumber(pdata.SeverityNumberINFO)
		log.SetSeverityText("info")
		log.Body().SetStringVal("something-happened")
		log.Attributes().InsertBool("k", true)
		log.SetDroppedAttributesCount(5)
		log.SetTraceID(pdata.NewTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1}))
		log.SetSpanID(pdata.NewSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 3}))

		logTests = append(logTests, struct {
			otel pdata.Logs
			lp   string
		}{
			otel: logs,
			lp: `
logs,span_id=0000000000000003,trace_id=00000000000000020000000000000001 body="something-happened",k=true,otel.span.dropped_attributes_count=5u,name="cpu_temp",severity_number=9i,severity_text="info" 1622848686000000000
`,
		})
	}
}
