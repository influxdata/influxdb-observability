package tests

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

var (
	metricTests []struct {
		otel pmetric.Metrics
		lp   string
	}

	traceTests []struct {
		otel ptrace.Traces
		lp   string
	}

	logTests []struct {
		otel plog.Logs
		lp   string
	}
)

func init() {
	{
		metrics := pmetric.NewMetrics()
		rm := metrics.ResourceMetrics().AppendEmpty()
		ilMetrics := rm.ScopeMetrics().AppendEmpty()
		m := ilMetrics.Metrics().AppendEmpty()
		m.SetName("cpu_temp")
		m.SetEmptyGauge()
		dp := m.Gauge().DataPoints().AppendEmpty()
		dp.Attributes().PutStr("foo", "bar")
		dp.SetTimestamp(pcommon.Timestamp(1622848686000000000))
		dp.SetDoubleValue(87.332)
		m = ilMetrics.Metrics().AppendEmpty()
		m.SetName("http_request_duration_seconds")
		m.SetEmptyHistogram()
		m.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		dp2 := m.Histogram().DataPoints().AppendEmpty()
		dp2.Attributes().PutStr("region", "eu")
		dp2.SetTimestamp(pcommon.Timestamp(1622848686000000000))
		dp2.SetCount(144320)
		dp2.SetSum(53423)
		dp2.ExplicitBounds().FromRaw([]float64{0.05, 0.1, 0.2, 0.5, 1})
		dp2.BucketCounts().FromRaw([]uint64{24054, 9390, 66948, 28997, 4599, 10332})
		m = ilMetrics.Metrics().AppendEmpty()
		m.SetName("http_requests_total")
		m.SetEmptySum()
		m.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		m.Sum().SetIsMonotonic(true)
		dp = m.Sum().DataPoints().AppendEmpty()
		dp.Attributes().PutStr("method", "post")
		dp.Attributes().PutStr("code", "200")
		dp.SetTimestamp(pcommon.Timestamp(1622848686000000000))
		dp.SetDoubleValue(1027)
		dp = m.Sum().DataPoints().AppendEmpty()
		dp.Attributes().PutStr("method", "post")
		dp.Attributes().PutStr("code", "400")
		dp.SetTimestamp(pcommon.Timestamp(1622848686000000000))
		dp.SetDoubleValue(3)

		metricTests = append(metricTests, struct {
			otel pmetric.Metrics
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
		traces := ptrace.NewTraces()
		rs := traces.ResourceSpans().AppendEmpty()
		ilSpan := rs.ScopeSpans().AppendEmpty()
		span := ilSpan.Spans().AppendEmpty()
		span.SetName("cpu_temp")
		span.SetTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1})
		span.SetSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 3})
		span.SetKind(ptrace.SpanKindInternal)
		span.SetStartTimestamp(pcommon.Timestamp(1622848000000000000))
		span.SetEndTimestamp(pcommon.Timestamp(1622848100000000000))
		span.Attributes().PutBool("k", true)
		span.SetDroppedAttributesCount(7)
		event := span.Events().AppendEmpty()
		event.SetName("yay-event")
		event.SetTimestamp(pcommon.Timestamp(1622848000000000001))
		event.Attributes().PutStr("foo", "bar")
		event.SetDroppedAttributesCount(5)
		span.SetDroppedEventsCount(13)
		link := span.Links().AppendEmpty()
		link.SetTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2})
		link.SetSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 3})
		link.Attributes().PutInt("yay-link", 123)
		link.SetDroppedAttributesCount(19)
		span.SetDroppedLinksCount(17)
		span = ilSpan.Spans().AppendEmpty()
		span.SetName("http_request")
		span.SetTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1})
		span.SetSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 4})
		span.SetParentSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 3})
		span.SetKind(ptrace.SpanKindClient)
		span.SetStartTimestamp(pcommon.Timestamp(1622848000000000002))
		span.SetEndTimestamp(pcommon.Timestamp(1622848000000000005))
		span = ilSpan.Spans().AppendEmpty()
		span.SetName("process_batch")
		span.SetTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 2})
		span.SetSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 5})
		span.SetKind(ptrace.SpanKindConsumer)
		span.SetStartTimestamp(pcommon.Timestamp(1622848000000000010))
		span.SetEndTimestamp(pcommon.Timestamp(1622848000000000012))

		traceTests = append(traceTests, struct {
			otel ptrace.Traces
			lp   string
		}{
			otel: traces,
			lp: `
spans,span_id=0000000000000003,trace_id=00000000000000020000000000000001 duration_nano=100000000000i,end_time_unix_nano=1622848100000000000i,span.kind="Internal",attributes="{\"k\":true}",dropped_attributes_count=7u,dropped_events_count=13u,dropped_links_count=17u,span.name="cpu_temp" 1622848000000000000
logs,span_id=0000000000000003,trace_id=00000000000000020000000000000001 attributes="{\"foo\":\"bar\"}",dropped_attributes_count=5u,event.name="yay-event" 1622848000000000001
span-links,linked_span_id=0000000000000003,linked_trace_id=00000000000000020000000000000002,span_id=0000000000000003,trace_id=00000000000000020000000000000001 dropped_attributes_count=19u,attributes="{\"yay-link\":123}" 1622848000000000000
spans,span_id=0000000000000004,trace_id=00000000000000020000000000000001 duration_nano=3i,end_time_unix_nano=1622848000000000005i,span.kind="Client",parent_span_id="0000000000000003",span.name="http_request" 1622848000000000002
spans,span_id=0000000000000005,trace_id=00000000000000020000000000000002 duration_nano=2i,end_time_unix_nano=1622848000000000012i,span.kind="Consumer",span.name="process_batch" 1622848000000000010
`,
		})
	}

	{
		logs := plog.NewLogs()
		rl := logs.ResourceLogs().AppendEmpty()
		ilLog := rl.ScopeLogs().AppendEmpty()
		log := ilLog.LogRecords().AppendEmpty()
		log.SetTimestamp(pcommon.Timestamp(1622848686000000000))
		log.SetSeverityNumber(plog.SeverityNumberInfo)
		log.SetSeverityText("info")
		log.Body().SetStr("something-happened")
		log.Attributes().PutBool("k", true)
		log.SetDroppedAttributesCount(5)
		log.SetTraceID([16]byte{0, 0, 0, 0, 0, 0, 0, 2, 0, 0, 0, 0, 0, 0, 0, 1})
		log.SetSpanID([8]byte{0, 0, 0, 0, 0, 0, 0, 3})

		logTests = append(logTests, struct {
			otel plog.Logs
			lp   string
		}{
			otel: logs,
			lp: `
logs,span_id=0000000000000003,trace_id=00000000000000020000000000000001 body="something-happened",k=true,dropped_attributes_count=5u,severity_number=9i,severity_text="info" 1622848686000000000
`,
		})
	}
}
