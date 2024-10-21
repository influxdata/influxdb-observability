package otel2influx_test

import (
	"context"
	"encoding/json"
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

func TestWriteTraces(t *testing.T) {
	//without custom trace
	writer := new(MockInfluxWriter)
	config := &otel2influx.OtelTracesToLineProtocolConfig{
		Logger: new(common.NoopLogger),
		Writer: writer,
		GlobalTrace: otel2influx.Trace{
			Table: "spantest",
			SpanDimensions: []string{
				"client_id",
				common.AttributeTraceID,
			},
			SpanFields: []string{
				"field1",
				"field3",
			},
		},
	}
	protocol, _ := otel2influx.NewOtelTracesToLineProtocol(config)
	err := protocol.WriteTraces(context.Background(), generateTraces(""))

	assert.Nil(t, err)
	point := writer.points
	assert.Equal(t, 1, len(point))
	assert.Equal(t, "spantest", point[0].measurement)
	assert.Equal(t, 2, len(point[0].tags))
	assert.Equal(t, "clientMockId", point[0].tags["client_id"])
	assert.Equal(t, pcommon.TraceID([16]byte{9, 8, 7}).String(), point[0].tags["trace_id"])
	assert.Equal(t, 9, len(point[0].fields))
	assert.Equal(t, pcommon.SpanID([8]byte{1, 2, 3}).String(), point[0].fields["span_id"])
	assert.Equal(t, "mockSpanName", point[0].fields["span.name"])
	assert.Equal(t, ptrace.SpanKindServer.String(), point[0].fields["span.kind"])
	assert.Equal(t, pcommon.SpanID([8]byte{4, 5, 6}).String(), point[0].fields["parent_span_id"])
	assert.Equal(t, time.Date(2024, 1, 1, 20, 0, 0, 10, time.UTC).UnixNano(), point[0].fields["end_time_unix_nano"])
	assert.Equal(t, time.Duration(10).Nanoseconds(), point[0].fields["duration_nano"])
	assert.Equal(t, "Ok", point[0].fields["otel.status_code"])
	assert.Equal(t, "field1Val", point[0].fields["field1"])
	attrsMap := map[string]string{}
	str, ok := point[0].fields["attributes"].(string)
	assert.True(t, ok)
	err = json.Unmarshal([]byte(str), &attrsMap)
	assert.Nil(t, err)
	assert.Equal(t, 3, len(attrsMap))
	assert.Equal(t, "field2Val", attrsMap["field2"])
	assert.Equal(t, "attr1Val", attrsMap["attr1"])
	assert.Equal(t, "attr2Val", attrsMap["attr2"])

	//custom trace overrides global
	writer = new(MockInfluxWriter)
	config = &otel2influx.OtelTracesToLineProtocolConfig{
		Logger: new(common.NoopLogger),
		Writer: writer,
		GlobalTrace: otel2influx.Trace{
			Table: "spantest",
			SpanDimensions: []string{
				common.AttributeTraceID,
			},
		},
		CustomTraces: map[string]otel2influx.Trace{"emqx": {
			Table: "emqxspan",
			SpanDimensions: []string{
				"client_id",
			},
			SpanFields: []string{
				"field2",
			},
		}},
	}
	protocol, _ = otel2influx.NewOtelTracesToLineProtocol(config)
	err = protocol.WriteTraces(context.Background(), generateTraces("emqx"))

	assert.Nil(t, err)
	point = writer.points
	assert.Equal(t, 1, len(point))
	assert.Equal(t, "emqxspan", point[0].measurement)
	assert.Equal(t, 1, len(point[0].tags))
	assert.Equal(t, "clientMockId", point[0].tags["client_id"])
	assert.Equal(t, 10, len(point[0].fields))
	assert.Equal(t, pcommon.TraceID([16]byte{9, 8, 7}).String(), point[0].fields["trace_id"])
	assert.Equal(t, pcommon.SpanID([8]byte{1, 2, 3}).String(), point[0].fields["span_id"])
	assert.Equal(t, "mockSpanName", point[0].fields["span.name"])
	assert.Equal(t, ptrace.SpanKindServer.String(), point[0].fields["span.kind"])
	assert.Equal(t, pcommon.SpanID([8]byte{4, 5, 6}).String(), point[0].fields["parent_span_id"])
	assert.Equal(t, time.Date(2024, 1, 1, 20, 0, 0, 10, time.UTC).UnixNano(), point[0].fields["end_time_unix_nano"])
	assert.Equal(t, time.Duration(10).Nanoseconds(), point[0].fields["duration_nano"])
	assert.Equal(t, "Ok", point[0].fields["otel.status_code"])
	assert.Equal(t, "field2Val", point[0].fields["field2"])
	attrsMap = map[string]string{}
	str, ok = point[0].fields["attributes"].(string)
	assert.True(t, ok)
	err = json.Unmarshal([]byte(str), &attrsMap)
	assert.Nil(t, err)
	assert.Equal(t, 4, len(attrsMap))
	assert.Equal(t, "field1Val", attrsMap["field1"])
	assert.Equal(t, "attr1Val", attrsMap["attr1"])
	assert.Equal(t, "attr2Val", attrsMap["attr2"])
	assert.Equal(t, "emqx", attrsMap["emq_service"])

	//custom trace not match
	writer = new(MockInfluxWriter)
	config = &otel2influx.OtelTracesToLineProtocolConfig{
		Logger: new(common.NoopLogger),
		Writer: writer,
		GlobalTrace: otel2influx.Trace{
			Table: "spantest",
			SpanDimensions: []string{
				common.AttributeTraceID,
			},
		},
		CustomTraces: map[string]otel2influx.Trace{"emqx": {
			Table: "emqxspan",
			SpanDimensions: []string{
				"client_id",
			},
			SpanFields: []string{
				"field2",
			},
		}},
	}
	protocol, _ = otel2influx.NewOtelTracesToLineProtocol(config)
	err = protocol.WriteTraces(context.Background(), generateTraces("ecp"))

	assert.Nil(t, err)
	point = writer.points
	assert.Equal(t, 1, len(point))
	assert.Equal(t, "spantest", point[0].measurement)
	assert.Equal(t, 1, len(point[0].tags))
	assert.Equal(t, pcommon.TraceID([16]byte{9, 8, 7}).String(), point[0].tags["trace_id"])
	assert.Equal(t, 8, len(point[0].fields))
	assert.Equal(t, pcommon.SpanID([8]byte{1, 2, 3}).String(), point[0].fields["span_id"])
	assert.Equal(t, "mockSpanName", point[0].fields["span.name"])
	assert.Equal(t, ptrace.SpanKindServer.String(), point[0].fields["span.kind"])
	assert.Equal(t, pcommon.SpanID([8]byte{4, 5, 6}).String(), point[0].fields["parent_span_id"])
	assert.Equal(t, time.Date(2024, 1, 1, 20, 0, 0, 10, time.UTC).UnixNano(), point[0].fields["end_time_unix_nano"])
	assert.Equal(t, time.Duration(10).Nanoseconds(), point[0].fields["duration_nano"])
	assert.Equal(t, "Ok", point[0].fields["otel.status_code"])
	attrsMap = map[string]string{}
	str, ok = point[0].fields["attributes"].(string)
	assert.True(t, ok)
	err = json.Unmarshal([]byte(str), &attrsMap)
	assert.Nil(t, err)
	assert.Equal(t, 6, len(attrsMap))
	assert.Equal(t, "clientMockId", attrsMap["client_id"])
	assert.Equal(t, "field1Val", attrsMap["field1"])
	assert.Equal(t, "field2Val", attrsMap["field2"])
	assert.Equal(t, "attr1Val", attrsMap["attr1"])
	assert.Equal(t, "attr2Val", attrsMap["attr2"])
	assert.Equal(t, "ecp", attrsMap["emq_service"])

	//span tags & fields duplicate
	writer = new(MockInfluxWriter)
	config = &otel2influx.OtelTracesToLineProtocolConfig{
		Logger: new(common.NoopLogger),
		Writer: writer,
		GlobalTrace: otel2influx.Trace{
			Table: "spantest",
			SpanDimensions: []string{
				"client_id",
				common.AttributeTraceID,
			},
			SpanFields: []string{
				"field1",
				"client_id",
			},
		},
	}
	protocol, _ = otel2influx.NewOtelTracesToLineProtocol(config)
	err = protocol.WriteTraces(context.Background(), generateTraces(""))

	assert.Nil(t, err)
	point = writer.points
	count, ok := point[0].fields["dropped_attributes_count"].(uint64)
	assert.True(t, ok)
	assert.Equal(t, (uint64)(1), count)
}

func generateTraces(customKey string) ptrace.Traces {
	traces := ptrace.NewTraces()

	resSpan := traces.ResourceSpans().AppendEmpty()
	res := resSpan.Resource()
	attrs := res.Attributes()
	attrs.PutStr("field1", "field1Val")
	attrs.PutStr("field2", "field2Val")
	attrs.PutStr("client_id", "clientMockId")
	attrs.PutStr("attr1", "attr1Val")
	attrs.PutStr("attr2", "attr2Val")
	if len(customKey) > 0 {
		attrs.PutStr("emq_service", customKey)
	}

	scope := resSpan.ScopeSpans().AppendEmpty()
	span := scope.Spans().AppendEmpty()
	span.SetTraceID([16]byte{9, 8, 7})
	span.SetSpanID([8]byte{1, 2, 3})
	span.SetName("mockSpanName")
	span.SetKind(ptrace.SpanKindServer)
	span.Status().SetCode(ptrace.StatusCodeOk)
	span.SetStartTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 1, 1, 20, 0, 0, 0, time.UTC)))
	span.SetEndTimestamp(pcommon.NewTimestampFromTime(time.Date(2024, 1, 1, 20, 0, 0, 10, time.UTC)))
	span.SetParentSpanID([8]byte{4, 5, 6})

	return traces
}
