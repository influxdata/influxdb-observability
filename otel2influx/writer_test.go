package otel2influx_test

import (
	"context"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/otel2influx"
)

type mockPoint struct {
	measurement string
	tags        map[string]string
	fields      map[string]interface{}
	ts          time.Time
	vType       common.InfluxMetricValueType
}

var _ otel2influx.InfluxWriter = &MockInfluxWriter{}
var _ otel2influx.InfluxWriterBatch = &MockInfluxWriterBatch{}

type MockInfluxWriter struct {
	points []mockPoint
}

func (w *MockInfluxWriter) NewBatch() otel2influx.InfluxWriterBatch {
	return &MockInfluxWriterBatch{w: w}
}

type MockInfluxWriterBatch struct {
	w *MockInfluxWriter
}

func (b *MockInfluxWriterBatch) EnqueuePoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	b.w.points = append(b.w.points, mockPoint{
		measurement: measurement,
		tags:        tags,
		fields:      fields,
		ts:          ts,
		vType:       vType,
	})
	return nil
}

func (b *MockInfluxWriterBatch) WriteBatch(ctx context.Context) error {
	return nil
}

var (
	timestamp      = pcommon.Timestamp(1395066363000000123)
	startTimestamp = pcommon.Timestamp(1395066363000000001)
)
