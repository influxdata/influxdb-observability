package otel2influx_test

import (
	"context"
	"time"

	"github.com/influxdata/influxdb-observability/common"
)

type mockPoint struct {
	measurement string
	tags        map[string]string
	fields      map[string]interface{}
	ts          time.Time
	vType       common.InfluxMetricValueType
}

type MockInfluxWriter struct {
	points []mockPoint
}

func (m *MockInfluxWriter) WritePoint(_ context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	m.points = append(m.points, mockPoint{
		measurement: measurement,
		tags:        tags,
		fields:      fields,
		ts:          ts,
		vType:       vType,
	})
	return nil
}
