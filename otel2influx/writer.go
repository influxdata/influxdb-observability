package otel2influx

import (
	"context"
	"time"

	"github.com/influxdata/influxdb-observability/common"
)

type InfluxWriter interface {
	NewBatch() InfluxWriterBatch
}

type InfluxWriterBatch interface {
	EnqueuePoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error
	WriteBatch(ctx context.Context) error
}

type NoopInfluxWriter struct{}

func (w *NoopInfluxWriter) NewBatch() InfluxWriterBatch {
	return w
}

func (w *NoopInfluxWriter) EnqueuePoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	return nil
}

func (w *NoopInfluxWriter) WriteBatch(ctx context.Context) error {
	return nil
}
