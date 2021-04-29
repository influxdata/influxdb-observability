package otel2influx

import (
	"context"
	"time"
)

type InfluxWriterValueType uint8

const (
	InfluxWriterValueTypeUntyped = iota
	InfluxWriterValueTypeGauge
	InfluxWriterValueTypeSum
	InfluxWriterValueTypeHistogram
	InfluxWriterValueTypeSummary
)

type InfluxWriter interface {
	WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, mvType InfluxWriterValueType) error
}
