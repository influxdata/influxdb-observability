package otel2influx

import (
	"context"
	"time"
)

type InfluxWriter interface {
	WritePoint(ctx context.Context, measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) error
}
