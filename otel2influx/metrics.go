package otel2influx

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb-observability/common"
	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

type MetricsSchema uint8

const (
	_ = iota
	MetricsSchemaTelegrafPrometheusV1
	MetricsSchemaTelegrafPrometheusV2
)

type metricWriter interface {
	writeMetric(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, metric *otlpmetrics.Metric, w InfluxWriter) error
}

type OtelMetricsToLineProtocol struct {
	writer metricWriter
}

func NewOtelMetricsToLineProtocol(logger common.Logger, schema MetricsSchema) (*OtelMetricsToLineProtocol, error) {
	var writer metricWriter
	switch schema {
	case MetricsSchemaTelegrafPrometheusV1:
		writer = &metricWriterTelegrafPrometheusV1{
			logger: logger,
		}
	case MetricsSchemaTelegrafPrometheusV2:
		panic("not implemented")
	default:
		return nil, fmt.Errorf("unrecognized metrics schema %d", schema)
	}
	return &OtelMetricsToLineProtocol{
		writer: writer,
	}, nil
}

func (c *OtelMetricsToLineProtocol) WriteMetricsFromRequestBytes(ctx context.Context, b []byte, w InfluxWriter) error {
	var req otlpcollectormetrics.ExportMetricsServiceRequest
	err := proto.Unmarshal(b, &req)
	if err != nil {
		return err
	}
	return c.WriteMetrics(ctx, req.ResourceMetrics, w)
}

func (c *OtelMetricsToLineProtocol) WriteMetrics(ctx context.Context, resourceMetricss []*otlpmetrics.ResourceMetrics, w InfluxWriter) error {
	for _, resourceMetrics := range resourceMetricss {
		resource := resourceMetrics.Resource
		for _, ilMetrics := range resourceMetrics.InstrumentationLibraryMetrics {
			instrumentationLibrary := ilMetrics.InstrumentationLibrary
			for _, metric := range ilMetrics.Metrics {
				if err := c.writer.writeMetric(ctx, resource, instrumentationLibrary, metric, w); err != nil {
					return fmt.Errorf("failed to convert OTLP metric to line protocol: %w", err)
				}
			}
		}
	}
	return nil
}
