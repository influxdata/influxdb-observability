package otel2influx

import (
	"context"
	"fmt"

	"github.com/influxdata/influxdb-observability/common"
	"go.opentelemetry.io/collector/model/pdata"
)

type metricWriter interface {
	writeMetric(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, metric pdata.Metric, w InfluxWriter) error
}

type OtelMetricsToLineProtocol struct {
	writer metricWriter
}

func NewOtelMetricsToLineProtocol(logger common.Logger, schema common.MetricsSchema) (*OtelMetricsToLineProtocol, error) {
	var writer metricWriter
	switch schema {
	case common.MetricsSchemaTelegrafPrometheusV1:
		writer = &metricWriterTelegrafPrometheusV1{
			logger: logger,
		}
	case common.MetricsSchemaTelegrafPrometheusV2:
		writer = &metricWriterTelegrafPrometheusV2{
			logger: logger,
		}
	default:
		return nil, fmt.Errorf("unrecognized metrics schema %d", schema)
	}
	return &OtelMetricsToLineProtocol{
		writer: writer,
	}, nil
}

func (c *OtelMetricsToLineProtocol) WriteMetrics(ctx context.Context, md pdata.Metrics, w InfluxWriter) error {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetrics := md.ResourceMetrics().At(i)
		for j := 0; j < resourceMetrics.InstrumentationLibraryMetrics().Len(); j++ {
			ilMetrics := resourceMetrics.InstrumentationLibraryMetrics().At(j)
			for k := 0; k < ilMetrics.Metrics().Len(); k++ {
				metric := ilMetrics.Metrics().At(k)
				if err := c.writer.writeMetric(ctx, resourceMetrics.Resource(), ilMetrics.InstrumentationLibrary(), metric, w); err != nil {
					return fmt.Errorf("failed to convert OTLP metric to line protocol: %w", err)
				}
			}
		}
	}
	return nil
}
