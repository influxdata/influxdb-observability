package otel2influx

import (
	"context"
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/influxdata/influxdb-observability/common"
)

type metricWriter interface {
	writeMetric(ctx context.Context, resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, metric pmetric.Metric, w InfluxWriter) error
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

func (c *OtelMetricsToLineProtocol) WriteMetrics(ctx context.Context, md pmetric.Metrics, w InfluxWriter) error {
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetrics := md.ResourceMetrics().At(i)
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			ilMetrics := resourceMetrics.ScopeMetrics().At(j)
			for k := 0; k < ilMetrics.Metrics().Len(); k++ {
				metric := ilMetrics.Metrics().At(k)
				if err := c.writer.writeMetric(ctx, resourceMetrics.Resource(), ilMetrics.Scope(), metric, w); err != nil {
					return fmt.Errorf("failed to convert OTLP metric to line protocol: %w", err)
				}
			}
		}
	}
	return nil
}
