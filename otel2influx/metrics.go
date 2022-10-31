package otel2influx

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/influxdata/influxdb-observability/common"
)

type metricWriter interface {
	writeMetric(ctx context.Context, resource pcommon.Resource, instrumentationScope pcommon.InstrumentationScope, metric pmetric.Metric, batch InfluxWriterBatch) error
}

type OtelMetricsToLineProtocol struct {
	iw InfluxWriter
	mw metricWriter
}

func NewOtelMetricsToLineProtocol(logger common.Logger, iw InfluxWriter, schema common.MetricsSchema) (*OtelMetricsToLineProtocol, error) {
	var mw metricWriter
	switch schema {
	case common.MetricsSchemaTelegrafPrometheusV1:
		mw = &metricWriterTelegrafPrometheusV1{
			logger: logger,
		}
	case common.MetricsSchemaTelegrafPrometheusV2:
		mw = &metricWriterTelegrafPrometheusV2{
			logger: logger,
		}
	case common.MetricsSchemaOtelV1:
		mw = &metricWriterOtelV1{
			logger: logger,
		}
	default:
		return nil, fmt.Errorf("unrecognized metrics schema %d", schema)
	}
	return &OtelMetricsToLineProtocol{
		iw: iw,
		mw: mw,
	}, nil
}

func (c *OtelMetricsToLineProtocol) WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
	batch := c.iw.NewBatch()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetrics := md.ResourceMetrics().At(i)
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			ilMetrics := resourceMetrics.ScopeMetrics().At(j)
			for k := 0; k < ilMetrics.Metrics().Len(); k++ {
				metric := ilMetrics.Metrics().At(k)
				if err := c.mw.writeMetric(ctx, resourceMetrics.Resource(), ilMetrics.Scope(), metric, batch); err != nil {
					return fmt.Errorf("failed to convert OTLP metric to line protocol: %w", err)
				}
			}
		}
	}
	return batch.FlushBatch(ctx)
}
