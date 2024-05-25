package otel2influx

import (
	"context"
	"fmt"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/influxdata/influxdb-observability/common"
)

type OtelMetricsToLineProtocolConfig struct {
	Logger common.Logger
	Writer InfluxWriter
	Schema common.MetricsSchema
}

func DefaultOtelMetricsToLineProtocolConfig() *OtelMetricsToLineProtocolConfig {
	return &OtelMetricsToLineProtocolConfig{
		Logger: new(common.NoopLogger),
		Writer: new(NoopInfluxWriter),
		Schema: common.MetricsSchemaTelegrafPrometheusV1,
	}
}

type metricWriter interface {
	enqueueMetric(ctx context.Context, resource pcommon.Resource, instrumentationScope pcommon.InstrumentationScope, metric pmetric.Metric, batch InfluxWriterBatch) error
}

type OtelMetricsToLineProtocol struct {
	iw InfluxWriter
	mw metricWriter
}

func NewOtelMetricsToLineProtocol(config *OtelMetricsToLineProtocolConfig) (*OtelMetricsToLineProtocol, error) {
	var mw metricWriter
	switch config.Schema {
	case common.MetricsSchemaTelegrafPrometheusV1:
		mw = &metricWriterTelegrafPrometheusV1{
			logger: config.Logger,
		}
	case common.MetricsSchemaTelegrafPrometheusV2:
		mw = &metricWriterTelegrafPrometheusV2{
			logger: config.Logger,
		}
	case common.MetricsSchemaOtelV1:
		mw = &metricWriterOtelV1{
			logger: config.Logger,
		}
	default:
		return nil, fmt.Errorf("unrecognized metrics schema %d", config.Logger)
	}
	return &OtelMetricsToLineProtocol{
		iw: config.Writer,
		mw: mw,
	}, nil
}

func (c *OtelMetricsToLineProtocol) WriteMetrics(ctx context.Context, md pmetric.Metrics) error {
	batch := c.iw.NewBatch()
	for i := 0; i < md.ResourceMetrics().Len(); i++ {
		resourceMetrics := md.ResourceMetrics().At(i)
		for j := 0; j < resourceMetrics.ScopeMetrics().Len(); j++ {
			isMetrics := resourceMetrics.ScopeMetrics().At(j)
			for k := 0; k < isMetrics.Metrics().Len(); k++ {
				metric := isMetrics.Metrics().At(k)
				if err := c.mw.enqueueMetric(ctx, resourceMetrics.Resource(), isMetrics.Scope(), metric, batch); err != nil {
					return consumererror.NewPermanent(fmt.Errorf("failed to convert OTLP metric to line protocol: %w", err))
				}
			}
		}
	}
	return batch.WriteBatch(ctx)
}

type basicDataPoint interface {
	Timestamp() pcommon.Timestamp
	StartTimestamp() pcommon.Timestamp
	Attributes() pcommon.Map
}
