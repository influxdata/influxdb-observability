package otel2influx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"go.opentelemetry.io/collector/model/pdata"
)

type metricWriterTelegrafPrometheusV1 struct {
	logger common.Logger
}

func (c *metricWriterTelegrafPrometheusV1) writeMetric(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, metric pdata.Metric, w InfluxWriter) error {
	// Ignore metric.Description() and metric.Unit() .
	switch metric.DataType() {
	case pdata.MetricDataTypeGauge:
		return c.writeGauge(ctx, resource, instrumentationLibrary, metric.Name(), metric.Gauge(), w)
	case pdata.MetricDataTypeSum:
		return c.writeSum(ctx, resource, instrumentationLibrary, metric.Name(), metric.Sum(), w)
	case pdata.MetricDataTypeHistogram:
		return c.writeHistogram(ctx, resource, instrumentationLibrary, metric.Name(), metric.Histogram(), w)
	case pdata.MetricDataTypeSummary:
		return c.writeSummary(ctx, resource, instrumentationLibrary, metric.Name(), metric.Summary(), w)
	default:
		return fmt.Errorf("unknown metric type %q", metric.DataType())
	}
}

func (c *metricWriterTelegrafPrometheusV1) initMetricTagsAndTimestamp(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, timestamp pdata.Timestamp, labels pdata.AttributeMap) (tags map[string]string, fields map[string]interface{}, ts time.Time, err error) {
	ts = timestamp.AsTime()
	ts.UTC()
	if ts.IsZero() {
		err = errors.New("metric has no timestamp")
		return
	}

	tags = make(map[string]string)
	fields = make(map[string]interface{})

	labels.Range(func(k string, v pdata.AttributeValue) bool {
		if k == "" {
			c.logger.Debug("metric label key is empty")
		} else {
			var vv string
			vv, err = common.AttributeValueToInfluxTagValue(v)
			if err != nil {
				return false
			}
			tags[k] = vv
		}
		return true
	})
	if err != nil {
		err = fmt.Errorf("failed to convert attribute value to string: %w", err)
		return
	}

	tags = ResourceToTags(c.logger, resource, tags)
	tags = InstrumentationLibraryToTags(instrumentationLibrary, tags)

	return
}

func (c *metricWriterTelegrafPrometheusV1) writeGauge(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, gauge pdata.Gauge, w InfluxWriter) error {
	for i := 0; i < gauge.DataPoints().Len(); i++ {
		dataPoint := gauge.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.Timestamp(), dataPoint.Attributes())
		if err != nil {
			return err
		}

		switch dataPoint.Type() {
		case pdata.MetricValueTypeNone:
			continue
		case pdata.MetricValueTypeDouble:
			fields[common.MetricGaugeFieldKey] = dataPoint.DoubleVal()
		case pdata.MetricValueTypeInt:
			fields[common.MetricGaugeFieldKey] = dataPoint.IntVal()
		default:
			return fmt.Errorf("unsupported gauge data point type %d", dataPoint.Type())
		}

		if err = w.WritePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeGauge); err != nil {
			return fmt.Errorf("failed to write point for gauge: %w", err)
		}
	}

	return nil
}

func (c *metricWriterTelegrafPrometheusV1) writeSum(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, sum pdata.Sum, w InfluxWriter) error {
	if sum.AggregationTemporality() != pdata.MetricAggregationTemporalityCumulative {
		return fmt.Errorf("unsupported sum aggregation temporality %q", sum.AggregationTemporality())
	}
	if !sum.IsMonotonic() {
		return fmt.Errorf("unsupported non-monotonic sum '%s'", measurement)
	}

	for i := 0; i < sum.DataPoints().Len(); i++ {
		dataPoint := sum.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.Timestamp(), dataPoint.Attributes())
		if err != nil {
			return err
		}

		switch dataPoint.Type() {
		case pdata.MetricValueTypeNone:
			continue
		case pdata.MetricValueTypeDouble:
			fields[common.MetricCounterFieldKey] = dataPoint.DoubleVal()
		case pdata.MetricValueTypeInt:
			fields[common.MetricCounterFieldKey] = dataPoint.IntVal()
		default:
			return fmt.Errorf("unsupported sum data point type %d", dataPoint.Type())
		}

		if err = w.WritePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeSum); err != nil {
			return fmt.Errorf("failed to write point for sum: %w", err)
		}
	}

	return nil
}

func (c *metricWriterTelegrafPrometheusV1) writeHistogram(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, histogram pdata.Histogram, w InfluxWriter) error {
	if histogram.AggregationTemporality() != pdata.MetricAggregationTemporalityCumulative {
		return fmt.Errorf("unsupported histogram aggregation temporality %q", histogram.AggregationTemporality())
	}

	for i := 0; i < histogram.DataPoints().Len(); i++ {
		dataPoint := histogram.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.Timestamp(), dataPoint.Attributes())
		if err != nil {
			return err
		}

		fields[common.MetricHistogramCountFieldKey] = float64(dataPoint.Count())
		fields[common.MetricHistogramSumFieldKey] = dataPoint.Sum()
		bucketCounts, explicitBounds := dataPoint.BucketCounts(), dataPoint.ExplicitBounds()
		if len(bucketCounts) > 0 && len(bucketCounts) != len(explicitBounds)+1 {
			return fmt.Errorf("invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", len(bucketCounts), len(explicitBounds))
		}
		for i, explicitBound := range explicitBounds {
			boundFieldKey := strconv.FormatFloat(explicitBound, 'f', -1, 64)
			fields[boundFieldKey] = float64(bucketCounts[i])
		} // Skip last bucket count - infinity not used in this schema

		if err = w.WritePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeHistogram); err != nil {
			return fmt.Errorf("failed to write point for histogram: %w", err)
		}
	}

	return nil
}

func (c *metricWriterTelegrafPrometheusV1) writeSummary(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, summary pdata.Summary, w InfluxWriter) error {
	for i := 0; i < summary.DataPoints().Len(); i++ {
		dataPoint := summary.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.Timestamp(), dataPoint.Attributes())
		if err != nil {
			return err
		}

		fields[common.MetricSummaryCountFieldKey] = float64(dataPoint.Count())
		fields[common.MetricSummarySumFieldKey] = dataPoint.Sum()
		for j := 0; j < dataPoint.QuantileValues().Len(); j++ {
			valueAtQuantile := dataPoint.QuantileValues().At(j)
			quantileFieldKey := strconv.FormatFloat(valueAtQuantile.Quantile(), 'f', -1, 64)
			fields[quantileFieldKey] = valueAtQuantile.Value()
		}

		if err = w.WritePoint(ctx, measurement, tags, fields, ts, common.InfluxMetricValueTypeSummary); err != nil {
			return fmt.Errorf("failed to write point for summary: %w", err)
		}
	}

	return nil
}
