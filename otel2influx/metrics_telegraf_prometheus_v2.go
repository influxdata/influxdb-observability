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

type metricWriterTelegrafPrometheusV2 struct {
	logger common.Logger
}

func (c *metricWriterTelegrafPrometheusV2) writeMetric(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, metric pdata.Metric, w InfluxWriter) error {
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

func (c *metricWriterTelegrafPrometheusV2) initMetricTagsAndTimestamp(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, timestamp pdata.Timestamp, attributes pdata.AttributeMap) (tags map[string]string, fields map[string]interface{}, ts time.Time, err error) {
	ts = timestamp.AsTime()
	if ts.IsZero() {
		err = errors.New("metric has no timestamp")
		return
	}

	tags = make(map[string]string)
	fields = make(map[string]interface{})

	attributes.Range(func(k string, v pdata.AttributeValue) bool {
		if k == "" {
			c.logger.Debug("metric attribute key is empty")
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

func (c *metricWriterTelegrafPrometheusV2) writeGauge(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, gauge pdata.Gauge, w InfluxWriter) error {
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
			fields[measurement] = dataPoint.DoubleVal()
		case pdata.MetricValueTypeInt:
			fields[measurement] = dataPoint.IntVal()
		default:
			return fmt.Errorf("unsupported gauge data point type %d", dataPoint.Type())
		}

		if err = w.WritePoint(ctx, common.MeasurementPrometheus, tags, fields, ts, common.InfluxMetricValueTypeGauge); err != nil {
			return fmt.Errorf("failed to write point for gauge: %w", err)
		}
	}

	return nil
}

func (c *metricWriterTelegrafPrometheusV2) writeSum(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, sum pdata.Sum, w InfluxWriter) error {
	if sum.AggregationTemporality() != pdata.AggregationTemporalityCumulative {
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
			fields[measurement] = dataPoint.DoubleVal()
		case pdata.MetricValueTypeInt:
			fields[measurement] = dataPoint.IntVal()
		default:
			return fmt.Errorf("unsupported sum data point type %d", dataPoint.Type())
		}

		if err = w.WritePoint(ctx, common.MeasurementPrometheus, tags, fields, ts, common.InfluxMetricValueTypeSum); err != nil {
			return fmt.Errorf("failed to write point for sum: %w", err)
		}
	}

	return nil
}

func (c *metricWriterTelegrafPrometheusV2) writeHistogram(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, histogram pdata.Histogram, w InfluxWriter) error {
	if histogram.AggregationTemporality() != pdata.AggregationTemporalityCumulative {
		return fmt.Errorf("unsupported histogram aggregation temporality %q", histogram.AggregationTemporality())
	}

	for i := 0; i < histogram.DataPoints().Len(); i++ {
		dataPoint := histogram.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.Timestamp(), dataPoint.Attributes())
		if err != nil {
			return err
		}

		{
			f := make(map[string]interface{}, len(fields)+2)
			for k, v := range fields {
				f[k] = v
			}

			f[measurement+common.MetricHistogramCountSuffix] = float64(dataPoint.Count())
			f[measurement+common.MetricHistogramSumSuffix] = dataPoint.Sum()

			if err = w.WritePoint(ctx, common.MeasurementPrometheus, tags, f, ts, common.InfluxMetricValueTypeHistogram); err != nil {
				return fmt.Errorf("failed to write point for histogram: %w", err)
			}
		}

		bucketCounts, explicitBounds := dataPoint.BucketCounts(), dataPoint.ExplicitBounds()
		if len(bucketCounts) > 0 && len(bucketCounts) != len(explicitBounds)+1 {
			return fmt.Errorf("invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", len(bucketCounts), len(explicitBounds))
		}

		for i, explicitBound := range explicitBounds {
			t := make(map[string]string, len(tags)+1)
			for k, v := range tags {
				t[k] = v
			}
			f := make(map[string]interface{}, len(fields)+1)
			for k, v := range fields {
				f[k] = v
			}

			boundTagValue := strconv.FormatFloat(explicitBound, 'f', -1, 64)
			t[common.MetricHistogramBoundKeyV2] = boundTagValue
			f[measurement+common.MetricHistogramBucketSuffix] = float64(bucketCounts[i])

			if err = w.WritePoint(ctx, common.MeasurementPrometheus, t, f, ts, common.InfluxMetricValueTypeHistogram); err != nil {
				return fmt.Errorf("failed to write point for histogram: %w", err)
			}
		} // Skip last bucket count - infinity not used in this schema
	}

	return nil
}

func (c *metricWriterTelegrafPrometheusV2) writeSummary(ctx context.Context, resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, summary pdata.Summary, w InfluxWriter) error {
	for i := 0; i < summary.DataPoints().Len(); i++ {
		dataPoint := summary.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.Timestamp(), dataPoint.Attributes())
		if err != nil {
			return err
		}

		{
			f := make(map[string]interface{}, len(fields)+2)
			for k, v := range fields {
				f[k] = v
			}

			f[measurement+common.MetricSummaryCountSuffix] = float64(dataPoint.Count())
			f[measurement+common.MetricSummarySumSuffix] = dataPoint.Sum()

			if err = w.WritePoint(ctx, common.MeasurementPrometheus, tags, f, ts, common.InfluxMetricValueTypeSummary); err != nil {
				return fmt.Errorf("failed to write point for summary: %w", err)
			}
		}

		for j := 0; j < dataPoint.QuantileValues().Len(); j++ {
			valueAtQuantile := dataPoint.QuantileValues().At(j)
			t := make(map[string]string, len(tags)+1)
			for k, v := range tags {
				t[k] = v
			}
			f := make(map[string]interface{}, len(fields)+1)
			for k, v := range fields {
				f[k] = v
			}

			quantileTagValue := strconv.FormatFloat(valueAtQuantile.Quantile(), 'f', -1, 64)
			t[common.MetricSummaryQuantileKeyV2] = quantileTagValue
			f[measurement] = float64(valueAtQuantile.Value())

			if err = w.WritePoint(ctx, common.MeasurementPrometheus, t, f, ts, common.InfluxMetricValueTypeSummary); err != nil {
				return fmt.Errorf("failed to write point for summary: %w", err)
			}
		}
	}

	return nil
}
