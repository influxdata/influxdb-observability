package otel2influx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlpmetrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	otlpresource "go.opentelemetry.io/proto/otlp/resource/v1"
)

func (c *OpenTelemetryToInfluxConverter) WriteMetrics(ctx context.Context, resourceMetricss []*otlpmetrics.ResourceMetrics, w InfluxWriter) (droppedMetrics int) {
	for _, resourceMetrics := range resourceMetricss {
		resource := resourceMetrics.Resource
		for _, ilMetrics := range resourceMetrics.InstrumentationLibraryMetrics {
			instrumentationLibrary := ilMetrics.InstrumentationLibrary
			for _, metric := range ilMetrics.Metrics {
				if err := c.writeMetric(ctx, resource, instrumentationLibrary, metric, w); err != nil {
					droppedMetrics++
					c.logger.Debug("failed to convert metric to line protocol", err)
				}
			}
		}
	}
	return
}

func (c *OpenTelemetryToInfluxConverter) writeMetric(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, metric *otlpmetrics.Metric, w InfluxWriter) error {
	// Ignore metric.Description() and metric.Unit() .
	switch metricData := metric.Data.(type) {
	case *otlpmetrics.Metric_IntGauge:
		return c.writeMetricIntGauge(ctx, resource, instrumentationLibrary, metric.Name, metricData.IntGauge, w)
	case *otlpmetrics.Metric_DoubleGauge:
		return c.writeMetricDoubleGauge(ctx, resource, instrumentationLibrary, metric.Name, metricData.DoubleGauge, w)
	case *otlpmetrics.Metric_IntSum:
		return c.writeMetricIntSum(ctx, resource, instrumentationLibrary, metric.Name, metricData.IntSum, w)
	case *otlpmetrics.Metric_DoubleSum:
		return c.writeMetricDoubleSum(ctx, resource, instrumentationLibrary, metric.Name, metricData.DoubleSum, w)
	case *otlpmetrics.Metric_IntHistogram:
		return c.writeMetricIntHistogram(ctx, resource, instrumentationLibrary, metric.Name, metricData.IntHistogram, w)
	case *otlpmetrics.Metric_DoubleHistogram:
		return c.writeMetricDoubleHistogram(ctx, resource, instrumentationLibrary, metric.Name, metricData.DoubleHistogram, w)
	case *otlpmetrics.Metric_DoubleSummary:
		return c.writeMetricDoubleSummary(ctx, resource, instrumentationLibrary, metric.Name, metricData.DoubleSummary, w)
	default:
		return fmt.Errorf("unknown metric data type %T", metric.Data)
	}
}

func (c *OpenTelemetryToInfluxConverter) initMetricTagsAndTimestamp(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, timeUnixNano uint64, labels []*otlpcommon.StringKeyValue) (tags map[string]string, fields map[string]interface{}, ts time.Time, err error) {
	ts = time.Unix(0, int64(timeUnixNano))
	if ts.IsZero() {
		err = errors.New("metric has no timestamp")
		return
	}

	tags = make(map[string]string)
	fields = make(map[string]interface{})

	for _, label := range labels {
		if k, v := label.Key, label.Value; k == "" {
			c.logger.Debug("metric label key is empty")
		} else {
			tags[k] = v
		}
	}

	var droppedResourceAttributesCount uint64
	tags, droppedResourceAttributesCount = c.resourceToTags(resource, tags)
	if droppedResourceAttributesCount > 0 {
		fields[attributeDroppedResourceAttributesCount] = droppedResourceAttributesCount
	}
	tags = c.instrumentationLibraryToTags(instrumentationLibrary, tags)

	return
}

func (c *OpenTelemetryToInfluxConverter) writeMetricIntGauge(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, gauge *otlpmetrics.IntGauge, w InfluxWriter) error {
	for _, dataPoint := range gauge.DataPoints {
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.TimeUnixNano, dataPoint.Labels)
		if err != nil {
			return err
		}

		fields[metricGaugeFieldKey] = dataPoint.Value

		if err = w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
			return fmt.Errorf("failed to write point for int gauge: %w", err)
		}
	}

	return nil
}

func (c *OpenTelemetryToInfluxConverter) writeMetricDoubleGauge(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, gauge *otlpmetrics.DoubleGauge, w InfluxWriter) error {
	for _, dataPoint := range gauge.DataPoints {
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.TimeUnixNano, dataPoint.Labels)
		if err != nil {
			return err
		}

		fields[metricGaugeFieldKey] = dataPoint.Value

		if err = w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
			return fmt.Errorf("failed to write point for double gauge: %w", err)
		}
	}

	return nil
}

func (c *OpenTelemetryToInfluxConverter) writeMetricIntSum(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, sum *otlpmetrics.IntSum, w InfluxWriter) error {
	// Ignore sum.IsMonotonic .
	if sum.AggregationTemporality != otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
		return fmt.Errorf("unsupported metric aggregation temporality %q", sum.AggregationTemporality)
	}

	for _, dataPoint := range sum.DataPoints {
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.TimeUnixNano, dataPoint.Labels)
		if err != nil {
			return err
		}

		fields[metricCounterFieldKey] = dataPoint.Value

		if err = w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
			return fmt.Errorf("failed to write point for int sum: %w", err)
		}
	}

	return nil
}

func (c *OpenTelemetryToInfluxConverter) writeMetricDoubleSum(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, sum *otlpmetrics.DoubleSum, w InfluxWriter) error {
	// Ignore sum.IsMonotonic .
	if sum.AggregationTemporality != otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
		return fmt.Errorf("unsupported metric aggregation temporality %q", sum.AggregationTemporality)
	}

	for _, dataPoint := range sum.DataPoints {
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.TimeUnixNano, dataPoint.Labels)
		if err != nil {
			return err
		}

		fields[metricCounterFieldKey] = dataPoint.Value

		if err = w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
			return fmt.Errorf("failed to write point for double sum: %w", err)
		}
	}

	return nil
}

func (c *OpenTelemetryToInfluxConverter) writeMetricIntHistogram(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, histogram *otlpmetrics.IntHistogram, w InfluxWriter) error {
	if histogram.AggregationTemporality != otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
		return fmt.Errorf("unsupported metric aggregation temporality %q", histogram.AggregationTemporality)
	}

	for _, dataPoint := range histogram.DataPoints {
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.TimeUnixNano, dataPoint.Labels)
		if err != nil {
			return err
		}

		fields[metricHistogramCountFieldKey] = dataPoint.Count
		fields[metricHistogramSumFieldKey] = dataPoint.Sum
		bucketCounts, explicitBounds := dataPoint.BucketCounts, dataPoint.ExplicitBounds
		if len(bucketCounts) > 0 && len(bucketCounts) != len(explicitBounds)+1 {
			return fmt.Errorf("invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", len(bucketCounts), len(explicitBounds))
		}
		for j, bucketCount := range bucketCounts {
			var boundFieldKey string
			if j < len(explicitBounds) {
				boundFieldKey = strconv.FormatFloat(explicitBounds[j], 'f', -1, 64)
			} else {
				boundFieldKey = metricHistogramInfFieldKey
			}
			fields[boundFieldKey] = bucketCount
		}

		if err = w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
			return fmt.Errorf("failed to write point for int histogram: %w", err)
		}
	}

	return nil
}

func (c *OpenTelemetryToInfluxConverter) writeMetricDoubleHistogram(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, histogram *otlpmetrics.DoubleHistogram, w InfluxWriter) error {
	if histogram.AggregationTemporality != otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
		return fmt.Errorf("unsupported metric aggregation temporality %q", histogram.AggregationTemporality)
	}

	for _, dataPoint := range histogram.DataPoints {
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.TimeUnixNano, dataPoint.Labels)
		if err != nil {
			return err
		}

		fields[metricHistogramCountFieldKey] = dataPoint.Count
		fields[metricHistogramSumFieldKey] = dataPoint.Sum
		bucketCounts, explicitBounds := dataPoint.BucketCounts, dataPoint.ExplicitBounds
		if len(bucketCounts) > 0 && len(bucketCounts) != len(explicitBounds)+1 {
			return fmt.Errorf("invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", len(bucketCounts), len(explicitBounds))
		}
		for j, bucketCount := range bucketCounts {
			var boundFieldKey string
			if j < len(explicitBounds) {
				boundFieldKey = strconv.FormatFloat(explicitBounds[j], 'f', -1, 64)
			} else {
				boundFieldKey = metricHistogramInfFieldKey
			}
			fields[boundFieldKey] = bucketCount
		}

		if err = w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
			return fmt.Errorf("failed to write point for double histogram: %w", err)
		}
	}

	return nil
}

func (c *OpenTelemetryToInfluxConverter) writeMetricDoubleSummary(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, summary *otlpmetrics.DoubleSummary, w InfluxWriter) error {
	for _, dataPoint := range summary.DataPoints {
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.TimeUnixNano, dataPoint.Labels)
		if err != nil {
			return err
		}

		fields[metricSummaryCountFieldKey] = dataPoint.Count
		fields[metricSummarySumFieldKey] = dataPoint.Sum
		for _, valueAtQuantile := range dataPoint.QuantileValues {
			quantileFieldKey := strconv.FormatFloat(valueAtQuantile.Quantile, 'f', -1, 64)
			fields[quantileFieldKey] = valueAtQuantile.Value
		}

		if err = w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
			return fmt.Errorf("failed to write point for summary: %w", err)
		}
	}

	return nil
}
