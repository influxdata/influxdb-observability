package otel2influx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"time"

	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
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
	case *otlpmetrics.Metric_Gauge:
		return c.writeMetricGauge(ctx, resource, instrumentationLibrary, metric.Name, metricData.Gauge, w)
	case *otlpmetrics.Metric_Sum:
		return c.writeMetricSum(ctx, resource, instrumentationLibrary, metric.Name, metricData.Sum, w)
	case *otlpmetrics.Metric_Histogram:
		return c.writeMetricHistogram(ctx, resource, instrumentationLibrary, metric.Name, metricData.Histogram, w)
	case *otlpmetrics.Metric_Summary:
		return c.writeMetricSummary(ctx, resource, instrumentationLibrary, metric.Name, metricData.Summary, w)
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

func (c *OpenTelemetryToInfluxConverter) writeMetricGauge(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, gauge *otlpmetrics.Gauge, w InfluxWriter) error {
	for _, dataPoint := range gauge.DataPoints {
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint.TimeUnixNano, dataPoint.Labels)
		if err != nil {
			return err
		}

		fields[metricGaugeFieldKey] = dataPoint.Value

		if err = w.WritePoint(ctx, measurement, tags, fields, ts); err != nil {
			return fmt.Errorf("failed to write point for gauge: %w", err)
		}
	}

	return nil
}

func (c *OpenTelemetryToInfluxConverter) writeMetricSum(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, sum *otlpmetrics.Sum, w InfluxWriter) error {
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
			return fmt.Errorf("failed to write point for sum: %w", err)
		}
	}

	return nil
}

func (c *OpenTelemetryToInfluxConverter) writeMetricHistogram(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, histogram *otlpmetrics.Histogram, w InfluxWriter) error {
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
			return fmt.Errorf("failed to write point for histogram: %w", err)
		}
	}

	return nil
}

func (c *OpenTelemetryToInfluxConverter) writeMetricSummary(ctx context.Context, resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, summary *otlpmetrics.Summary, w InfluxWriter) error {
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
