package otel2lineprotocol

import (
	"fmt"
	"io"
	"strconv"
	"time"

	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
	otlpcommon "go.opentelemetry.io/proto/otlp/common/v1"
	otlpmetrics "go.opentelemetry.io/proto/otlp/metrics/v1"
	otlpresource "go.opentelemetry.io/proto/otlp/resource/v1"
)

func (c *OpenTelemetryToLineProtocolConverter) WriteMetrics(resourceMetricss []*otlpmetrics.ResourceMetrics, w io.Writer) (droppedMetrics int) {
	for _, resourceMetrics := range resourceMetricss {
		resource := resourceMetrics.Resource
		for _, ilMetrics := range resourceMetrics.InstrumentationLibraryMetrics {
			instrumentationLibrary := ilMetrics.InstrumentationLibrary
			for _, metric := range ilMetrics.Metrics {
				if err := c.writeMetric(resource, instrumentationLibrary, metric, w); err != nil {
					droppedMetrics++
					c.logger.Debug("failed to convert metric to line protocol", err)
				}
			}
		}
	}
	return
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetric(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, metric *otlpmetrics.Metric, w io.Writer) error {
	// Ignore metric.Description() and metric.Unit() .
	switch metricData := metric.Data.(type) {
	case *otlpmetrics.Metric_IntGauge:
		return c.writeMetricIntGauge(resource, instrumentationLibrary, metric.Name, metricData.IntGauge, w)
	case *otlpmetrics.Metric_DoubleGauge:
		return c.writeMetricDoubleGauge(resource, instrumentationLibrary, metric.Name, metricData.DoubleGauge, w)
	case *otlpmetrics.Metric_IntSum:
		return c.writeMetricIntSum(resource, instrumentationLibrary, metric.Name, metricData.IntSum, w)
	case *otlpmetrics.Metric_DoubleSum:
		return c.writeMetricDoubleSum(resource, instrumentationLibrary, metric.Name, metricData.DoubleSum, w)
	case *otlpmetrics.Metric_IntHistogram:
		return c.writeMetricIntHistogram(resource, instrumentationLibrary, metric.Name, metricData.IntHistogram, w)
	case *otlpmetrics.Metric_DoubleHistogram:
		return c.writeMetricDoubleHistogram(resource, instrumentationLibrary, metric.Name, metricData.DoubleHistogram, w)
	case *otlpmetrics.Metric_DoubleSummary:
		return c.writeMetricDoubleSummary(resource, instrumentationLibrary, metric.Name, metricData.DoubleSummary, w)
	default:
		return fmt.Errorf("unknown metric data type %T", metric.Data)
	}
}

func (c *OpenTelemetryToLineProtocolConverter) metricLabelsToTags(labels []*otlpcommon.StringKeyValue, encoder *lineprotocol.Encoder) {
	for _, label := range labels {
		if k, v := label.Key, label.Value; k == "" {
			c.logger.Debug("metric label key is empty")
		} else {
			encoder.AddTag(k, v)
		}
	}
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricIntGauge(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, gauge *otlpmetrics.IntGauge, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	dataPoints := gauge.DataPoints

	for _, dataPoint := range dataPoints {
		encoder.StartLine(measurement)

		c.resourceToTags(resource, encoder)
		instrumentationLibraryToTags(instrumentationLibrary, encoder)
		c.metricLabelsToTags(dataPoint.Labels, encoder)
		encoder.AddField(metricGaugeFieldKey, lineprotocol.IntValue(dataPoint.Value))

		encoder.EndLine(time.Unix(0, int64(dataPoint.TimeUnixNano)))
	}

	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return fmt.Errorf("failed to encode int gauge metric: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return err
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricDoubleGauge(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, gauge *otlpmetrics.DoubleGauge, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	dataPoints := gauge.DataPoints

	for _, dataPoint := range dataPoints {
		encoder.StartLine(measurement)

		c.resourceToTags(resource, encoder)
		instrumentationLibraryToTags(instrumentationLibrary, encoder)
		c.metricLabelsToTags(dataPoint.Labels, encoder)
		if v, ok := lineprotocol.FloatValue(dataPoint.Value); !ok {
			c.logger.Debug("invalid double gauge metric value", "value", dataPoint.Value)
			// TODO encoder.AbortLine()
		} else {
			encoder.AddField(metricGaugeFieldKey, v)

			encoder.EndLine(time.Unix(0, int64(dataPoint.TimeUnixNano)))
		}
	}

	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return fmt.Errorf("failed to encode double gauge metric: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return err
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricIntSum(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, sum *otlpmetrics.IntSum, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	// Ignore sum.IsMonotonic .
	if sum.AggregationTemporality != otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
		return fmt.Errorf("unsupported metric aggregation temporality %q", sum.AggregationTemporality)
	}

	for _, dataPoint := range sum.DataPoints {
		encoder.StartLine(measurement)

		c.resourceToTags(resource, encoder)
		instrumentationLibraryToTags(instrumentationLibrary, encoder)
		c.metricLabelsToTags(dataPoint.Labels, encoder)
		encoder.AddField(metricCounterFieldKey, lineprotocol.IntValue(dataPoint.Value))

		encoder.EndLine(time.Unix(0, int64(dataPoint.TimeUnixNano)))
	}

	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return fmt.Errorf("failed to encode int sum metric: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return err
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricDoubleSum(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, sum *otlpmetrics.DoubleSum, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	// Ignore sum.IsMonotonic .
	if sum.AggregationTemporality != otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
		return fmt.Errorf("unsupported metric aggregation temporality %q", sum.AggregationTemporality)
	}

	for _, dataPoint := range sum.DataPoints {
		encoder.StartLine(measurement)

		c.resourceToTags(resource, encoder)
		instrumentationLibraryToTags(instrumentationLibrary, encoder)
		c.metricLabelsToTags(dataPoint.Labels, encoder)
		if v, ok := lineprotocol.FloatValue(dataPoint.Value); !ok {
			c.logger.Debug("invalid double sum metric value", "value", dataPoint.Value)
			// TODO encoder.AbortLine()
		} else {
			encoder.AddField(metricCounterFieldKey, v)

			encoder.EndLine(time.Unix(0, int64(dataPoint.TimeUnixNano)))
		}
	}

	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return fmt.Errorf("failed to encode double sum metric: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return err
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricIntHistogram(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, histogram *otlpmetrics.IntHistogram, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	if histogram.AggregationTemporality != otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
		return fmt.Errorf("unsupported metric aggregation temporality %q", histogram.AggregationTemporality)
	}

	for _, dataPoint := range histogram.DataPoints {
		encoder.StartLine(measurement)

		c.resourceToTags(resource, encoder)
		instrumentationLibraryToTags(instrumentationLibrary, encoder)
		c.metricLabelsToTags(dataPoint.Labels, encoder)
		encoder.AddField(metricHistogramCountFieldKey, lineprotocol.UintValue(dataPoint.Count))
		encoder.AddField(metricHistogramSumFieldKey, lineprotocol.IntValue(dataPoint.Sum))
		bucketCounts, explicitBounds := dataPoint.BucketCounts, dataPoint.ExplicitBounds
		if len(bucketCounts) > 0 && len(bucketCounts) != len(explicitBounds)+1 {
			return fmt.Errorf("invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", len(bucketCounts), len(explicitBounds))
		}
		for j, bucketCount := range bucketCounts {
			if j < len(explicitBounds) {
				boundFieldKey := strconv.FormatFloat(explicitBounds[j], 'f', -1, 64)
				encoder.AddField(boundFieldKey, lineprotocol.UintValue(bucketCount))
			} else {
				encoder.AddField(metricHistogramInfFieldKey, lineprotocol.UintValue(bucketCount))
			}
		}

		encoder.EndLine(time.Unix(0, int64(dataPoint.TimeUnixNano)))
	}

	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return fmt.Errorf("failed to encode int histogram metric: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return err
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricDoubleHistogram(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, histogram *otlpmetrics.DoubleHistogram, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	if histogram.AggregationTemporality != otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE {
		return fmt.Errorf("unsupported metric aggregation temporality %q", histogram.AggregationTemporality)
	}

	for _, dataPoint := range histogram.DataPoints {
		encoder.StartLine(measurement)

		c.resourceToTags(resource, encoder)
		instrumentationLibraryToTags(instrumentationLibrary, encoder)
		c.metricLabelsToTags(dataPoint.Labels, encoder)
		encoder.AddField(metricHistogramCountFieldKey, lineprotocol.UintValue(dataPoint.Count))
		if v, ok := lineprotocol.FloatValue(dataPoint.Sum); !ok {
			c.logger.Debug("invalid double histogram metric sum", "value", dataPoint.Sum)
			// TODO encoder.AbortLine()
			continue
		} else {
			encoder.AddField(metricHistogramSumFieldKey, v)
		}
		bucketCounts, explicitBounds := dataPoint.BucketCounts, dataPoint.ExplicitBounds
		if len(bucketCounts) > 0 && len(bucketCounts) != len(explicitBounds)+1 {
			return fmt.Errorf("invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", len(bucketCounts), len(explicitBounds))
		}
		for j, bucketCount := range bucketCounts {
			if j < len(explicitBounds) {
				boundFieldKey := strconv.FormatFloat(explicitBounds[j], 'f', -1, 64)
				encoder.AddField(boundFieldKey, lineprotocol.UintValue(bucketCount))
			} else {
				encoder.AddField(metricHistogramInfFieldKey, lineprotocol.UintValue(bucketCount))
			}
		}

		encoder.EndLine(time.Unix(0, int64(dataPoint.TimeUnixNano)))
	}

	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return fmt.Errorf("failed to encode double histogram metric: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return err
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricDoubleSummary(resource *otlpresource.Resource, instrumentationLibrary *otlpcommon.InstrumentationLibrary, measurement string, summary *otlpmetrics.DoubleSummary, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	for _, dataPoint := range summary.DataPoints {
		encoder.StartLine(measurement)

		c.resourceToTags(resource, encoder)
		instrumentationLibraryToTags(instrumentationLibrary, encoder)
		c.metricLabelsToTags(dataPoint.Labels, encoder)

		encoder.AddField(metricSummaryCountFieldKey, lineprotocol.UintValue(dataPoint.Count))
		if v, ok := lineprotocol.FloatValue(dataPoint.Sum); !ok {
			c.logger.Debug("invalid summary metric sum", "value", dataPoint.Sum)
			// TODO encoder.AbortLine()
			continue
		} else {
			encoder.AddField(metricSummarySumFieldKey, v)
		}
		for _, valueAtQuantile := range dataPoint.QuantileValues {
			k := strconv.FormatFloat(valueAtQuantile.Quantile, 'f', -1, 64)
			if v, ok := lineprotocol.FloatValue(valueAtQuantile.Value); !ok {
				c.logger.Debug("invalid summary quantile value", "value", valueAtQuantile.Value)
				// TODO encoder.AbortLine()
				continue
			} else {
				encoder.AddField(k, v)
			}
		}

		encoder.EndLine(time.Unix(0, int64(dataPoint.TimeUnixNano)))
	}

	if b, err := encoder.Bytes(), encoder.Err(); err != nil {
		return fmt.Errorf("failed to encode summary metric: %w", err)
	} else if _, err = w.Write(b); err != nil {
		return err
	}

	return nil
}
