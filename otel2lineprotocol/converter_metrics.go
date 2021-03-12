package otel2lineprotocol

import (
	"fmt"
	"io"
	"strconv"

	lineprotocol "github.com/influxdata/line-protocol/v2/influxdata"
	"go.opentelemetry.io/collector/consumer/pdata"
	"go.uber.org/zap"
)

func (c *OpenTelemetryToLineProtocolConverter) WriteMetrics(md pdata.Metrics, w io.Writer) (droppedMetrics int) {
	resourceMetricss := md.ResourceMetrics()
	for i := 0; i < resourceMetricss.Len(); i++ {
		resourceMetrics := resourceMetricss.At(i)
		resource := resourceMetrics.Resource()
		ilMetricss := resourceMetrics.InstrumentationLibraryMetrics()
		for j := 0; j < ilMetricss.Len(); j++ {
			ilMetrics := ilMetricss.At(j)
			instrumentationLibrary := ilMetrics.InstrumentationLibrary()
			metrics := ilMetrics.Metrics()
			for k := 0; k < metrics.Len(); k++ {
				metric := metrics.At(k)
				if err := c.writeMetric(resource, instrumentationLibrary, metric, w); err != nil {
					droppedMetrics++
					c.logger.Debug("failed to convert metric to line protocol", zap.Error(err))
				}
			}
		}
	}
	return
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetric(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, metric pdata.Metric, w io.Writer) error {
	// Ignore metric.Description() and metric.Unit() .
	// TODO should we include metric.Unit() ?

	switch metric.DataType() {
	case pdata.MetricDataTypeNone:
		return nil
	case pdata.MetricDataTypeIntGauge:
		return c.writeMetricIntGauge(resource, instrumentationLibrary, metric.Name(), metric.IntGauge(), w)
	case pdata.MetricDataTypeDoubleGauge:
		return c.writeMetricDoubleGauge(resource, instrumentationLibrary, metric.Name(), metric.DoubleGauge(), w)
	case pdata.MetricDataTypeIntSum:
		return c.writeMetricIntSum(resource, instrumentationLibrary, metric.Name(), metric.IntSum(), w)
	case pdata.MetricDataTypeDoubleSum:
		return c.writeMetricDoubleSum(resource, instrumentationLibrary, metric.Name(), metric.DoubleSum(), w)
	case pdata.MetricDataTypeIntHistogram:
		return c.writeMetricIntHistogram(resource, instrumentationLibrary, metric.Name(), metric.IntHistogram(), w)
	case pdata.MetricDataTypeDoubleHistogram:
		return c.writeMetricDoubleHistogram(resource, instrumentationLibrary, metric.Name(), metric.DoubleHistogram(), w)
	case pdata.MetricDataTypeDoubleSummary:
		return c.writeMetricDoubleSummary(resource, instrumentationLibrary, metric.Name(), metric.DoubleSummary(), w)
	default:
		return fmt.Errorf("Unknown metric data type %q", metric.DataType())
	}
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricIntGauge(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, gauge pdata.IntGauge, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	dataPoints := gauge.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dataPoint := dataPoints.At(i)

		encoder.StartLine(measurement)
		c.resourceToTags(resource, encoder)
		c.instrumentationLibraryToTags(instrumentationLibrary, encoder)

		dataPoint.LabelsMap().ForEach(func(k string, v string) {
			encoder.AddTag(k, v)
		})
		encoder.AddField(metricGaugeFieldKey, lineprotocol.MustNewValue(dataPoint.Value()))

		encoder.EndLine(dataPoint.Timestamp().AsTime())
		if b, err := encoder.Bytes(), encoder.Err(); err != nil {
			b = append(b, '\n')
			return fmt.Errorf("failed to convert metric to line protocol: %w", err)
		} else if _, err = w.Write(b); err != nil {
			return fmt.Errorf("failed to write metric as line protocol: %w", err)
		}
		encoder.Reset()
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricDoubleGauge(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, gauge pdata.DoubleGauge, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	dataPoints := gauge.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dataPoint := dataPoints.At(i)

		encoder.StartLine(measurement)
		c.resourceToTags(resource, encoder)
		c.instrumentationLibraryToTags(instrumentationLibrary, encoder)

		dataPoint.LabelsMap().ForEach(func(k string, v string) {
			encoder.AddTag(k, v)
		})
		encoder.AddField(metricGaugeFieldKey, lineprotocol.MustNewValue(dataPoint.Value()))

		encoder.EndLine(dataPoint.Timestamp().AsTime())
		if b, err := encoder.Bytes(), encoder.Err(); err != nil {
			return fmt.Errorf("failed to convert metric to line protocol: %w", err)
		} else if _, err = w.Write(b); err != nil {
			return fmt.Errorf("failed to write metric as line protocol: %w", err)
		} else if _, err = w.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
		encoder.Reset()
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricIntSum(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, sum pdata.IntSum, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	if sum.AggregationTemporality() != pdata.AggregationTemporalityCumulative {
		return fmt.Errorf("Unsupported metric aggregation temporality %q", sum.AggregationTemporality())
	}

	// Ignore sum.IsMonotonic() .
	dataPoints := sum.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dataPoint := dataPoints.At(i)

		encoder.StartLine(measurement)
		c.resourceToTags(resource, encoder)
		c.instrumentationLibraryToTags(instrumentationLibrary, encoder)

		dataPoint.LabelsMap().ForEach(func(k string, v string) {
			encoder.AddTag(k, v)
		})
		encoder.AddField(metricCounterFieldKey, lineprotocol.MustNewValue(dataPoint.Value()))

		encoder.EndLine(dataPoint.Timestamp().AsTime())
		if b, err := encoder.Bytes(), encoder.Err(); err != nil {
			return fmt.Errorf("failed to convert metric to line protocol: %w", err)
		} else if _, err = w.Write(b); err != nil {
			return fmt.Errorf("failed to write metric as line protocol: %w", err)
		} else if _, err = w.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
		encoder.Reset()
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricDoubleSum(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, sum pdata.DoubleSum, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	if sum.AggregationTemporality() != pdata.AggregationTemporalityCumulative {
		return fmt.Errorf("Unsupported metric aggregation temporality %q", sum.AggregationTemporality())
	}

	// Ignore sum.IsMonotonic() .
	dataPoints := sum.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dataPoint := dataPoints.At(i)

		encoder.StartLine(measurement)
		c.resourceToTags(resource, encoder)
		c.instrumentationLibraryToTags(instrumentationLibrary, encoder)

		dataPoint.LabelsMap().ForEach(func(k string, v string) {
			encoder.AddTag(k, v)
		})
		encoder.AddField(metricCounterFieldKey, lineprotocol.MustNewValue(dataPoint.Value()))

		encoder.EndLine(dataPoint.Timestamp().AsTime())
		if b, err := encoder.Bytes(), encoder.Err(); err != nil {
			return fmt.Errorf("failed to convert metric to line protocol: %w", err)
		} else if _, err = w.Write(b); err != nil {
			return fmt.Errorf("failed to write metric as line protocol: %w", err)
		} else if _, err = w.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
		encoder.Reset()
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricIntHistogram(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, histogram pdata.IntHistogram, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	if histogram.AggregationTemporality() != pdata.AggregationTemporalityCumulative {
		return fmt.Errorf("Unsupported metric aggregation temporality %q", histogram.AggregationTemporality())
	}

	dataPoints := histogram.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dataPoint := dataPoints.At(i)

		encoder.StartLine(measurement)
		c.resourceToTags(resource, encoder)
		c.instrumentationLibraryToTags(instrumentationLibrary, encoder)
		dataPoint.LabelsMap().ForEach(func(k string, v string) {
			encoder.AddTag(k, v)
		})

		encoder.AddField(metricHistogramCountFieldKey, lineprotocol.MustNewValue(dataPoint.Count()))
		encoder.AddField(metricHistogramSumFieldKey, lineprotocol.MustNewValue(dataPoint.Sum()))
		bucketCounts, explicitBounds := dataPoint.BucketCounts(), dataPoint.ExplicitBounds()
		if len(bucketCounts) > 0 && len(bucketCounts) != len(explicitBounds)+1 {
			return fmt.Errorf("Invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", len(dataPoint.BucketCounts()), len(dataPoint.ExplicitBounds()))
		}
		for j, bucketCount := range bucketCounts {
			if j < len(explicitBounds) {
				boundFieldKey := strconv.FormatFloat(explicitBounds[j], 'f', -1, 64)
				encoder.AddField(boundFieldKey, lineprotocol.MustNewValue(bucketCount))
			} else {
				encoder.AddField(metricHistogramInfFieldKey, lineprotocol.MustNewValue(bucketCount))
			}
		}

		encoder.EndLine(dataPoint.Timestamp().AsTime())
		if b, err := encoder.Bytes(), encoder.Err(); err != nil {
			return fmt.Errorf("failed to convert metric to line protocol: %w", err)
		} else if _, err = w.Write(b); err != nil {
			return fmt.Errorf("failed to write metric as line protocol: %w", err)
		} else if _, err = w.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
		encoder.Reset()
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricDoubleHistogram(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, histogram pdata.DoubleHistogram, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	if histogram.AggregationTemporality() != pdata.AggregationTemporalityCumulative {
		return fmt.Errorf("Unsupported metric aggregation temporality %q", histogram.AggregationTemporality())
	}

	dataPoints := histogram.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dataPoint := dataPoints.At(i)

		encoder.StartLine(measurement)
		c.resourceToTags(resource, encoder)
		c.instrumentationLibraryToTags(instrumentationLibrary, encoder)
		dataPoint.LabelsMap().ForEach(func(k string, v string) {
			encoder.AddTag(k, v)
		})

		encoder.AddField(metricHistogramCountFieldKey, lineprotocol.MustNewValue(dataPoint.Count()))
		encoder.AddField(metricHistogramSumFieldKey, lineprotocol.MustNewValue(dataPoint.Sum()))
		bucketCounts, explicitBounds := dataPoint.BucketCounts(), dataPoint.ExplicitBounds()
		if len(bucketCounts) > 0 && len(bucketCounts) != len(explicitBounds)+1 {
			return fmt.Errorf("Invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", len(dataPoint.BucketCounts()), len(dataPoint.ExplicitBounds()))
		}
		for j, bucketCount := range bucketCounts {
			if j < len(explicitBounds) {
				boundFieldKey := strconv.FormatFloat(explicitBounds[j], 'f', -1, 64)
				encoder.AddField(boundFieldKey, lineprotocol.MustNewValue(bucketCount))
			} else {
				encoder.AddField(metricHistogramInfFieldKey, lineprotocol.MustNewValue(bucketCount))
			}
		}

		encoder.EndLine(dataPoint.Timestamp().AsTime())
		if b, err := encoder.Bytes(), encoder.Err(); err != nil {
			return fmt.Errorf("failed to convert metric to line protocol: %w", err)
		} else if _, err = w.Write(b); err != nil {
			return fmt.Errorf("failed to write metric as line protocol: %w", err)
		} else if _, err = w.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
		encoder.Reset()
	}

	return nil
}

func (c *OpenTelemetryToLineProtocolConverter) writeMetricDoubleSummary(resource pdata.Resource, instrumentationLibrary pdata.InstrumentationLibrary, measurement string, summary pdata.DoubleSummary, w io.Writer) error {
	encoder := c.encoderPool.Get().(*lineprotocol.Encoder)
	defer func() {
		encoder.Reset()
		c.encoderPool.Put(encoder)
	}()

	dataPoints := summary.DataPoints()

	for i := 0; i < dataPoints.Len(); i++ {
		dataPoint := dataPoints.At(i)

		encoder.StartLine(measurement)
		c.resourceToTags(resource, encoder)
		c.instrumentationLibraryToTags(instrumentationLibrary, encoder)
		dataPoint.LabelsMap().ForEach(func(k string, v string) {
			encoder.AddTag(k, v)
		})

		encoder.AddField(metricSummaryCountFieldKey, lineprotocol.MustNewValue(dataPoint.Count()))
		encoder.AddField(metricSummarySumFieldKey, lineprotocol.MustNewValue(dataPoint.Sum()))
		quantileValues := dataPoint.QuantileValues()
		for j := 0; j < quantileValues.Len(); j++ {
			valueAtQuantile := quantileValues.At(j)
			quantileFieldKey := strconv.FormatFloat(valueAtQuantile.Quantile(), 'f', -1, 64)
			encoder.AddField(quantileFieldKey, lineprotocol.MustNewValue(valueAtQuantile.Quantile()))
		}

		encoder.EndLine(dataPoint.Timestamp().AsTime())
		if b, err := encoder.Bytes(), encoder.Err(); err != nil {
			return fmt.Errorf("failed to convert metric to line protocol: %w", err)
		} else if _, err = w.Write(b); err != nil {
			return fmt.Errorf("failed to write metric as line protocol: %w", err)
		} else if _, err = w.Write([]byte("\n")); err != nil {
			return fmt.Errorf("failed to write newline: %w", err)
		}
		encoder.Reset()
	}

	return nil
}
