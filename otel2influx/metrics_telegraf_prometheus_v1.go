package otel2influx

import (
	"errors"
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/influxdata/influxdb-observability/common"
)

type metricWriterTelegrafPrometheusV1 struct {
	logger common.Logger
}

func (c *metricWriterTelegrafPrometheusV1) enqueueMetric(resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, metric pmetric.Metric, batch InfluxWriterBatch) error {
	// Ignore metric.Description() and metric.Unit() .
	switch metric.Type() {
	case pmetric.MetricTypeGauge:
		return c.enqueueGauge(resource, instrumentationLibrary, metric.Name(), metric.Gauge(), batch)
	case pmetric.MetricTypeSum:
		if metric.Sum().IsMonotonic() {
			return c.enqueueSum(resource, instrumentationLibrary, metric.Name(), metric.Sum(), batch)
		}
		return c.enqueueGaugeFromSum(resource, instrumentationLibrary, metric.Name(), metric.Sum(), batch)
	case pmetric.MetricTypeHistogram:
		return c.enqueueHistogram(resource, instrumentationLibrary, metric.Name(), metric.Histogram(), batch)
	case pmetric.MetricTypeSummary:
		return c.enqueueSummary(resource, instrumentationLibrary, metric.Name(), metric.Summary(), batch)
	default:
		return fmt.Errorf("unknown metric type %q", metric.Type())
	}
}

func (c *metricWriterTelegrafPrometheusV1) initMetricTagsAndTimestamp(resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, dataPoint basicDataPoint) (tags map[string]string, fields map[string]interface{}, ts time.Time, err error) {
	ts = dataPoint.Timestamp().AsTime()
	if ts.IsZero() {
		err = errors.New("metric has no timestamp")
		return
	}

	tags = make(map[string]string)
	fields = make(map[string]interface{})
	if dataPoint.StartTimestamp() != 0 {
		fields[common.AttributeStartTimeUnixNano] = int64(dataPoint.StartTimestamp())
	}

	dataPoint.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k == "" {
			c.logger.Debug("metric attribute key is empty")
		} else {
			tags[k] = v.AsString()
		}
		return true
	})
	if err != nil {
		err = fmt.Errorf("failed to convert attribute value to string: %w", err)
		return
	}

	tags = ResourceToTags(c.logger, resource, tags)
	tags = InstrumentationScopeToTags(instrumentationLibrary, tags)

	return
}

func (c *metricWriterTelegrafPrometheusV1) enqueueGauge(resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, measurement string, gauge pmetric.Gauge, batch InfluxWriterBatch) error {
	for i := 0; i < gauge.DataPoints().Len(); i++ {
		dataPoint := gauge.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint)
		if err != nil {
			return err
		}

		switch dataPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeEmpty:
			continue
		case pmetric.NumberDataPointValueTypeDouble:
			fields[common.MetricGaugeFieldKey] = dataPoint.DoubleValue()
		case pmetric.NumberDataPointValueTypeInt:
			fields[common.MetricGaugeFieldKey] = dataPoint.IntValue()
		default:
			return fmt.Errorf("unsupported gauge data point type %d", dataPoint.ValueType())
		}

		if err = batch.EnqueuePoint(measurement, tags, fields, ts, common.InfluxMetricValueTypeGauge); err != nil {
			return fmt.Errorf("failed to write point for gauge: %w", err)
		}
	}

	return nil
}

func (c *metricWriterTelegrafPrometheusV1) enqueueGaugeFromSum(resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, measurement string, sum pmetric.Sum, batch InfluxWriterBatch) error {
	for i := 0; i < sum.DataPoints().Len(); i++ {
		dataPoint := sum.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint)
		if err != nil {
			return err
		}

		switch dataPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeEmpty:
			continue
		case pmetric.NumberDataPointValueTypeDouble:
			fields[common.MetricGaugeFieldKey] = dataPoint.DoubleValue()
		case pmetric.NumberDataPointValueTypeInt:
			fields[common.MetricGaugeFieldKey] = dataPoint.IntValue()
		default:
			return fmt.Errorf("unsupported sum (as gauge) data point type %d", dataPoint.ValueType())
		}

		if err = batch.EnqueuePoint(measurement, tags, fields, ts, common.InfluxMetricValueTypeGauge); err != nil {
			return fmt.Errorf("failed to write point for sum (as gauge): %w", err)
		}
	}

	return nil
}

func (c *metricWriterTelegrafPrometheusV1) enqueueSum(resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, measurement string, sum pmetric.Sum, batch InfluxWriterBatch) error {
	for i := 0; i < sum.DataPoints().Len(); i++ {
		dataPoint := sum.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint)
		if err != nil {
			return err
		}

		switch dataPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeEmpty:
			continue
		case pmetric.NumberDataPointValueTypeDouble:
			fields[common.MetricCounterFieldKey] = dataPoint.DoubleValue()
		case pmetric.NumberDataPointValueTypeInt:
			fields[common.MetricCounterFieldKey] = dataPoint.IntValue()
		default:
			return fmt.Errorf("unsupported sum data point type %d", dataPoint.ValueType())
		}

		if err = batch.EnqueuePoint(measurement, tags, fields, ts, common.InfluxMetricValueTypeSum); err != nil {
			return fmt.Errorf("failed to write point for sum: %w", err)
		}
	}

	return nil
}

func (c *metricWriterTelegrafPrometheusV1) enqueueHistogram(resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, measurement string, histogram pmetric.Histogram, batch InfluxWriterBatch) error {
	for i := 0; i < histogram.DataPoints().Len(); i++ {
		dataPoint := histogram.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint)
		if err != nil {
			return err
		}

		fields[common.MetricHistogramCountFieldKey] = float64(dataPoint.Count())
		fields[common.MetricHistogramSumFieldKey] = dataPoint.Sum()
		if dataPoint.HasMin() {
			fields[common.MetricHistogramMinFieldKey] = dataPoint.Min()
		}
		if dataPoint.HasMax() {
			fields[common.MetricHistogramMaxFieldKey] = dataPoint.Max()
		}
		bucketCounts, explicitBounds := dataPoint.BucketCounts(), dataPoint.ExplicitBounds()
		if bucketCounts.Len() > 0 &&
			bucketCounts.Len() != explicitBounds.Len() &&
			bucketCounts.Len() != explicitBounds.Len()+1 {
			// The infinity bucket is not used in this schema,
			// so accept input if that particular bucket is missing.
			return fmt.Errorf("invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", bucketCounts.Len(), explicitBounds.Len())
		}
		for i := 0; i < explicitBounds.Len(); i++ {
			var bucketCount uint64
			for j := 0; j <= i; j++ {
				bucketCount += bucketCounts.At(j)
			}

			boundFieldKey := strconv.FormatFloat(explicitBounds.At(i), 'f', -1, 64)
			fields[boundFieldKey] = float64(bucketCount)
		}

		if err = batch.EnqueuePoint(measurement, tags, fields, ts, common.InfluxMetricValueTypeHistogram); err != nil {
			return fmt.Errorf("failed to write point for histogram: %w", err)
		}
	}

	return nil
}

func (c *metricWriterTelegrafPrometheusV1) enqueueSummary(resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, measurement string, summary pmetric.Summary, batch InfluxWriterBatch) error {
	for i := 0; i < summary.DataPoints().Len(); i++ {
		dataPoint := summary.DataPoints().At(i)
		tags, fields, ts, err := c.initMetricTagsAndTimestamp(resource, instrumentationLibrary, dataPoint)
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

		if err = batch.EnqueuePoint(measurement, tags, fields, ts, common.InfluxMetricValueTypeSummary); err != nil {
			return fmt.Errorf("failed to write point for summary: %w", err)
		}
	}

	return nil
}
