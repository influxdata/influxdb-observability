package otel2influx

import (
	"context"
	"errors"
	"fmt"
	"strconv"
	"strings"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"

	"github.com/influxdata/influxdb-observability/common"
)

var _ metricWriter = (*metricWriterOtelV1)(nil)

type metricWriterOtelV1 struct {
	logger common.Logger
}

func (m *metricWriterOtelV1) enqueueMetric(ctx context.Context, resource pcommon.Resource, is pcommon.InstrumentationScope, pm pmetric.Metric, batch InfluxWriterBatch) (err error) {
	defer func() {
		if r := recover(); r != nil {
			var rerr error
			switch v := r.(type) {
			case error:
				rerr = v
			case string:
				rerr = errors.New(v)
			default:
				rerr = fmt.Errorf("%+v", r)
			}
			err = multierr.Combine(err, rerr)
		}
	}()

	// TODO metric description
	measurementName := fmt.Sprintf("%s_%s_%s", pm.Name(), pm.Unit(), strings.ToLower(pm.Type().String()))
	tags := make(map[string]string)
	tags = ResourceToTags(resource, tags)
	tags = InstrumentationScopeToTags(is, tags)

	switch pm.Type() {
	// case pmetric.MetricTypeGauge:
	//	return m.writeGauge(ctx, resource, is, pm.Name(), pm.Gauge(), batch)
	case pmetric.MetricTypeSum:
		m.enqueueSum(ctx, measurementName, tags, pm, batch)
	case pmetric.MetricTypeHistogram:
		m.enqueueHistogram(ctx, measurementName, tags, pm, batch)
	default:
		err = fmt.Errorf("unrecognized metric type %q", pm.Type())
	}
	return
}

// formatFieldKeyMetricSumOtelV1 composes a value field key from (sum temporality, sum monotonicity, and datapoint value type)
func formatFieldKeyMetricSumOtelV1(temporality string, monotonic bool, dataPointValueType string) string {
	var monotonicity string
	if monotonic {
		monotonicity = "monotonic"
	} else {
		monotonicity = "nonmonotonic"
	}

	return fmt.Sprintf("value_%s_%s_%s", strings.ToLower(temporality), monotonicity, strings.ToLower(dataPointValueType))
}

func (m *metricWriterOtelV1) enqueueSum(ctx context.Context, measurementName string, resourceTags map[string]string, pm pmetric.Metric, batch InfluxWriterBatch) {
	temporality := pm.Sum().AggregationTemporality().String()
	monotonic := pm.Sum().IsMonotonic()

	buildValue := func(dataPoint pmetric.NumberDataPoint) (string, interface{}) {
		fieldKey := formatFieldKeyMetricSumOtelV1(temporality, monotonic, dataPoint.ValueType().String())
		switch dataPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			return fieldKey, dataPoint.IntValue()
		case pmetric.NumberDataPointValueTypeDouble:
			return fieldKey, dataPoint.DoubleValue()
		default:
			panic(fmt.Sprintf("unsupported data point value type '%s'", dataPoint.ValueType().String()))
		}
	}

	for i := 0; i < pm.Sum().DataPoints().Len(); i++ {
		// TODO datapoint exemplars
		dataPoint := pm.Sum().DataPoints().At(i)

		fields := make(map[string]interface{}, 3)
		if dataPoint.StartTimestamp() != 0 {
			fields[common.AttributeStartTimeUnixNano] = int64(dataPoint.StartTimestamp())
		}
		valueFieldKey, value := buildValue(dataPoint)
		fields[valueFieldKey] = value

		tags := make(map[string]string, dataPoint.Attributes().Len()+len(resourceTags))
		for k, v := range resourceTags {
			tags[k] = v
		}
		dataPoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			tags[k] = v.AsString()
			return true
		})

		err := batch.EnqueuePoint(ctx, measurementName, tags, fields, dataPoint.Timestamp().AsTime(), common.InfluxMetricValueTypeUntyped)
		if err != nil {
			panic(err)
		}
	}
}

func (m *metricWriterOtelV1) enqueueHistogram(ctx context.Context, measurementName string, resourceTags map[string]string, pm pmetric.Metric, batch InfluxWriterBatch) {
	temporality := strings.ToLower(pm.Histogram().AggregationTemporality().String())

	for i := 0; i < pm.Histogram().DataPoints().Len(); i++ {
		// TODO datapoint exemplars
		dataPoint := pm.Histogram().DataPoints().At(i)

		bucketCounts, explicitBounds := dataPoint.BucketCounts(), dataPoint.ExplicitBounds()
		if bucketCounts.Len() > 0 &&
			bucketCounts.Len() != explicitBounds.Len() &&
			bucketCounts.Len() != explicitBounds.Len()+1 {
			// The infinity bucket is not used in this schema,
			// so accept input if that particular bucket is missing.
			panic(fmt.Sprintf("invalid metric histogram bucket counts qty %d vs explicit bounds qty %d", bucketCounts.Len(), explicitBounds.Len()))
		}

		fields := make(map[string]interface{}, explicitBounds.Len()+6)
		if dataPoint.StartTimestamp() != 0 {
			fields[common.AttributeStartTimeUnixNano] = int64(dataPoint.StartTimestamp())
		}
		for i := 0; i < explicitBounds.Len(); i++ {
			boundStr := strconv.FormatFloat(explicitBounds.At(i), 'f', -1, 64)
			k := fmt.Sprintf("%s_%s", temporality, boundStr)
			fields[k] = bucketCounts.At(i)
		}

		fields["count"] = dataPoint.Count()
		if dataPoint.HasSum() {
			fields["sum"] = dataPoint.Sum()
		}
		if dataPoint.HasMin() && dataPoint.HasMax() {
			fields["min"] = dataPoint.Min()
			fields["max"] = dataPoint.Max()
		}

		tags := make(map[string]string, dataPoint.Attributes().Len()+len(resourceTags))
		for k, v := range resourceTags {
			tags[k] = v
		}
		dataPoint.Attributes().Range(func(k string, v pcommon.Value) bool {
			tags[k] = v.AsString()
			return true
		})

		err := batch.EnqueuePoint(ctx, measurementName, tags, fields, dataPoint.Timestamp().AsTime(), common.InfluxMetricValueTypeUntyped)
		if err != nil {
			panic(err)
		}
	}
}
