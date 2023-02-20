package influx2otel

import (
	"fmt"
	"strconv"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/influxdata/influxdb-observability/common"
)

func (b *MetricsBatch) addPointTelegrafPrometheusV1(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	vType = b.inferMetricValueTypeV1(vType, fields)
	if vType == common.InfluxMetricValueTypeUntyped {
		return errValueTypeUnknown
	}

	if ts.IsZero() {
		ts = time.Now()
	}

	switch vType {
	case common.InfluxMetricValueTypeGauge:
		return b.convertGaugeV1(measurement, tags, fields, ts)
	case common.InfluxMetricValueTypeSum:
		return b.convertSumV1(measurement, tags, fields, ts)
	case common.InfluxMetricValueTypeHistogram:
		return b.convertHistogramV1(measurement, tags, fields, ts)
	case common.InfluxMetricValueTypeSummary:
		return b.convertSummaryV1(measurement, tags, fields, ts)
	default:
		return fmt.Errorf("impossible InfluxMetricValueType %d", vType)
	}
}

// inferMetricValueTypeV1 attempts to derive a metric value type
// for line protocol data formatted as schema Telegraf/Prometheus V1.
//
// If the type cannot be inferred, then common.InfluxMetricValueTypeUntyped is returned.
func (b *MetricsBatch) inferMetricValueTypeV1(vType common.InfluxMetricValueType, fields map[string]interface{}) common.InfluxMetricValueType {
	if vType != common.InfluxMetricValueTypeUntyped {
		return vType
	}
	if _, found := fields[common.MetricGaugeFieldKey]; found {
		return common.InfluxMetricValueTypeGauge
	}
	if _, found := fields[common.MetricCounterFieldKey]; found {
		return common.InfluxMetricValueTypeSum
	}
	for k := range fields {
		if k == common.MetricHistogramCountFieldKey || k == common.MetricHistogramSumFieldKey || isStringNumeric(k) {
			// We cannot reliably distinguish between histogram and summary
			// without knowing we have all points, so here we assume histogram.
			return common.InfluxMetricValueTypeHistogram
		}
	}
	return common.InfluxMetricValueTypeUntyped
}

func isStringNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err == nil
}

func (b *MetricsBatch) convertGaugeV1(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	if fieldValue, found := fields[common.MetricGaugeFieldKey]; found {
		var floatValue *float64
		var intValue *int64
		switch typedValue := fieldValue.(type) {
		case float64:
			floatValue = &typedValue
		case int64:
			intValue = &typedValue
		case uint64:
			convertedTypedValue := int64(typedValue)
			intValue = &convertedTypedValue
		default:
			return fmt.Errorf("unsupported gauge value type %T", fieldValue)
		}

		metric, attributes, err := b.lookupMetric(measurement, tags, common.InfluxMetricValueTypeGauge)
		if err != nil {
			return err
		}
		dataPoint := metric.Gauge().DataPoints().AppendEmpty()
		attributes.CopyTo(dataPoint.Attributes())
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		// set start_time, if exists and is RFC3339
		// used by statsd input plugin
		if startTimeObj, ok := fields["start_time"]; ok {
			if startTimeStr, ok := startTimeObj.(string); ok {
				if t, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
					dataPoint.SetStartTimestamp(pcommon.NewTimestampFromTime(t))
				}
			}
		}

		if floatValue != nil {
			dataPoint.SetDoubleValue(*floatValue)
		} else if intValue != nil {
			dataPoint.SetIntValue(*intValue)
		} else {
			panic("unreachable")
		}
		return nil
	}

	for k, fieldValue := range fields {
		var floatValue *float64
		var intValue *int64

		// start_time is a metadata field about the metric,
		// provided by statsd plugin.
		if k == "start_time" {
			continue
		}
		switch typedValue := fieldValue.(type) {
		case float64:
			floatValue = &typedValue
		case int64:
			intValue = &typedValue
		case uint64:
			convertedTypedValue := int64(typedValue)
			intValue = &convertedTypedValue
		default:
			b.logger.Debug("unsupported gauge value type", "type", fmt.Sprintf("%T", fieldValue))
			continue
		}

		metricName := fmt.Sprintf("%s_%s", measurement, k)
		metric, attributes, err := b.lookupMetric(metricName, tags, common.InfluxMetricValueTypeGauge)
		if err != nil {
			return err
		}
		dataPoint := metric.Gauge().DataPoints().AppendEmpty()
		attributes.CopyTo(dataPoint.Attributes())
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		// set start_time, if exists and is RFC3339
		// used by statsd input plugin
		if startTimeObj, ok := fields["start_time"]; ok {
			if startTimeStr, ok := startTimeObj.(string); ok {
				if t, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
					dataPoint.SetStartTimestamp(pcommon.NewTimestampFromTime(t))
				}
			}
		}

		if floatValue != nil {
			dataPoint.SetDoubleValue(*floatValue)
		} else if intValue != nil {
			dataPoint.SetIntValue(*intValue)
		} else {
			panic("unreachable")
		}
	}

	return nil
}

func (b *MetricsBatch) convertSumV1(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	if fieldValue, found := fields[common.MetricCounterFieldKey]; found {
		var floatValue *float64
		var intValue *int64
		switch typedValue := fieldValue.(type) {
		case float64:
			floatValue = &typedValue
		case int64:
			intValue = &typedValue
		case uint64:
			convertedTypedValue := int64(typedValue)
			intValue = &convertedTypedValue
		default:
			return fmt.Errorf("unsupported counter value type %T", fieldValue)
		}

		metric, attributes, err := b.lookupMetric(measurement, tags, common.InfluxMetricValueTypeSum)
		if err != nil {
			return err
		}
		dataPoint := metric.Sum().DataPoints().AppendEmpty()
		attributes.CopyTo(dataPoint.Attributes())
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		// set start_time, if exists and is RFC3339
		// used by statsd input plugin
		if startTimeObj, ok := fields["start_time"]; ok {
			if startTimeStr, ok := startTimeObj.(string); ok {
				if t, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
					dataPoint.SetStartTimestamp(pcommon.NewTimestampFromTime(t))
				}
			}
		}

		if floatValue != nil {
			dataPoint.SetDoubleValue(*floatValue)
		} else if intValue != nil {
			dataPoint.SetIntValue(*intValue)
		} else {
			panic("unreachable")
		}

		return nil
	}

	for k, fieldValue := range fields {
		// start_time is a metadata field about the metric,
		// provided by statsd plugin.
		if k == "start_time" {
			continue
		}

		var floatValue *float64
		var intValue *int64
		switch typedValue := fieldValue.(type) {
		case float64:
			floatValue = &typedValue
		case int64:
			intValue = &typedValue
		case uint64:
			convertedTypedValue := int64(typedValue)
			intValue = &convertedTypedValue
		default:
			b.logger.Debug("unsupported counter value type", "type", fmt.Sprintf("%T", fieldValue))
			continue
		}

		metricName := fmt.Sprintf("%s_%s", measurement, k)
		metric, attributes, err := b.lookupMetric(metricName, tags, common.InfluxMetricValueTypeSum)
		if err != nil {
			return err
		}
		dataPoint := metric.Sum().DataPoints().AppendEmpty()
		attributes.CopyTo(dataPoint.Attributes())
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		// set start_time, if exists and is RFC3339
		// used by statsd input plugin
		if startTimeObj, ok := fields["start_time"]; ok {
			if startTimeStr, ok := startTimeObj.(string); ok {
				if t, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
					dataPoint.SetStartTimestamp(pcommon.NewTimestampFromTime(t))
				}
			}
		}

		if floatValue != nil {
			dataPoint.SetDoubleValue(*floatValue)
		} else if intValue != nil {
			dataPoint.SetIntValue(*intValue)
		} else {
			panic("unreachable")
		}
	}

	return nil
}

func (b *MetricsBatch) convertHistogramV1(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	var count uint64
	foundCount := false
	var sum float64
	foundSum := false
	var bucketCounts []uint64
	var explicitBounds []float64

	for k, vi := range fields {
		if k == common.MetricHistogramCountFieldKey {
			foundCount = true
			if vCount, ok := vi.(float64); !ok {
				return fmt.Errorf("unsupported histogram count value type %T", vi)
			} else {
				count = uint64(vCount)
			}

		} else if k == common.MetricHistogramSumFieldKey {
			foundSum = true
			var ok bool
			if sum, ok = vi.(float64); !ok {
				return fmt.Errorf("unsupported histogram sum value type %T", vi)
			}

		} else if explicitBound, err := strconv.ParseFloat(k, 64); err == nil {
			if vBucketCount, ok := vi.(float64); !ok {
				return fmt.Errorf("unsupported histogram bucket bound value type %T", vi)
			} else {
				explicitBounds = append(explicitBounds, explicitBound)
				bucketCounts = append(bucketCounts, uint64(vBucketCount))
			}

		} else if k == "start_time" {
		} else {
			b.logger.Debug("skipping unrecognized histogram field", "field", k, "value", vi)
		}
	}
	if !foundCount {
		return fmt.Errorf("histogram count field not found")
	}
	if !foundSum {
		return fmt.Errorf("histogram sum field not found")
	}

	bucketCounts = append(bucketCounts, count)
	sortHistogramBuckets(bucketCounts, explicitBounds)

	bucketsAreCumulative := true
	for i := 0; i < len(bucketCounts)-1; i++ {
		if bucketCounts[i] > bucketCounts[i+1] {
			// this can happen when an untyped summary is handled as a histogram
			bucketsAreCumulative = false
			break
		}
	}
	if bucketsAreCumulative {
		for i := len(bucketCounts) - 1; i > 0; i-- {
			bucketCounts[i] -= bucketCounts[i-1]
		}
	}

	metric, attributes, err := b.lookupMetric(measurement, tags, common.InfluxMetricValueTypeHistogram)
	if err != nil {
		return err
	}
	dataPoint := metric.Histogram().DataPoints().AppendEmpty()
	attributes.CopyTo(dataPoint.Attributes())
	dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	// set start_time, if exists and is RFC3339
	// used by statsd input plugin
	if startTimeObj, ok := fields["start_time"]; ok {
		if startTimeStr, ok := startTimeObj.(string); ok {
			if t, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
				dataPoint.SetStartTimestamp(pcommon.NewTimestampFromTime(t))
			}
		}
	}

	dataPoint.SetCount(count)
	dataPoint.SetSum(sum)
	dataPoint.BucketCounts().FromRaw(bucketCounts)
	dataPoint.ExplicitBounds().FromRaw(explicitBounds)
	return nil
}

func (b *MetricsBatch) convertSummaryV1(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	var count uint64
	foundCount := false
	var sum float64
	foundSum := false
	quantileValues := pmetric.NewSummaryDataPointValueAtQuantileSlice()

	for k, vi := range fields {
		if k == common.MetricSummaryCountFieldKey {
			foundCount = true
			if vCount, ok := vi.(float64); !ok {
				return fmt.Errorf("unsupported summary count value type %T", vi)
			} else {
				count = uint64(vCount)
			}

		} else if k == common.MetricSummarySumFieldKey {
			foundSum = true
			var ok bool
			if sum, ok = vi.(float64); !ok {
				return fmt.Errorf("unsupported summary sum value type %T", vi)
			}

		} else if quantile, err := strconv.ParseFloat(k, 64); err == nil {
			if value, ok := vi.(float64); !ok {
				return fmt.Errorf("unsupported summary bucket bound value type %T", vi)
			} else {
				valueAtQuantile := quantileValues.AppendEmpty()
				valueAtQuantile.SetQuantile(quantile)
				valueAtQuantile.SetValue(value)
			}

		} else if k == "start_time" {

		} else {
			b.logger.Debug("skipping unrecognized summary field", "field", k, "value", vi)
		}
	}
	if !foundCount {
		return fmt.Errorf("summary count not found")
	}
	if !foundSum {
		return fmt.Errorf("summary sum not found")
	}

	metric, attributes, err := b.lookupMetric(measurement, tags, common.InfluxMetricValueTypeSummary)
	if err != nil {
		return err
	}
	dataPoint := metric.Summary().DataPoints().AppendEmpty()
	attributes.CopyTo(dataPoint.Attributes())
	dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(ts))
	// set start_time, if exists and is RFC3339
	// used by statsd input plugin
	if startTimeObj, ok := fields["start_time"]; ok {
		if startTimeStr, ok := startTimeObj.(string); ok {
			if t, err := time.Parse(time.RFC3339, startTimeStr); err == nil {
				dataPoint.SetStartTimestamp(pcommon.NewTimestampFromTime(t))
			}
		}
	}

	dataPoint.SetCount(count)
	dataPoint.SetSum(sum)
	quantileValues.MoveAndAppendTo(dataPoint.QuantileValues())
	return nil
}
