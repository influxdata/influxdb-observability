package influx2otel

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"go.opentelemetry.io/collector/model/pdata"
)

func (b *MetricsBatch) addPointTelegrafPrometheusV2(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	if measurement != common.MeasurementPrometheus {
		return fmt.Errorf("unexpected measurement name '%s'", measurement)
	}

	vType = b.inferMetricValueTypeV2(vType, tags, fields)
	if vType == common.InfluxMetricValueTypeUntyped {
		return errValueTypeUnknown
	}

	if ts.IsZero() {
		ts = time.Now()
	}

	switch vType {
	case common.InfluxMetricValueTypeGauge:
		return b.convertGaugeV2(tags, fields, ts)
	case common.InfluxMetricValueTypeSum:
		return b.convertSumV2(tags, fields, ts)
	case common.InfluxMetricValueTypeHistogram:
		return b.convertHistogramV2(tags, fields, ts)
	case common.InfluxMetricValueTypeSummary:
		return b.convertSummaryV2(tags, fields, ts)
	default:
		return fmt.Errorf("impossible InfluxMetricValueType %d", vType)
	}
}

func (b *MetricsBatch) inferMetricValueTypeV2(vType common.InfluxMetricValueType, tags map[string]string, fields map[string]interface{}) common.InfluxMetricValueType {
	if vType != common.InfluxMetricValueTypeUntyped {
		return vType
	}
	for k := range tags {
		if k == common.MetricHistogramBoundKeyV2 || k == common.MetricSummaryQuantileKeyV2 {
			return common.InfluxMetricValueTypeHistogram
		}
	}
	for k := range fields {
		if strings.HasSuffix(k, common.MetricHistogramCountSuffix) || strings.HasSuffix(k, common.MetricHistogramSumSuffix) {
			return common.InfluxMetricValueTypeHistogram
		}
	}
	if len(fields) == 1 {
		return common.InfluxMetricValueTypeGauge
	}
	return common.InfluxMetricValueTypeUntyped
}

type dataPointKey string

func newDataPointKey(unixNanos uint64, attributes pdata.AttributeMap) dataPointKey {
	attributes.Sort()
	components := make([]string, 0, attributes.Len()*2+1)
	components = append(components, strconv.FormatUint(unixNanos, 32))
	var err error
	attributes.Range(func(k string, v pdata.AttributeValue) bool {
		var vv string
		vv, err = common.AttributeValueToInfluxTagValue(v)
		if err != nil {
			return false
		}
		components = append(components, k, vv)
		return true
	})
	return dataPointKey(strings.Join(components, ":"))
}

func (b *MetricsBatch) convertGaugeV2(tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	if len(fields) != 1 {
		return fmt.Errorf("gauge metric should have 1 field, found %d", len(fields))
	}

	var metricName string
	var floatValue *float64
	var intValue *int64
	for k, fieldValue := range fields {
		metricName = k
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
	}

	metric, attributes, err := b.lookupMetric(metricName, tags, common.InfluxMetricValueTypeGauge)
	if err != nil {
		return err
	}
	dataPoint := metric.Gauge().DataPoints().AppendEmpty()
	attributes.CopyTo(dataPoint.Attributes())
	dataPoint.SetTimestamp(pdata.NewTimestampFromTime(ts))
	if floatValue != nil {
		dataPoint.SetDoubleVal(*floatValue)
	} else if intValue != nil {
		dataPoint.SetIntVal(*intValue)
	} else {
		panic("unreachable")
	}
	return nil
}

func (b *MetricsBatch) convertSumV2(tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	if len(fields) != 1 {
		return fmt.Errorf("sum metric should have 1 field, found %d", len(fields))
	}

	var metricName string
	var floatValue *float64
	var intValue *int64
	for k, fieldValue := range fields {
		metricName = k
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
	}

	metric, attributes, err := b.lookupMetric(metricName, tags, common.InfluxMetricValueTypeSum)
	if err != nil {
		return err
	}
	dataPoint := metric.Sum().DataPoints().AppendEmpty()
	attributes.CopyTo(dataPoint.Attributes())
	dataPoint.SetTimestamp(pdata.NewTimestampFromTime(ts))
	if floatValue != nil {
		dataPoint.SetDoubleVal(*floatValue)
	} else if intValue != nil {
		dataPoint.SetIntVal(*intValue)
	} else {
		panic("unreachable")
	}
	return nil
}

func (b *MetricsBatch) convertHistogramV2(tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	var metricName string
	if _, found := tags[common.MetricHistogramBoundKeyV2]; found {
		if len(fields) != 1 {
			return fmt.Errorf("histogram metric 'le' tagged line should have 1 field, found %d", len(fields))
		}
		for k := range fields {
			metricName = strings.TrimSuffix(k, common.MetricHistogramBucketSuffix)
		}
	} else if _, found = tags[common.MetricSummaryQuantileKeyV2]; found {
		if len(fields) != 1 {
			return fmt.Errorf("summary metric (interpreted as histogram) 'quantile' tagged line should have 1 field, found %d", len(fields))
		}
		for k := range fields {
			metricName = k
		}
	} else {
		if len(fields) != 2 {
			return fmt.Errorf("histogram metric count+sum fields should have two values, found %d", len(fields))
		}
		for k := range fields {
			if strings.HasSuffix(k, common.MetricHistogramCountSuffix) {
				metricName = strings.TrimSuffix(k, common.MetricHistogramCountSuffix)
			} else if strings.HasSuffix(k, common.MetricHistogramSumSuffix) {
				metricName = strings.TrimSuffix(k, common.MetricHistogramSumSuffix)
			} else {
				return fmt.Errorf("histogram count+sum field lacks _count or _sum suffix, found '%s'", k)
			}
		}
	}

	metric, attributes, err := b.lookupMetric(metricName, tags, common.InfluxMetricValueTypeHistogram)
	if err != nil {
		return err
	}

	dpk := newDataPointKey(uint64(ts.UnixNano()), attributes)
	dataPoint, found := b.histogramDataPointsByMDPK[metric][dpk]
	if !found {
		dataPoint = metric.Histogram().DataPoints().AppendEmpty()
		attributes.CopyTo(dataPoint.Attributes())
		dataPoint.SetTimestamp(pdata.NewTimestampFromTime(ts))
		b.histogramDataPointsByMDPK[metric][dpk] = dataPoint
	}

	if sExplicitBound, found := tags[common.MetricHistogramBoundKeyV2]; found {
		if iBucketCount, found := fields[metric.Name()+common.MetricHistogramBucketSuffix]; found {
			explicitBound, err := strconv.ParseFloat(sExplicitBound, 64)
			if err != nil {
				return fmt.Errorf("invalid value for histogram bucket bound: '%s'", sExplicitBound)
			}
			bucketCount, ok := iBucketCount.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for histogram bucket count: %q", iBucketCount, iBucketCount)
			}
			dataPoint.SetExplicitBounds(append(dataPoint.ExplicitBounds(), explicitBound))
			dataPoint.SetBucketCounts(append(dataPoint.BucketCounts(), uint64(bucketCount)))
		} else {
			return fmt.Errorf("histogram bucket bound has no matching count")
		}
	} else if _, found = fields[metric.Name()+common.MetricHistogramBucketSuffix]; found {
		return fmt.Errorf("histogram bucket count has no matching bound")
	}

	if sQuantile, found := tags[common.MetricSummaryQuantileKeyV2]; found {
		if iValue, found := fields[metric.Name()]; found {
			quantile, err := strconv.ParseFloat(sQuantile, 64)
			if err != nil {
				return fmt.Errorf("invalid value for summary (interpreted as histogram) quantile: '%s'", sQuantile)
			}
			value, ok := iValue.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for summary (interpreted as histogram) quantile value: %q", iValue, iValue)
			}
			dataPoint.SetExplicitBounds(append(dataPoint.ExplicitBounds(), quantile))
			dataPoint.SetBucketCounts(append(dataPoint.BucketCounts(), uint64(value)))
		} else {
			return fmt.Errorf("summary (interpreted as histogram) quantile has no matching value")
		}
	} else if _, found = fields[metric.Name()]; found {
		return fmt.Errorf("summary (interpreted as histogram) quantile value has no matching quantile")
	}

	if iCount, found := fields[metric.Name()+common.MetricHistogramCountSuffix]; found {
		if iSum, found := fields[metric.Name()+common.MetricHistogramSumSuffix]; found {
			count, ok := iCount.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for histogram count %q", iCount, iCount)
			}
			sum, ok := iSum.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for histogram sum %q", iSum, iSum)
			}

			dataPoint.SetCount(uint64(count))
			dataPoint.SetSum(sum)
		} else {
			return fmt.Errorf("histogram count has no matching sum")
		}
	} else if _, found = fields[metric.Name()+common.MetricHistogramSumSuffix]; found {
		return fmt.Errorf("histogram sum has no matching count")
	}

	return nil
}

func (b *MetricsBatch) convertSummaryV2(tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	var metricName string
	if _, found := tags[common.MetricSummaryQuantileKeyV2]; found {
		if len(fields) != 1 {
			return fmt.Errorf("summary metric 'quantile' tagged line should have 1 field, found %d", len(fields))
		}
		for k := range fields {
			metricName = k
		}
	} else {
		if len(fields) != 2 {
			return fmt.Errorf("summary metric count+sum fields should have two values, found %d", len(fields))
		}
		for k := range fields {
			if strings.HasSuffix(k, common.MetricSummaryCountSuffix) {
				metricName = strings.TrimSuffix(k, common.MetricSummaryCountSuffix)
			} else if strings.HasSuffix(k, common.MetricSummarySumSuffix) {
				metricName = strings.TrimSuffix(k, common.MetricSummarySumSuffix)
			} else {
				return fmt.Errorf("summary count+sum field lacks _count or _sum suffix, found '%s'", k)
			}
		}
	}

	metric, attributes, err := b.lookupMetric(metricName, tags, common.InfluxMetricValueTypeSummary)
	if err != nil {
		return err
	}

	dpk := newDataPointKey(uint64(ts.UnixNano()), attributes)
	dataPoint, found := b.summaryDataPointsByMDPK[metric][dpk]
	if !found {
		dataPoint = metric.Summary().DataPoints().AppendEmpty()
		attributes.CopyTo(dataPoint.Attributes())
		dataPoint.SetTimestamp(pdata.NewTimestampFromTime(ts))
		b.summaryDataPointsByMDPK[metric][dpk] = dataPoint
	}

	if sQuantile, found := tags[common.MetricSummaryQuantileKeyV2]; found {
		if iValue, found := fields[metric.Name()]; found {
			quantile, err := strconv.ParseFloat(sQuantile, 64)
			if err != nil {
				return fmt.Errorf("invalid value for summary quantile: '%s'", sQuantile)
			}
			value, ok := iValue.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for summary quantile value: %q", iValue, iValue)
			}
			valueAtQuantile := dataPoint.QuantileValues().AppendEmpty()
			valueAtQuantile.SetQuantile(quantile)
			valueAtQuantile.SetValue(value)
		} else {
			return fmt.Errorf("summary quantile has no matching value")
		}
	} else if _, found = fields[metric.Name()]; found {
		return fmt.Errorf("summary quantile value has no matching quantile")
	}

	if iCount, found := fields[metric.Name()+common.MetricSummaryCountSuffix]; found {
		if iSum, found := fields[metric.Name()+common.MetricSummarySumSuffix]; found {
			count, ok := iCount.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for summary count %q", iCount, iCount)
			}
			sum, ok := iSum.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for summary sum %q", iSum, iSum)
			}

			dataPoint.SetCount(uint64(count))
			dataPoint.SetSum(sum)
		} else {
			return fmt.Errorf("summary count has no matching sum")
		}
	} else if _, found = fields[metric.Name()+common.MetricSummarySumSuffix]; found {
		return fmt.Errorf("summary sum has no matching count")
	}

	return nil
}
