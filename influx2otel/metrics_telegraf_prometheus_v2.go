package influx2otel

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
)

func (b *MetricsBatch) addPointTelegrafPrometheusV2(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	if measurement != common.MeasurementPrometheus {
		return fmt.Errorf("unexpected measurement name '%s'", measurement)
	}

	vType = b.inferMetricValueTypeV2(vType, tags, fields)
	if vType == common.InfluxMetricValueTypeUntyped {
		return errValueTypeUnknown
	}

	metricName, err := b.getMetricName(vType, tags, fields)
	if err != nil {
		return err
	}

	rAttributes, ilName, ilVersion, labels := b.unpackTags(tags)

	metric, err := b.lookupMetric(metricName, rAttributes, ilName, ilVersion, vType)
	if err != nil {
		return err
	}
	if ts.IsZero() {
		ts = time.Now()
	}

	switch vType {
	case common.InfluxMetricValueTypeGauge:
		err = b.convertGauge(metric, labels, fields, ts)
	case common.InfluxMetricValueTypeSum:
		err = b.convertSum(metric, labels, fields, ts)
	case common.InfluxMetricValueTypeHistogram:
		err = b.convertHistogram(metric, labels, tags, fields, ts)
	case common.InfluxMetricValueTypeSummary:
		err = b.convertSummary(metric, labels, tags, fields, ts)
	default:
		err = fmt.Errorf("impossible InfluxMetricValueType %d", vType)
	}

	return err
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

func (b *MetricsBatch) getMetricName(vType common.InfluxMetricValueType, tags map[string]string, fields map[string]interface{}) (metricName string, err error) {
	switch vType {
	case common.InfluxMetricValueTypeGauge:
		if len(fields) != 1 {
			return "", fmt.Errorf("gauge metric should have 1 field, found %d", len(fields))
		}
		for k := range fields {
			metricName = k
		}

	case common.InfluxMetricValueTypeSum:
		if len(fields) != 1 {
			return "", fmt.Errorf("sum metric should have 1 field, found %d", len(fields))
		}
		for k := range fields {
			metricName = k
		}

	case common.InfluxMetricValueTypeHistogram:
		if _, found := tags[common.MetricHistogramBoundKeyV2]; found {
			if len(fields) != 1 {
				return "", fmt.Errorf("histogram metric 'le' tagged line should have 1 field, found %d", len(fields))
			}
			for k := range fields {
				metricName = strings.TrimSuffix(k, common.MetricHistogramBucketSuffix)
			}
		} else if _, found = tags[common.MetricSummaryQuantileKeyV2]; found {
			if len(fields) != 1 {
				return "", fmt.Errorf("summary metric (interpreted as histogram) 'quantile' tagged line should have 1 field, found %d", len(fields))
			}
			for k := range fields {
				metricName = k
			}
		} else {
			if len(fields) != 2 {
				return "", fmt.Errorf("histogram metric count+sum fields should have two values, found %d", len(fields))
			}
			for k := range fields {
				if strings.HasSuffix(k, common.MetricHistogramCountSuffix) {
					metricName = strings.TrimSuffix(k, common.MetricHistogramCountSuffix)
				} else if strings.HasSuffix(k, common.MetricHistogramSumSuffix) {
					metricName = strings.TrimSuffix(k, common.MetricHistogramSumSuffix)
				} else {
					return "", fmt.Errorf("histogram count+sum field lacks _count or _sum suffix, found '%s'", k)
				}
			}
		}

	case common.InfluxMetricValueTypeSummary:
		if _, found := tags[common.MetricSummaryQuantileKeyV2]; found {
			if len(fields) != 1 {
				return "", fmt.Errorf("summary metric 'quantile' tagged line should have 1 field, found %d", len(fields))
			}
			for k := range fields {
				metricName = k
			}
		} else {
			if len(fields) != 2 {
				return "", fmt.Errorf("summary metric count+sum fields should have two values, found %d", len(fields))
			}
			for k := range fields {
				if strings.HasSuffix(k, common.MetricSummaryCountSuffix) {
					metricName = strings.TrimSuffix(k, common.MetricSummaryCountSuffix)
				} else if strings.HasSuffix(k, common.MetricSummarySumSuffix) {
					metricName = strings.TrimSuffix(k, common.MetricSummarySumSuffix)
				} else {
					return "", fmt.Errorf("summary count+sum field lacks _count or _sum suffix, found '%s'", k)
				}
			}
		}
	}

	if metricName == "" {
		return "", errors.New("metric name not found, not sure why")
	}

	return
}

type dataPointKey string

func newDataPointKey(unixNanos uint64, labels []*otlpcommon.StringKeyValue) dataPointKey {
	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Key < labels[j].Key
	})
	components := make([]string, 0, len(labels)*2+1)
	components = append(components, strconv.FormatUint(unixNanos, 36))
	for _, label := range labels {
		components = append(components, label.Key, label.Value)
	}
	return dataPointKey(strings.Join(components, ":"))
}

func (b *MetricsBatch) convertGauge(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
	var gauge float64
	foundGauge := false
	for k, vi := range fields {
		if k == metric.Name {
			foundGauge = true
			var ok bool
			if gauge, ok = vi.(float64); !ok {
				return fmt.Errorf("unsupported gauge value type %T", vi)
			}

		} else {
			b.logger.Debug("skipping unrecognized gauge field '%s'=%q", k, vi)
		}
	}
	if !foundGauge {
		return fmt.Errorf("gauge field not found")
	}

	dataPoint := &otlpmetrics.DoubleDataPoint{
		Labels:       labels,
		TimeUnixNano: uint64(ts.UnixNano()),
		Value:        gauge,
	}
	metric.Data.(*otlpmetrics.Metric_DoubleGauge).DoubleGauge.DataPoints =
		append(metric.Data.(*otlpmetrics.Metric_DoubleGauge).DoubleGauge.DataPoints,
			dataPoint)

	return nil
}

func (b *MetricsBatch) convertSum(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
	var counter float64
	foundCounter := false
	for k, vi := range fields {
		if k == metric.Name {
			foundCounter = true
			var ok bool
			if counter, ok = vi.(float64); !ok {
				return fmt.Errorf("unsupported counter value type %T", vi)
			}

		} else {
			b.logger.Debug("skipping unrecognized counter field '%s'=%q", k, vi)
		}
	}
	if !foundCounter {
		return fmt.Errorf("counter field not found")
	}

	dataPoint := &otlpmetrics.DoubleDataPoint{
		Labels:       labels,
		TimeUnixNano: uint64(ts.UnixNano()),
		Value:        counter,
	}
	metric.Data.(*otlpmetrics.Metric_DoubleSum).DoubleSum.DataPoints =
		append(metric.Data.(*otlpmetrics.Metric_DoubleSum).DoubleSum.DataPoints,
			dataPoint)

	return nil
}

func (b *MetricsBatch) convertHistogram(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	var dataPoint *otlpmetrics.DoubleHistogramDataPoint
	{
		dpk := newDataPointKey(uint64(ts.UnixNano()), labels)
		var found bool
		if dataPoint, found = b.histogramDataPointsByMDPK[metric][dpk]; !found {
			dataPoint = &otlpmetrics.DoubleHistogramDataPoint{
				Labels:       labels,
				TimeUnixNano: uint64(ts.UnixNano()),
			}
			b.histogramDataPointsByMDPK[metric][dpk] = dataPoint
			metric.Data.(*otlpmetrics.Metric_DoubleHistogram).DoubleHistogram.DataPoints =
				append(metric.Data.(*otlpmetrics.Metric_DoubleHistogram).DoubleHistogram.DataPoints,
					dataPoint)
		}
	}

	if sExplicitBound, found := tags[common.MetricHistogramBoundKeyV2]; found {
		if iBucketCount, found := fields[metric.Name+common.MetricHistogramBucketSuffix]; found {
			explicitBound, err := strconv.ParseFloat(sExplicitBound, 64)
			if err != nil {
				return fmt.Errorf("invalid value for histogram bucket bound: '%s'", sExplicitBound)
			}
			bucketCount, ok := iBucketCount.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for histogram bucket count: %q", iBucketCount, iBucketCount)
			}

			dataPoint.ExplicitBounds = append(dataPoint.ExplicitBounds, explicitBound)
			dataPoint.BucketCounts = append(dataPoint.BucketCounts, uint64(bucketCount))
		} else {
			return fmt.Errorf("histogram bucket bound has no matching count")
		}
	} else if _, found = fields[metric.Name+common.MetricHistogramBucketSuffix]; found {
		return fmt.Errorf("histogram bucket count has no matching bound")
	}

	if sQuantile, found := tags[common.MetricSummaryQuantileKeyV2]; found {
		if iValue, found := fields[metric.Name]; found {
			quantile, err := strconv.ParseFloat(sQuantile, 64)
			if err != nil {
				return fmt.Errorf("invalid value for summary (interpreted as histogram) quantile: '%s'", sQuantile)
			}
			value, ok := iValue.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for summary (interpreted as histogram) quantile value: %q", iValue, iValue)
			}

			dataPoint.ExplicitBounds = append(dataPoint.ExplicitBounds, quantile)
			dataPoint.BucketCounts = append(dataPoint.BucketCounts, uint64(value))
		} else {
			return fmt.Errorf("summary (interpreted as histogram) quantile has no matching value")
		}
	} else if _, found = fields[metric.Name]; found {
		return fmt.Errorf("summary (interpreted as histogram) quantile value has no matching quantile")
	}

	if iCount, found := fields[metric.Name+common.MetricHistogramCountSuffix]; found {
		if iSum, found := fields[metric.Name+common.MetricHistogramSumSuffix]; found {
			count, ok := iCount.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for histogram count %q", iCount, iCount)
			}
			sum, ok := iSum.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for histogram sum %q", iSum, iSum)
			}

			dataPoint.Count = uint64(count)
			dataPoint.Sum = sum
		} else {
			return fmt.Errorf("histogram count has no matching sum")
		}
	} else if _, found = fields[metric.Name+common.MetricHistogramSumSuffix]; found {
		return fmt.Errorf("histogram sum has no matching count")
	}

	return nil
}

func (b *MetricsBatch) convertSummary(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	var dataPoint *otlpmetrics.DoubleSummaryDataPoint
	{
		dpk := newDataPointKey(uint64(ts.UnixNano()), labels)
		var found bool
		if dataPoint, found = b.summaryDataPointsByMDPK[metric][dpk]; !found {
			dataPoint = &otlpmetrics.DoubleSummaryDataPoint{
				Labels:       labels,
				TimeUnixNano: uint64(ts.UnixNano()),
			}
			b.summaryDataPointsByMDPK[metric][dpk] = dataPoint
			metric.Data.(*otlpmetrics.Metric_DoubleSummary).DoubleSummary.DataPoints =
				append(metric.Data.(*otlpmetrics.Metric_DoubleSummary).DoubleSummary.DataPoints,
					dataPoint)
		}
	}

	if sQuantile, found := tags[common.MetricSummaryQuantileKeyV2]; found {
		if iValue, found := fields[metric.Name]; found {
			quantile, err := strconv.ParseFloat(sQuantile, 64)
			if err != nil {
				return fmt.Errorf("invalid value for summary quantile: '%s'", sQuantile)
			}
			value, ok := iValue.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for summary quantile value: %q", iValue, iValue)
			}

			dataPoint.QuantileValues =
				append(dataPoint.QuantileValues,
					&otlpmetrics.DoubleSummaryDataPoint_ValueAtQuantile{
						Quantile: quantile,
						Value:    value,
					})
		} else {
			return fmt.Errorf("summary quantile has no matching value")
		}
	} else if _, found = fields[metric.Name]; found {
		return fmt.Errorf("summary quantile value has no matching quantile")
	}

	if iCount, found := fields[metric.Name+common.MetricSummaryCountSuffix]; found {
		if iSum, found := fields[metric.Name+common.MetricSummarySumSuffix]; found {
			count, ok := iCount.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for summary count %q", iCount, iCount)
			}
			sum, ok := iSum.(float64)
			if !ok {
				return fmt.Errorf("invalid value type %T for summary sum %q", iSum, iSum)
			}

			dataPoint.Count = uint64(count)
			dataPoint.Sum = sum
		} else {
			return fmt.Errorf("summary count has no matching sum")
		}
	} else if _, found = fields[metric.Name+common.MetricHistogramSumSuffix]; found {
		return fmt.Errorf("summary sum has no matching count")
	}

	return nil
}
