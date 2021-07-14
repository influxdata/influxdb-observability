package influx2otel

import (
	"fmt"
	"strconv"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
)

func (b *MetricsBatch) addPointTelegrafPrometheusV1(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	vType = b.inferMetricValueTypeV1(vType, fields)
	if vType == common.InfluxMetricValueTypeUntyped {
		return errValueTypeUnknown
	}

	rAttributes, ilName, ilVersion, labels := b.unpackTags(tags)

	metric, err := b.lookupMetric(measurement, rAttributes, ilName, ilVersion, vType)
	if err != nil {
		return err
	}
	if ts.IsZero() {
		ts = time.Now()
	}

	switch vType {
	case common.InfluxMetricValueTypeGauge:
		err = b.convertGaugeV1(metric, labels, fields, ts)
	case common.InfluxMetricValueTypeSum:
		err = b.convertSumV1(metric, labels, fields, ts)
	case common.InfluxMetricValueTypeHistogram:
		err = b.convertHistogramV1(metric, labels, fields, ts)
	case common.InfluxMetricValueTypeSummary:
		err = b.convertSummaryV1(metric, labels, fields, ts)
	default:
		err = fmt.Errorf("impossible InfluxMetricValueType %d", vType)
	}

	return err
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

func (b *MetricsBatch) convertGaugeV1(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
	var gauge float64
	foundGauge := false
	for k, vi := range fields {
		if k == common.MetricGaugeFieldKey {
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

func (b *MetricsBatch) convertSumV1(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
	var counter float64
	foundCounter := false
	for k, vi := range fields {
		if k == common.MetricCounterFieldKey {
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

func (b *MetricsBatch) convertHistogramV1(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
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

		} else {
			b.logger.Debug("skipping unrecognized histogram field '%s'=%q", k, vi)
		}
	}
	if !foundCount {
		return fmt.Errorf("histogram count field not found")
	}
	if !foundSum {
		return fmt.Errorf("histogram sum field not found")
	}

	bucketCounts = append(bucketCounts, count)

	dataPoint := &otlpmetrics.DoubleHistogramDataPoint{
		Labels:         labels,
		TimeUnixNano:   uint64(ts.UnixNano()),
		Count:          count,
		Sum:            sum,
		BucketCounts:   bucketCounts,
		ExplicitBounds: explicitBounds,
	}
	metric.Data.(*otlpmetrics.Metric_DoubleHistogram).DoubleHistogram.DataPoints =
		append(metric.Data.(*otlpmetrics.Metric_DoubleHistogram).DoubleHistogram.DataPoints,
			dataPoint)

	return nil
}

func (b *MetricsBatch) convertSummaryV1(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
	var count uint64
	foundCount := false
	var sum float64
	foundSum := false
	var quantileValues []*otlpmetrics.DoubleSummaryDataPoint_ValueAtQuantile

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
				quantileValues = append(quantileValues, &otlpmetrics.DoubleSummaryDataPoint_ValueAtQuantile{
					Quantile: quantile,
					Value:    value,
				})
			}

		} else {
			b.logger.Debug("skipping unrecognized summary field '%s'=%q", k, vi)
		}
	}
	if !foundCount {
		return fmt.Errorf("summary count not found")
	}
	if !foundSum {
		return fmt.Errorf("summary sum not found")
	}

	dataPoint := &otlpmetrics.DoubleSummaryDataPoint{
		Labels:         labels,
		TimeUnixNano:   uint64(ts.UnixNano()),
		Count:          count,
		Sum:            sum,
		QuantileValues: quantileValues,
	}
	metric.Data.(*otlpmetrics.Metric_DoubleSummary).DoubleSummary.DataPoints =
		append(metric.Data.(*otlpmetrics.Metric_DoubleSummary).DoubleSummary.DataPoints,
			dataPoint)

	return nil
}
