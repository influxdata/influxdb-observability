package influx2otel

import (
	"fmt"
	"sort"
	"strconv"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

type metricsBatchPrometheusV1 struct {
	rmByAttributes         map[string]*otlpmetrics.ResourceMetrics
	ilmByRMAttributesAndIL map[string]map[string]*otlpmetrics.InstrumentationLibraryMetrics
	metricByRMIL           map[string]map[string]map[string]*otlpmetrics.Metric

	logger common.Logger
}

func newMetricsBatchPrometheusV1(logger common.Logger) MetricsBatch {
	return &metricsBatchPrometheusV1{
		rmByAttributes:         make(map[string]*otlpmetrics.ResourceMetrics),
		ilmByRMAttributesAndIL: make(map[string]map[string]*otlpmetrics.InstrumentationLibraryMetrics),
		metricByRMIL:           make(map[string]map[string]map[string]*otlpmetrics.Metric),

		logger: logger,
	}
}

func (b *metricsBatchPrometheusV1) ToProto() []*otlpmetrics.ResourceMetrics {
	var resourceMetricss []*otlpmetrics.ResourceMetrics
	for _, resourceMetrics := range b.rmByAttributes {

		// Ensure that the extra bucket counts have been added.
		for _, ilMetrics := range resourceMetrics.InstrumentationLibraryMetrics {
			for _, metric := range ilMetrics.Metrics {
				if histogram, ok := metric.Data.(*otlpmetrics.Metric_DoubleHistogram); ok {
					for _, dp := range histogram.DoubleHistogram.DataPoints {
						if len(dp.BucketCounts) == len(dp.ExplicitBounds) {
							dp.BucketCounts = append(dp.BucketCounts, dp.Count)
						}
					}
				}
			}
		}

		resourceMetricss = append(resourceMetricss, resourceMetrics)
	}
	return resourceMetricss
}

func (b *metricsBatchPrometheusV1) ToProtoBytes() ([]byte, error) {
	req := otlpcollectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: b.ToProto(),
	}
	return proto.Marshal(&req)
}

func (b *metricsBatchPrometheusV1) AddPoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	vType = b.inferMetricValueType(vType, fields)
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
		err = b.convertGauge(metric, labels, fields, ts)
	case common.InfluxMetricValueTypeSum:
		err = b.convertSum(metric, labels, fields, ts)
	case common.InfluxMetricValueTypeHistogram:
		err = b.convertHistogram(metric, labels, fields, ts)
	case common.InfluxMetricValueTypeSummary:
		err = b.convertSummary(metric, labels, fields, ts)
	default:
		err = fmt.Errorf("impossible InfluxMetricValueType %d", vType)
	}

	return err
}

// unpackTags extracts resource attributes and instrumentation library name and version from tags.
// Return values are (metric name, resource attributes, IL name, IL version, labels).
func (b *metricsBatchPrometheusV1) unpackTags(tags map[string]string) (rAttributes []*otlpcommon.KeyValue, ilName string, ilVersion string, labels []*otlpcommon.StringKeyValue) {
	attributeKeys := make(map[string]struct{})
	for k, v := range tags {
		switch {
		case k == common.AttributeInstrumentationLibraryName:
			ilName = v
		case k == common.AttributeInstrumentationLibraryVersion:
			ilVersion = v
		case common.ResourceNamespace.MatchString(k):
			rAttributes = append(rAttributes, &otlpcommon.KeyValue{
				Key:   k,
				Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: v}},
			})
			attributeKeys[k] = struct{}{}
		default:
			labels = append(labels, &otlpcommon.StringKeyValue{
				Key:   k,
				Value: v,
			})
		}
	}

	sort.Slice(rAttributes, func(i, j int) bool {
		return rAttributes[i].Key < rAttributes[j].Key
	})

	return
}

func (b *metricsBatchPrometheusV1) lookupMetric(metricName string, rAttributes []*otlpcommon.KeyValue, ilName, ilVersion string, vType common.InfluxMetricValueType) (*otlpmetrics.Metric, error) {
	rKey := resourceAttributesToKey(rAttributes)
	var resourceMetrics *otlpmetrics.ResourceMetrics
	if rm, found := b.rmByAttributes[rKey]; found {
		resourceMetrics = rm
	} else {
		resourceMetrics = &otlpmetrics.ResourceMetrics{
			Resource: &otlpresource.Resource{
				Attributes: rAttributes,
			},
		}
		b.rmByAttributes[rKey] = resourceMetrics
		b.ilmByRMAttributesAndIL[rKey] = make(map[string]*otlpmetrics.InstrumentationLibraryMetrics)
		b.metricByRMIL[rKey] = make(map[string]map[string]*otlpmetrics.Metric)
	}

	ilmKey := ilName + ":" + ilVersion
	var ilMetrics *otlpmetrics.InstrumentationLibraryMetrics
	if ilm, found := b.ilmByRMAttributesAndIL[rKey][ilmKey]; found {
		ilMetrics = ilm
	} else {
		ilMetrics = &otlpmetrics.InstrumentationLibraryMetrics{
			InstrumentationLibrary: &otlpcommon.InstrumentationLibrary{
				Name:    ilName,
				Version: ilVersion,
			},
		}
		resourceMetrics.InstrumentationLibraryMetrics = append(resourceMetrics.InstrumentationLibraryMetrics, ilMetrics)
		b.ilmByRMAttributesAndIL[rKey][ilmKey] = ilMetrics
		b.metricByRMIL[rKey][ilmKey] = make(map[string]*otlpmetrics.Metric)
	}

	var metric *otlpmetrics.Metric
	if m, found := b.metricByRMIL[rKey][ilmKey][metricName]; found {
		switch m.Data.(type) {
		case *otlpmetrics.Metric_DoubleGauge:
			if vType != common.InfluxMetricValueTypeGauge && vType != common.InfluxMetricValueTypeUntyped {
				return nil, fmt.Errorf("value type conflict for metric '%s'; expected '%s' or '%s', got '%s", metricName, common.InfluxMetricValueTypeGauge, common.InfluxMetricValueTypeUntyped, vType)
			}
		case *otlpmetrics.Metric_DoubleSum:
			if vType != common.InfluxMetricValueTypeSum {
				return nil, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s", metricName, common.InfluxMetricValueTypeSum, vType)
			}
		case *otlpmetrics.Metric_DoubleHistogram:
			if vType != common.InfluxMetricValueTypeHistogram {
				return nil, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s", metricName, common.InfluxMetricValueTypeHistogram, vType)
			}
		case *otlpmetrics.Metric_DoubleSummary:
			if vType != common.InfluxMetricValueTypeSummary {
				return nil, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s", metricName, common.InfluxMetricValueTypeSummary, vType)
			}
		default:
			return nil, fmt.Errorf("impossible InfluxMetricValueType %d", vType)
		}
		metric = m

	} else {
		switch vType {
		case common.InfluxMetricValueTypeGauge, common.InfluxMetricValueTypeUntyped:
			metric = &otlpmetrics.Metric{
				Name: metricName,
				Data: &otlpmetrics.Metric_DoubleGauge{
					DoubleGauge: &otlpmetrics.DoubleGauge{
						DataPoints: make([]*otlpmetrics.DoubleDataPoint, 0, 1),
					},
				},
			}
		case common.InfluxMetricValueTypeSum:
			metric = &otlpmetrics.Metric{
				Name: metricName,
				Data: &otlpmetrics.Metric_DoubleSum{
					DoubleSum: &otlpmetrics.DoubleSum{
						DataPoints:             make([]*otlpmetrics.DoubleDataPoint, 0, 1),
						AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
						IsMonotonic:            true,
					},
				},
			}
		case common.InfluxMetricValueTypeHistogram:
			metric = &otlpmetrics.Metric{
				Name: metricName,
				Data: &otlpmetrics.Metric_DoubleHistogram{
					DoubleHistogram: &otlpmetrics.DoubleHistogram{
						DataPoints:             make([]*otlpmetrics.DoubleHistogramDataPoint, 0, 1),
						AggregationTemporality: otlpmetrics.AggregationTemporality_AGGREGATION_TEMPORALITY_CUMULATIVE,
					},
				},
			}
		case common.InfluxMetricValueTypeSummary:
			metric = &otlpmetrics.Metric{
				Name: metricName,
				Data: &otlpmetrics.Metric_DoubleSummary{
					DoubleSummary: &otlpmetrics.DoubleSummary{
						DataPoints: make([]*otlpmetrics.DoubleSummaryDataPoint, 0, 1),
					},
				},
			}
		default:
			return nil, fmt.Errorf("unrecognized InfluxMetricValueType %d", vType)
		}
		b.ilmByRMAttributesAndIL[rKey][ilmKey].Metrics = append(b.ilmByRMAttributesAndIL[rKey][ilmKey].Metrics, metric)
		b.metricByRMIL[rKey][ilmKey][metricName] = metric
	}

	return metric, nil
}

func (b *metricsBatchPrometheusV1) inferMetricValueType(vType common.InfluxMetricValueType, fields map[string]interface{}) common.InfluxMetricValueType {
	if vType == common.InfluxMetricValueTypeUntyped {
		for k := range fields {
			if _, found := fields[common.MetricGaugeFieldKey]; found {
				vType = common.InfluxMetricValueTypeGauge
				break
			} else if _, found = fields[common.MetricCounterFieldKey]; found {
				vType = common.InfluxMetricValueTypeSum
				break
			} else if k == common.MetricHistogramCountFieldKey || k == common.MetricHistogramSumFieldKey || isStringNumeric(k) {
				// We cannot reliably distinguish between histogram and summary
				// without knowing we have all points, so assume/cast histogram.
				vType = common.InfluxMetricValueTypeHistogram
				break
			}
		}
	}
	return vType
}

func (b *metricsBatchPrometheusV1) convertGauge(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
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

func (b *metricsBatchPrometheusV1) convertSum(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
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

func (b *metricsBatchPrometheusV1) convertHistogram(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
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

func (b *metricsBatchPrometheusV1) convertSummary(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
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
