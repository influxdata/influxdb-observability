package influx2otel

import (
	"errors"
	"fmt"
	"sort"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
)

type metricsBatchPrometheusV2 struct {
	rmByAttributes map[string]*otlpmetrics.ResourceMetrics
	// TODO convert some of these maps from string:foo to pointer:foo
	ilmByRMAttributesAndIL    map[string]map[string]*otlpmetrics.InstrumentationLibraryMetrics
	metricByRMIL              map[string]map[string]map[string]*otlpmetrics.Metric
	histogramDataPointsByMDPK map[*otlpmetrics.Metric]map[dataPointKey]*otlpmetrics.DoubleHistogramDataPoint
	summaryDataPointsByMDPK   map[*otlpmetrics.Metric]map[dataPointKey]*otlpmetrics.DoubleSummaryDataPoint

	logger common.Logger
}

func newmetricsBatchPrometheusV2(logger common.Logger) MetricsBatch {
	return &metricsBatchPrometheusV2{
		rmByAttributes:            make(map[string]*otlpmetrics.ResourceMetrics),
		ilmByRMAttributesAndIL:    make(map[string]map[string]*otlpmetrics.InstrumentationLibraryMetrics),
		metricByRMIL:              make(map[string]map[string]map[string]*otlpmetrics.Metric),
		histogramDataPointsByMDPK: make(map[*otlpmetrics.Metric]map[dataPointKey]*otlpmetrics.DoubleHistogramDataPoint),
		summaryDataPointsByMDPK:   make(map[*otlpmetrics.Metric]map[dataPointKey]*otlpmetrics.DoubleSummaryDataPoint),

		logger: logger,
	}
}

func (b *metricsBatchPrometheusV2) ToProto() []*otlpmetrics.ResourceMetrics {
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

func (b *metricsBatchPrometheusV2) ToProtoBytes() ([]byte, error) {
	req := otlpcollectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: b.ToProto(),
	}
	return proto.Marshal(&req)
}

func (b *metricsBatchPrometheusV2) AddPoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	if measurement != common.MeasurementPrometheus {
		return fmt.Errorf("unexpected measurement name '%s'", measurement)
	}

	vType = b.inferMetricValueType(vType, tags, fields)

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

func (b *metricsBatchPrometheusV2) inferMetricValueType(vType common.InfluxMetricValueType, tags map[string]string, fields map[string]interface{}) common.InfluxMetricValueType {
	if vType == common.InfluxMetricValueTypeUntyped {
		for k := range tags {
			if k == common.MetricHistogramBoundKeyV2 || k == common.MetricSummaryQuantileKeyV2 {
				vType = common.InfluxMetricValueTypeHistogram
				break
			}
		}
	}
	if vType == common.InfluxMetricValueTypeUntyped {
		for k := range fields {
			if strings.HasSuffix(k, common.MetricHistogramCountSuffix) || strings.HasSuffix(k, common.MetricHistogramSumSuffix) {
				vType = common.InfluxMetricValueTypeHistogram
				break
			}
		}
	}
	if vType == common.InfluxMetricValueTypeUntyped {
		vType = common.InfluxMetricValueTypeGauge
	}
	return vType
}

func (b *metricsBatchPrometheusV2) getMetricName(vType common.InfluxMetricValueType, tags map[string]string, fields map[string]interface{}) (metricName string, err error) {
	switch vType {
	case common.InfluxMetricValueTypeGauge:
		if len(fields) != 1 {
			return "", fmt.Errorf("gauge metric should have 1 field, found %d", len(fields))
		}
		fallthrough

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

// unpackTags extracts resource attributes and instrumentation library name and version from tags.
// Return values are (metric name, resource attributes, IL name, IL version, labels).
func (b *metricsBatchPrometheusV2) unpackTags(tags map[string]string) (rAttributes []*otlpcommon.KeyValue, ilName string, ilVersion string, labels []*otlpcommon.StringKeyValue) {
	attributeKeys := make(map[string]struct{})
	for k, v := range tags {
		switch {
		case k == common.MetricHistogramBoundKeyV2 || k == common.MetricSummaryQuantileKeyV2:
			continue
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

func (b *metricsBatchPrometheusV2) lookupMetric(metricName string, rAttributes []*otlpcommon.KeyValue, ilName, ilVersion string, vType common.InfluxMetricValueType) (*otlpmetrics.Metric, error) {
	rKey := common.ResourceAttributesToKey(rAttributes)
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
		case common.InfluxMetricValueTypeGauge:
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
		b.histogramDataPointsByMDPK[metric] = make(map[dataPointKey]*otlpmetrics.DoubleHistogramDataPoint)
		b.summaryDataPointsByMDPK[metric] = make(map[dataPointKey]*otlpmetrics.DoubleSummaryDataPoint)
	}

	return metric, nil
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

func (b *metricsBatchPrometheusV2) convertGauge(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
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

func (b *metricsBatchPrometheusV2) convertSum(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, fields map[string]interface{}, ts time.Time) error {
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

func (b *metricsBatchPrometheusV2) convertHistogram(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
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

func (b *metricsBatchPrometheusV2) convertSummary(metric *otlpmetrics.Metric, labels []*otlpcommon.StringKeyValue, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
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
