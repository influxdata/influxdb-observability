package influx2otel

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatautil"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"

	"github.com/influxdata/influxdb-observability/common"
)

type LineProtocolToOtelMetrics struct {
	logger    common.Logger
	startTime time.Time
}

func NewLineProtocolToOtelMetrics(logger common.Logger, startTime time.Time) (*LineProtocolToOtelMetrics, error) {
	return &LineProtocolToOtelMetrics{
		logger:    logger,
		startTime: startTime,
	}, nil
}

func (c *LineProtocolToOtelMetrics) NewBatch() *MetricsBatch {
	return &MetricsBatch{
		rmByAttributes:            make(map[[16]byte]pmetric.ResourceMetrics),
		ilmByRMAttributesAndIL:    make(map[[16]byte]map[string]pmetric.ScopeMetrics),
		metricByRMIL:              make(map[[16]byte]map[string]map[string]pmetric.Metric),
		histogramDataPointsByMDPK: make(map[pmetric.Metric]map[dataPointKey]pmetric.HistogramDataPoint),
		summaryDataPointsByMDPK:   make(map[pmetric.Metric]map[dataPointKey]pmetric.SummaryDataPoint),

		logger:    c.logger,
		startTime: c.startTime,
	}
}

type MetricsBatch struct {
	rmByAttributes            map[[16]byte]pmetric.ResourceMetrics
	ilmByRMAttributesAndIL    map[[16]byte]map[string]pmetric.ScopeMetrics
	metricByRMIL              map[[16]byte]map[string]map[string]pmetric.Metric
	histogramDataPointsByMDPK map[pmetric.Metric]map[dataPointKey]pmetric.HistogramDataPoint
	summaryDataPointsByMDPK   map[pmetric.Metric]map[dataPointKey]pmetric.SummaryDataPoint

	logger    common.Logger
	startTime time.Time
}

func (b *MetricsBatch) AddPoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error {
	if measurement == common.MeasurementPrometheus {
		err := b.addPointTelegrafPrometheusV2(measurement, tags, fields, ts, vType)
		if err == errValueTypeUnknown {
			return b.addPointWithUnknownSchema(measurement, tags, fields, ts)
		} else {
			return err
		}
	}

	if mt, ok := tags["metric_type"]; ok {
		if mt == "timing" {
			return b.addPointWithUnknownSchema(measurement, tags, fields, ts)
		}
	}

	err := b.addPointTelegrafPrometheusV1(measurement, tags, fields, ts, vType)
	if err == errValueTypeUnknown {
		return b.addPointWithUnknownSchema(measurement, tags, fields, ts)
	} else {
		return err
	}
}

var errValueTypeUnknown = errors.New("value type unknown")

func (b *MetricsBatch) lookupMetric(metricName string, tags map[string]string, vType common.InfluxMetricValueType) (pmetric.Metric, pcommon.Map, error) {
	var ilName, ilVersion string
	rAttributes := pcommon.NewMap()
	mAttributes := pcommon.NewMap()
	for k, v := range tags {
		switch {
		case k == common.MetricHistogramBoundKeyV2 || k == common.MetricSummaryQuantileKeyV2:
			continue
		case k == semconv.OtelLibraryName:
			ilName = v
		case k == semconv.OtelLibraryVersion:
			ilVersion = v
		case common.ResourceNamespace.MatchString(k):
			rAttributes.PutStr(k, v)
		default:
			mAttributes.PutStr(k, v)
		}
	}

	rKey := pdatautil.MapHash(rAttributes)
	var resourceMetrics pmetric.ResourceMetrics
	if rm, found := b.rmByAttributes[rKey]; found {
		resourceMetrics = rm
	} else {
		resourceMetrics = pmetric.NewResourceMetrics()
		rAttributes.CopyTo(resourceMetrics.Resource().Attributes())
		b.rmByAttributes[rKey] = resourceMetrics
		b.ilmByRMAttributesAndIL[rKey] = make(map[string]pmetric.ScopeMetrics)
		b.metricByRMIL[rKey] = make(map[string]map[string]pmetric.Metric)
	}

	ilmKey := ilName + ":" + ilVersion
	if _, found := b.ilmByRMAttributesAndIL[rKey][ilmKey]; !found {
		ilMetrics := resourceMetrics.ScopeMetrics().AppendEmpty()
		ilMetrics.Scope().SetName(ilName)
		ilMetrics.Scope().SetVersion(ilVersion)
		b.ilmByRMAttributesAndIL[rKey][ilmKey] = ilMetrics
		b.metricByRMIL[rKey][ilmKey] = make(map[string]pmetric.Metric)
	}

	var metric pmetric.Metric
	if m, found := b.metricByRMIL[rKey][ilmKey][metricName]; found {
		switch m.Type() {
		case pmetric.MetricTypeGauge:
			if vType != common.InfluxMetricValueTypeGauge && vType != common.InfluxMetricValueTypeUntyped {
				return pmetric.Metric{}, pcommon.Map{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s' or '%s', got '%s'", metricName, common.InfluxMetricValueTypeGauge, common.InfluxMetricValueTypeUntyped, vType)
			}
		case pmetric.MetricTypeSum:
			if vType != common.InfluxMetricValueTypeSum {
				return pmetric.Metric{}, pcommon.Map{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeSum, vType)
			}
		case pmetric.MetricTypeHistogram:
			if vType != common.InfluxMetricValueTypeHistogram {
				return pmetric.Metric{}, pcommon.Map{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeHistogram, vType)
			}
		case pmetric.MetricTypeSummary:
			if vType != common.InfluxMetricValueTypeSummary {
				return pmetric.Metric{}, pcommon.Map{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeSummary, vType)
			}
		default:
			return pmetric.Metric{}, pcommon.Map{}, fmt.Errorf("impossible InfluxMetricValueType %d", vType)
		}
		metric = m

	} else {
		metric = b.ilmByRMAttributesAndIL[rKey][ilmKey].Metrics().AppendEmpty()
		metric.SetName(metricName)
		switch vType {
		case common.InfluxMetricValueTypeGauge:
			metric.SetEmptyGauge()
		case common.InfluxMetricValueTypeSum:
			metric.SetEmptySum()
			metric.Sum().SetIsMonotonic(true)
			metric.Sum().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		case common.InfluxMetricValueTypeHistogram:
			metric.SetEmptyHistogram()
			metric.Histogram().SetAggregationTemporality(pmetric.AggregationTemporalityCumulative)
		case common.InfluxMetricValueTypeSummary:
			metric.SetEmptySummary()
		default:
			return pmetric.Metric{}, pcommon.Map{}, fmt.Errorf("unrecognized InfluxMetricValueType %d", vType)
		}
		b.metricByRMIL[rKey][ilmKey][metricName] = metric
		b.histogramDataPointsByMDPK[metric] = make(map[dataPointKey]pmetric.HistogramDataPoint)
		b.summaryDataPointsByMDPK[metric] = make(map[dataPointKey]pmetric.SummaryDataPoint)
	}

	return metric, mAttributes, nil
}

func (b *MetricsBatch) GetMetrics() pmetric.Metrics {
	metrics := pmetric.NewMetrics()
	// Ensure that the extra bucket counts have been added.
	for _, resourceMetrics := range b.rmByAttributes {
		for i := 0; i < resourceMetrics.ScopeMetrics().Len(); i++ {
			ilMetrics := resourceMetrics.ScopeMetrics().At(i)
			for j := 0; j < ilMetrics.Metrics().Len(); j++ {
				metric := ilMetrics.Metrics().At(j)
				if metric.Type() == pmetric.MetricTypeHistogram {
					for k := 0; k < metric.Histogram().DataPoints().Len(); k++ {
						dataPoint := metric.Histogram().DataPoints().At(k)
						if dataPoint.BucketCounts().Len() == dataPoint.ExplicitBounds().Len() {
							dataPoint.BucketCounts().Append(dataPoint.Count())
						}
					}
				}
			}
		}
		resourceMetrics.CopyTo(metrics.ResourceMetrics().AppendEmpty())
	}
	return metrics
}

func (b *MetricsBatch) addPointWithUnknownSchema(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	if ts.IsZero() {
		ts = time.Now()
	}

	for k, v := range fields {
		var floatValue *float64
		var intValue *int64
		switch vv := v.(type) {
		case float64:
			floatValue = &vv
		case int64:
			intValue = &vv
		case uint64:
			convertedTypedValue := int64(vv)
			intValue = &convertedTypedValue
		default:
			b.logger.Debug("field has unsupported type", "measurement", measurement, "field", k, "type", fmt.Sprintf("%T", v))
			continue
		}

		metricName := fmt.Sprintf("%s_%s", measurement, k)
		metric, attributes, err := b.lookupMetric(metricName, tags, common.InfluxMetricValueTypeGauge)
		if err != nil {
			return err
		}
		dataPoint := metric.Gauge().DataPoints().AppendEmpty()
		attributes.CopyTo(dataPoint.Attributes())
		dataPoint.SetStartTimestamp(pcommon.NewTimestampFromTime(b.startTime))
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(ts))
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

func sortHistogramBuckets(hdp pmetric.HistogramDataPoint) {
	sBuckets := make(sortableBuckets, hdp.ExplicitBounds().Len())
	for i := 0; i < hdp.ExplicitBounds().Len(); i++ {
		sBuckets[i] = sortableBucket{hdp.BucketCounts().At(i), hdp.ExplicitBounds().At(i)}
	}
	sort.Sort(sBuckets)
	counts := make([]uint64, hdp.ExplicitBounds().Len())
	buckets := make([]float64, hdp.ExplicitBounds().Len())
	for i, bucket := range sBuckets {
		counts[i], buckets[i] = bucket.count, bucket.bound
	}
	hdp.BucketCounts().FromRaw(counts)
	hdp.ExplicitBounds().FromRaw(buckets)
}

type sortableBucket struct {
	count uint64
	bound float64
}

type sortableBuckets []sortableBucket

func (s sortableBuckets) Len() int {
	return len(s)
}

func (s sortableBuckets) Less(i, j int) bool {
	return s[i].bound < s[j].bound
}

func (s sortableBuckets) Swap(i, j int) {
	s[i].count, s[j].count = s[j].count, s[i].count
	s[i].bound, s[j].bound = s[j].bound, s[i].bound
}
