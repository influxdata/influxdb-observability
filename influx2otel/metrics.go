package influx2otel

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/influxdata/influxdb-observability/common"
)

type LineProtocolToOtelMetrics struct {
	logger common.Logger
}

func NewLineProtocolToOtelMetrics(logger common.Logger) (*LineProtocolToOtelMetrics, error) {
	return &LineProtocolToOtelMetrics{
		logger: logger,
	}, nil
}

func (c *LineProtocolToOtelMetrics) NewBatch() *MetricsBatch {
	return &MetricsBatch{
		rmByAttributes:            make(map[string]pmetric.ResourceMetrics),
		ilmByRMAttributesAndIL:    make(map[string]map[string]pmetric.ScopeMetrics),
		metricByRMIL:              make(map[string]map[string]map[string]pmetric.Metric),
		histogramDataPointsByMDPK: make(map[pmetric.Metric]map[dataPointKey]pmetric.HistogramDataPoint),
		summaryDataPointsByMDPK:   make(map[pmetric.Metric]map[dataPointKey]pmetric.SummaryDataPoint),

		logger: c.logger,
	}
}

type MetricsBatch struct {
	rmByAttributes            map[string]pmetric.ResourceMetrics
	ilmByRMAttributesAndIL    map[string]map[string]pmetric.ScopeMetrics
	metricByRMIL              map[string]map[string]map[string]pmetric.Metric
	histogramDataPointsByMDPK map[pmetric.Metric]map[dataPointKey]pmetric.HistogramDataPoint
	summaryDataPointsByMDPK   map[pmetric.Metric]map[dataPointKey]pmetric.SummaryDataPoint

	logger common.Logger
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

	err := b.addPointTelegrafPrometheusV1(measurement, tags, fields, ts, vType)
	if err == errValueTypeUnknown {
		return b.addPointWithUnknownSchema(measurement, tags, fields, ts)
	} else {
		return err
	}
}

func resourceAttributesToKey(rAttributes pcommon.Map) string {
	var key strings.Builder
	rAttributes.Range(func(k string, v pcommon.Value) bool {
		key.WriteString(k)
		key.WriteByte(':')
		return true
	})
	return key.String()
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
		case k == common.AttributeInstrumentationLibraryName:
			ilName = v
		case k == common.AttributeInstrumentationLibraryVersion:
			ilVersion = v
		case common.ResourceNamespace.MatchString(k):
			rAttributes.PutString(k, v)
		default:
			mAttributes.PutString(k, v)
		}
	}

	rAttributes.Sort()

	rKey := resourceAttributesToKey(rAttributes)
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
		switch m.DataType() {
		case pmetric.MetricDataTypeGauge:
			if vType != common.InfluxMetricValueTypeGauge && vType != common.InfluxMetricValueTypeUntyped {
				return pmetric.Metric{}, pcommon.Map{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s' or '%s', got '%s'", metricName, common.InfluxMetricValueTypeGauge, common.InfluxMetricValueTypeUntyped, vType)
			}
		case pmetric.MetricDataTypeSum:
			if vType != common.InfluxMetricValueTypeSum {
				return pmetric.Metric{}, pcommon.Map{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeSum, vType)
			}
		case pmetric.MetricDataTypeHistogram:
			if vType != common.InfluxMetricValueTypeHistogram {
				return pmetric.Metric{}, pcommon.Map{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeHistogram, vType)
			}
		case pmetric.MetricDataTypeSummary:
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
			metric.Sum().SetAggregationTemporality(pmetric.MetricAggregationTemporalityCumulative)
		case common.InfluxMetricValueTypeHistogram:
			metric.SetEmptyHistogram()
			metric.Histogram().SetAggregationTemporality(pmetric.MetricAggregationTemporalityCumulative)
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
				if metric.DataType() == pmetric.MetricDataTypeHistogram {
					for k := 0; k < metric.Histogram().DataPoints().Len(); k++ {
						dataPoint := metric.Histogram().DataPoints().At(k)
						if dataPoint.BucketCounts().Len() == dataPoint.ExplicitBounds().Len() {
							dataPoint.BucketCounts().FromRaw(append(dataPoint.BucketCounts().AsRaw(), dataPoint.Count()))
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
		dataPoint.SetTimestamp(pcommon.NewTimestampFromTime(ts))
		if floatValue != nil {
			dataPoint.SetDoubleVal(*floatValue)
		} else if intValue != nil {
			dataPoint.SetIntVal(*intValue)
		} else {
			panic("unreachable")
		}
	}

	return nil
}
