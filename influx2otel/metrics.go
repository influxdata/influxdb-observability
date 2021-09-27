package influx2otel

import (
	"errors"
	"fmt"
	"strings"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"go.opentelemetry.io/collector/model/pdata"
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
		rmByAttributes:            make(map[string]pdata.ResourceMetrics),
		ilmByRMAttributesAndIL:    make(map[string]map[string]pdata.InstrumentationLibraryMetrics),
		metricByRMIL:              make(map[string]map[string]map[string]pdata.Metric),
		histogramDataPointsByMDPK: make(map[pdata.Metric]map[dataPointKey]pdata.HistogramDataPoint),
		summaryDataPointsByMDPK:   make(map[pdata.Metric]map[dataPointKey]pdata.SummaryDataPoint),

		logger: c.logger,
	}
}

type MetricsBatch struct {
	rmByAttributes            map[string]pdata.ResourceMetrics
	ilmByRMAttributesAndIL    map[string]map[string]pdata.InstrumentationLibraryMetrics
	metricByRMIL              map[string]map[string]map[string]pdata.Metric
	histogramDataPointsByMDPK map[pdata.Metric]map[dataPointKey]pdata.HistogramDataPoint
	summaryDataPointsByMDPK   map[pdata.Metric]map[dataPointKey]pdata.SummaryDataPoint

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

func resourceAttributesToKey(rAttributes pdata.AttributeMap) string {
	var key strings.Builder
	rAttributes.Range(func(k string, v pdata.AttributeValue) bool {
		key.WriteString(k)
		key.WriteByte(':')
		return true
	})
	return key.String()
}

var errValueTypeUnknown = errors.New("value type unknown")

func (b *MetricsBatch) lookupMetric(metricName string, tags map[string]string, vType common.InfluxMetricValueType) (pdata.Metric, pdata.AttributeMap, error) {
	var ilName, ilVersion string
	rAttributes := pdata.NewAttributeMap()
	mAttributes := pdata.NewAttributeMap()
	for k, v := range tags {
		switch {
		case k == common.MetricHistogramBoundKeyV2 || k == common.MetricSummaryQuantileKeyV2:
			continue
		case k == common.AttributeInstrumentationLibraryName:
			ilName = v
		case k == common.AttributeInstrumentationLibraryVersion:
			ilVersion = v
		case common.ResourceNamespace.MatchString(k):
			rAttributes.InsertString(k, v)
		default:
			mAttributes.InsertString(k, v)
		}
	}

	rAttributes.Sort()

	rKey := resourceAttributesToKey(rAttributes)
	var resourceMetrics pdata.ResourceMetrics
	if rm, found := b.rmByAttributes[rKey]; found {
		resourceMetrics = rm
	} else {
		resourceMetrics = pdata.NewResourceMetrics()
		rAttributes.CopyTo(resourceMetrics.Resource().Attributes())
		b.rmByAttributes[rKey] = resourceMetrics
		b.ilmByRMAttributesAndIL[rKey] = make(map[string]pdata.InstrumentationLibraryMetrics)
		b.metricByRMIL[rKey] = make(map[string]map[string]pdata.Metric)
	}

	ilmKey := ilName + ":" + ilVersion
	var ilMetrics pdata.InstrumentationLibraryMetrics
	if ilm, found := b.ilmByRMAttributesAndIL[rKey][ilmKey]; found {
		ilMetrics = ilm
	} else {
		ilMetrics = resourceMetrics.InstrumentationLibraryMetrics().AppendEmpty()
		ilMetrics.InstrumentationLibrary().SetName(ilName)
		ilMetrics.InstrumentationLibrary().SetVersion(ilVersion)
		b.ilmByRMAttributesAndIL[rKey][ilmKey] = ilMetrics
		b.metricByRMIL[rKey][ilmKey] = make(map[string]pdata.Metric)
	}

	var metric pdata.Metric
	if m, found := b.metricByRMIL[rKey][ilmKey][metricName]; found {
		switch m.DataType() {
		case pdata.MetricDataTypeGauge:
			if vType != common.InfluxMetricValueTypeGauge && vType != common.InfluxMetricValueTypeUntyped {
				return pdata.Metric{}, pdata.AttributeMap{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s' or '%s', got '%s'", metricName, common.InfluxMetricValueTypeGauge, common.InfluxMetricValueTypeUntyped, vType)
			}
		case pdata.MetricDataTypeSum:
			if vType != common.InfluxMetricValueTypeSum {
				return pdata.Metric{}, pdata.AttributeMap{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeSum, vType)
			}
		case pdata.MetricDataTypeHistogram:
			if vType != common.InfluxMetricValueTypeHistogram {
				return pdata.Metric{}, pdata.AttributeMap{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeHistogram, vType)
			}
		case pdata.MetricDataTypeSummary:
			if vType != common.InfluxMetricValueTypeSummary {
				return pdata.Metric{}, pdata.AttributeMap{}, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeSummary, vType)
			}
		default:
			return pdata.Metric{}, pdata.AttributeMap{}, fmt.Errorf("impossible InfluxMetricValueType %d", vType)
		}
		metric = m

	} else {
		metric = b.ilmByRMAttributesAndIL[rKey][ilmKey].Metrics().AppendEmpty()
		metric.SetName(metricName)
		switch vType {
		case common.InfluxMetricValueTypeGauge:
			metric.SetDataType(pdata.MetricDataTypeGauge)
		case common.InfluxMetricValueTypeSum:
			metric.SetDataType(pdata.MetricDataTypeSum)
			metric.Sum().SetIsMonotonic(true)
			metric.Sum().SetAggregationTemporality(pdata.MetricAggregationTemporalityCumulative)
		case common.InfluxMetricValueTypeHistogram:
			metric.SetDataType(pdata.MetricDataTypeHistogram)
			metric.Histogram().SetAggregationTemporality(pdata.MetricAggregationTemporalityCumulative)
		case common.InfluxMetricValueTypeSummary:
			metric.SetDataType(pdata.MetricDataTypeSummary)
		default:
			return pdata.Metric{}, pdata.AttributeMap{}, fmt.Errorf("unrecognized InfluxMetricValueType %d", vType)
		}
		b.metricByRMIL[rKey][ilmKey][metricName] = metric
		b.histogramDataPointsByMDPK[metric] = make(map[dataPointKey]pdata.HistogramDataPoint)
		b.summaryDataPointsByMDPK[metric] = make(map[dataPointKey]pdata.SummaryDataPoint)
	}

	return metric, mAttributes, nil
}

func (b *MetricsBatch) GetMetrics() pdata.Metrics {
	metrics := pdata.NewMetrics()
	// Ensure that the extra bucket counts have been added.
	for _, resourceMetrics := range b.rmByAttributes {
		for i := 0; i < resourceMetrics.InstrumentationLibraryMetrics().Len(); i++ {
			ilMetrics := resourceMetrics.InstrumentationLibraryMetrics().At(i)
			for j := 0; j < ilMetrics.Metrics().Len(); j++ {
				metric := ilMetrics.Metrics().At(j)
				if metric.DataType() == pdata.MetricDataTypeHistogram {
					for k := 0; k < metric.Histogram().DataPoints().Len(); k++ {
						dataPoint := metric.Histogram().DataPoints().At(k)
						if len(dataPoint.BucketCounts()) == len(dataPoint.ExplicitBounds()) {
							dataPoint.SetBucketCounts(append(dataPoint.BucketCounts(), dataPoint.Count()))
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
		dataPoint.SetTimestamp(pdata.NewTimestampFromTime(ts))
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
