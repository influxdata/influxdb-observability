package influx2otel

import (
	"errors"
	"fmt"
	"sort"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcollectormetrics "github.com/influxdata/influxdb-observability/otlp/collector/metrics/v1"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
	"google.golang.org/protobuf/proto"
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
		rmByAttributes:            make(map[string]*otlpmetrics.ResourceMetrics),
		ilmByRMAttributesAndIL:    make(map[string]map[string]*otlpmetrics.InstrumentationLibraryMetrics),
		metricByRMIL:              make(map[string]map[string]map[string]*otlpmetrics.Metric),
		histogramDataPointsByMDPK: make(map[*otlpmetrics.Metric]map[dataPointKey]*otlpmetrics.DoubleHistogramDataPoint),
		summaryDataPointsByMDPK:   make(map[*otlpmetrics.Metric]map[dataPointKey]*otlpmetrics.DoubleSummaryDataPoint),

		logger: c.logger,
	}
}

type MetricsBatch struct {
	rmByAttributes            map[string]*otlpmetrics.ResourceMetrics
	ilmByRMAttributesAndIL    map[string]map[string]*otlpmetrics.InstrumentationLibraryMetrics
	metricByRMIL              map[string]map[string]map[string]*otlpmetrics.Metric
	histogramDataPointsByMDPK map[*otlpmetrics.Metric]map[dataPointKey]*otlpmetrics.DoubleHistogramDataPoint
	summaryDataPointsByMDPK   map[*otlpmetrics.Metric]map[dataPointKey]*otlpmetrics.DoubleSummaryDataPoint

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

var errValueTypeUnknown = errors.New("value type unknown")

// unpackTags extracts resource attributes and instrumentation library name and version from tags.
// Return values are (metric name, resource attributes, IL name, IL version, labels).
func (b *MetricsBatch) unpackTags(tags map[string]string) (rAttributes []*otlpcommon.KeyValue, ilName string, ilVersion string, labels []*otlpcommon.StringKeyValue) {
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

func (b *MetricsBatch) lookupMetric(metricName string, rAttributes []*otlpcommon.KeyValue, ilName, ilVersion string, vType common.InfluxMetricValueType) (*otlpmetrics.Metric, error) {
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
				return nil, fmt.Errorf("value type conflict for metric '%s'; expected '%s' or '%s', got '%s'", metricName, common.InfluxMetricValueTypeGauge, common.InfluxMetricValueTypeUntyped, vType)
			}
		case *otlpmetrics.Metric_DoubleSum:
			if vType != common.InfluxMetricValueTypeSum {
				return nil, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeSum, vType)
			}
		case *otlpmetrics.Metric_DoubleHistogram:
			if vType != common.InfluxMetricValueTypeHistogram {
				return nil, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeHistogram, vType)
			}
		case *otlpmetrics.Metric_DoubleSummary:
			if vType != common.InfluxMetricValueTypeSummary {
				return nil, fmt.Errorf("value type conflict for metric '%s'; expected '%s', got '%s'", metricName, common.InfluxMetricValueTypeSummary, vType)
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

func (b *MetricsBatch) ToProto() []*otlpmetrics.ResourceMetrics {
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

func (b *MetricsBatch) ToProtoBytes() ([]byte, error) {
	req := otlpcollectormetrics.ExportMetricsServiceRequest{
		ResourceMetrics: b.ToProto(),
	}
	return proto.Marshal(&req)
}

func (b *MetricsBatch) addPointWithUnknownSchema(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) error {
	if ts.IsZero() {
		ts = time.Now()
	}

	rAttributes, ilName, ilVersion, labels := b.unpackTags(tags)

	for k, v := range fields {
		measurement := fmt.Sprintf("%s:%s", measurement, k)
		metric, err := b.lookupMetric(measurement, rAttributes, ilName, ilVersion, common.InfluxMetricValueTypeGauge)
		if err != nil {
			return err
		}
		dataPoint := &otlpmetrics.DoubleDataPoint{
			Labels:       labels,
			TimeUnixNano: uint64(ts.UnixNano()),
		}
		switch vv := v.(type) {
		case int64:
			dataPoint.Value = float64(vv)
		case float64:
			dataPoint.Value = vv
		case uint64:
			dataPoint.Value = float64(vv)
		default:
			return fmt.Errorf("unsupported field value type %T", v)
		}
		metric.Data.(*otlpmetrics.Metric_DoubleGauge).DoubleGauge.DataPoints =
			append(metric.Data.(*otlpmetrics.Metric_DoubleGauge).DoubleGauge.DataPoints,
				dataPoint)
	}

	return nil
}
