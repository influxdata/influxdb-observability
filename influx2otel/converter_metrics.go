package influx2otel

import (
	"fmt"
	"sort"
	"strings"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
	otlpresource "github.com/influxdata/influxdb-observability/otlp/resource/v1"
)

type MetricsBatch struct {
	resourceMetricss       []*otlpmetrics.ResourceMetrics
	rmByAttributes         map[string]*otlpmetrics.ResourceMetrics
	ilmByRMAttributesAndIL map[string]map[string]*otlpmetrics.InstrumentationLibraryMetrics
	metricByRMIL           map[string]map[string]map[string]*otlpmetrics.Metric

	logger common.Logger
}

func NewMetricsBatch(logger common.Logger) *MetricsBatch {
	return &MetricsBatch{
		rmByAttributes:         make(map[string]*otlpmetrics.ResourceMetrics),
		ilmByRMAttributesAndIL: make(map[string]map[string]*otlpmetrics.InstrumentationLibraryMetrics),
		metricByRMIL:           make(map[string]map[string]map[string]*otlpmetrics.Metric),

		logger: logger,
	}
}

// tagsToResourceAndILAttributes extracts resource attributes and
// instrumentation library name and version from tags. Return values are
// (resource attributes, IL name, IL version, tags).
// Returned tags have any discovered resource attributes and instrumentation
// library fields removed.
func tagsToResourceAndILAttributes(tags map[string]string) ([]*otlpcommon.KeyValue, string, string, map[string]string) {
	attributeKeys := make(map[string]struct{})
	var rAttributes []*otlpcommon.KeyValue
	var ilName, ilVersion string

	for k, v := range tags {
		switch {
		case k == common.AttributeInstrumentationLibraryName:
			ilName = v
			delete(tags, k)
		case k == common.AttributeInstrumentationLibraryVersion:
			ilVersion = v
			delete(tags, k)
		case common.ResourceNamespace.MatchString(k):
			rAttributes = append(rAttributes, &otlpcommon.KeyValue{
				Key:   k,
				Value: &otlpcommon.AnyValue{Value: &otlpcommon.AnyValue_StringValue{StringValue: v}},
			})
			attributeKeys[k] = struct{}{}
			delete(tags, k)
		}
	}

	sort.Slice(rAttributes, func(i, j int) bool {
		return rAttributes[i].Key < rAttributes[j].Key
	})

	return rAttributes, ilName, ilVersion, tags
}

func tagsToLabels(tags map[string]string) []*otlpcommon.StringKeyValue {
	var labels []*otlpcommon.StringKeyValue
	for k, v := range tags {
		labels = append(labels, &otlpcommon.StringKeyValue{
			Key:   k,
			Value: v,
		})
	}

	sort.Slice(labels, func(i, j int) bool {
		return labels[i].Key < labels[j].Key
	})

	return labels
}

func resourceAttributesToMapKey(rAttributes []*otlpcommon.KeyValue) string {
	var key strings.Builder
	for _, kv := range rAttributes {
		key.WriteString(kv.Key)
		key.WriteByte(':')
	}
	return key.String()
}

func (b *MetricsBatch) AddPoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time) {
	rAttributes, ilName, ilVersion, tags := tagsToResourceAndILAttributes(tags)

	rKey := resourceAttributesToMapKey(rAttributes)
	var resourceMetrics *otlpmetrics.ResourceMetrics
	if rm, found := b.rmByAttributes[rKey]; found {
		resourceMetrics = rm
	} else {
		resourceMetrics = &otlpmetrics.ResourceMetrics{
			Resource: &otlpresource.Resource{
				Attributes: rAttributes,
			},
		}
		b.resourceMetricss = append(b.resourceMetricss, resourceMetrics)
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
	if m, found := b.metricByRMIL[rKey][ilmKey][measurement]; found {
		metric = m
	} else {
		metric = &otlpmetrics.Metric{
			Name: measurement,
			Data: &otlpmetrics.Metric_Gauge{
				Gauge: &otlpmetrics.Gauge{},
			},
		}
		ilMetrics.Metrics = append(ilMetrics.Metrics, metric)
		b.metricByRMIL[rKey][ilmKey][measurement] = metric
	}

	labels := tagsToLabels(tags)
	timeUnixNano := uint64(ts.UnixNano())
	for k, v := range fields {
		dataPoint := &otlpmetrics.NumberDataPoint{
			Labels:       labels,
			TimeUnixNano: timeUnixNano,
		}

		switch vv := v.(type) {
		case int64:
			dataPoint.Value = &otlpmetrics.NumberDataPoint_AsInt{AsInt: vv}
		case uint64:
			dataPoint.Value = &otlpmetrics.NumberDataPoint_AsInt{AsInt: int64(vv)}
		case float64:
			dataPoint.Value = &otlpmetrics.NumberDataPoint_AsDouble{AsDouble: vv}
		default:
			b.logger.Debug("unsupported field value type", "field-key", k, "field-value-type", fmt.Sprintf("%T", v))
			continue
		}
		metric.Data.(*otlpmetrics.Metric_Gauge).Gauge.DataPoints = append(metric.Data.(*otlpmetrics.Metric_Gauge).Gauge.DataPoints, dataPoint)
	}
}

func (b *MetricsBatch) Finish() []*otlpmetrics.ResourceMetrics {
	return b.resourceMetricss
}
