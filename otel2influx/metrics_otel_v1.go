package otel2influx

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"go.opentelemetry.io/collector/consumer/consumererror"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"go.uber.org/multierr"

	"github.com/influxdata/influxdb-observability/common"
)

var _ metricWriter = (*metricWriterOtelV1)(nil)

type metricWriterOtelV1 struct {
	logger common.Logger
}

func (m *metricWriterOtelV1) writeMetric(ctx context.Context, res pcommon.Resource, il pcommon.InstrumentationScope, pm pmetric.Metric, batch InfluxWriterBatch) (err error) {
	defer func() {
		if r := recover(); r != nil {
			var rerr error
			switch v := r.(type) {
			case error:
				rerr = v
			case string:
				rerr = errors.New(v)
			default:
				rerr = fmt.Errorf("%+v", r)
			}
			err = multierr.Combine(err, rerr)
		}

		if err != nil && !consumererror.IsPermanent(err) {
			m.logger.Debug(err.Error())
			err = nil
		}
	}()

	switch pm.Type() {
	//case pmetric.MetricTypeGauge:
	//	return m.writeGauge(ctx, res, il, pm.Name(), pm.Gauge(), batch)
	case pmetric.MetricTypeSum:
		m.writeSum(ctx, res, il, pm, batch)
	//case pmetric.MetricTypeHistogram:
	//	return m.writeHistogram(ctx, res, il, pm.Name(), pm.Histogram(), batch)
	default:
		err = fmt.Errorf("unrecognized metric type %q", pm.Type())
	}
	return
}

// formatMeasurementNameMetricOtelV1 composes a measurement name from metric (name, unit, type)
func formatMeasurementNameMetricOtelV1(metricName, metricUnit, metricType string) string {
	mt := strings.ToLower(metricType)
	if mt != "sum" {
		panic(fmt.Sprintf("unsupported metric type '%s'", metricType))
	}
	return fmt.Sprintf("%s_%s_%s", metricName, metricUnit, mt)
}

// formatFieldKeyMetricOtelV1 composes a value field key from (sum temporality, sum monotonicity, and datapoint value type)
func formatFieldKeyMetricOtelV1(temporality string, monotonic bool, dataPointValueType string) string {
	at := strings.ToLower(temporality)
	if at != "delta" && at != "cumulative" {
		panic(fmt.Sprintf("unsupported aggregation temporality '%s'", temporality))
	}

	var monotonicity string
	if monotonic {
		monotonicity = "monotonic"
	} else {
		monotonicity = "nonmonotonic"
	}

	vt := strings.ToLower(dataPointValueType)
	if vt != "int" && vt != "double" {
		panic(fmt.Sprintf("unsupported data point value type '%s'", dataPointValueType))
	}
	return fmt.Sprintf("value_%s_%s_%s", temporality, monotonicity, vt)
}

func (m *metricWriterOtelV1) writeSum(ctx context.Context, resource pcommon.Resource, instrumentationLibrary pcommon.InstrumentationScope, pm pmetric.Metric, batch InfluxWriterBatch) {
	// TODO metric description
	measurementName := formatMeasurementNameMetricOtelV1(pm.Name(), pm.Unit(), pm.Type().String())
	temporality := pm.Sum().AggregationTemporality()
	monotonic := pm.Sum().IsMonotonic()

	buildValue := func(dataPoint pmetric.NumberDataPoint) (string, interface{}) {
		fieldKey := formatFieldKeyMetricOtelV1(temporality.String(), monotonic, dataPoint.ValueType().String())
		switch dataPoint.ValueType() {
		case pmetric.NumberDataPointValueTypeInt:
			return fieldKey, dataPoint.IntValue()
		case pmetric.NumberDataPointValueTypeDouble:
			return fieldKey, dataPoint.DoubleValue()
		default:
			panic(fmt.Sprintf("unsupported data point value type '%s'", dataPoint.ValueType().String()))
		}
	}
	resourceTags := make(map[string]string)
	attributesToInfluxTags(resource.Attributes(), resourceTags)
	InstrumentationLibraryToTags(instrumentationLibrary, resourceTags)

	for i := 0; i < pm.Sum().DataPoints().Len(); i++ {
		// TODO datapoint exemplars
		// TODO datapoint flags
		dataPoint := pm.Sum().DataPoints().At(i)

		valueFieldKey, value := buildValue(dataPoint)
		fields := map[string]interface{}{
			// TODO move constant string to common.go
			"start_time_unix_nano": dataPoint.StartTimestamp().AsTime().UnixNano(),
			valueFieldKey:          value,
		}
		attributeTags := make(map[string]string, dataPoint.Attributes().Len()+len(resourceTags))
		for k, v := range resourceTags {
			attributeTags[k] = v
		}
		attributesToInfluxTags(dataPoint.Attributes(), attributeTags)

		err := batch.WritePoint(ctx, measurementName, resourceTags, fields, dataPoint.Timestamp().AsTime(), common.InfluxMetricValueTypeUntyped)
		if err != nil {
			panic(err)
		}
	}
}
