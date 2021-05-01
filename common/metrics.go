package common

type InfluxMetricValueType uint8

const (
	InfluxMetricValueTypeUntyped InfluxMetricValueType = iota
	InfluxMetricValueTypeGauge
	InfluxMetricValueTypeSum
	InfluxMetricValueTypeHistogram
	InfluxMetricValueTypeSummary
)

func (vType InfluxMetricValueType) String() string {
	switch vType {
	case InfluxMetricValueTypeUntyped:
		return "untyped"
	case InfluxMetricValueTypeGauge:
		return "gauge"
	case InfluxMetricValueTypeSum:
		return "sum"
	case InfluxMetricValueTypeHistogram:
		return "histogram"
	case InfluxMetricValueTypeSummary:
		return "summary"
	default:
		panic("invalid InfluxMetricValueType")
	}
}

type MetricsSchema uint8

const (
	_ MetricsSchema = iota
	MetricsSchemaTelegrafPrometheusV1
	MetricsSchemaTelegrafPrometheusV2
)

func (ms MetricsSchema) String() string {
	switch ms {
	case MetricsSchemaTelegrafPrometheusV1:
		return "telegraf-prometheus-v1"
	case MetricsSchemaTelegrafPrometheusV2:
		return "telegraf-prometheus-v2"
	default:
		panic("invalid MetricsSchema")
	}
}
