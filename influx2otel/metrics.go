package influx2otel

import (
	"fmt"
	"strconv"
	"strings"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
)

type LineProtocolToOtelMetrics struct {
	newBatch func(common.Logger) MetricsBatch

	logger common.Logger
}

func NewLineProtocolToOtelMetrics(logger common.Logger, schema common.MetricsSchema) (*LineProtocolToOtelMetrics, error) {
	var newBatch func(common.Logger) MetricsBatch
	switch schema {
	case common.MetricsSchemaTelegrafPrometheusV1:
		newBatch = newMetricsBatchPrometheusV1
	case common.MetricsSchemaTelegrafPrometheusV2:
		newBatch = newmetricsBatchPrometheusV2
	default:
		return nil, fmt.Errorf("unrecognized metrics schema %d", schema)
	}
	return &LineProtocolToOtelMetrics{
		newBatch: newBatch,
		logger:   logger,
	}, nil
}

func (c *LineProtocolToOtelMetrics) NewBatch() MetricsBatch {
	return c.newBatch(c.logger)
}

type MetricsBatch interface {
	AddPoint(measurement string, tags map[string]string, fields map[string]interface{}, ts time.Time, vType common.InfluxMetricValueType) error
	ToProto() []*otlpmetrics.ResourceMetrics
}

func resourceAttributesToKey(rAttributes []*otlpcommon.KeyValue) string {
	var key strings.Builder
	for _, kv := range rAttributes {
		key.WriteString(kv.Key)
		key.WriteByte(':')
	}
	return key.String()
}

func isStringNumeric(s string) bool {
	_, err := strconv.ParseFloat(s, 64)
	return err != nil
}
