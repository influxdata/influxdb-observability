package influx2otel_test

import (
	"testing"
	"time"

	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/influxdb-observability/influx2otel"
)

func TestUnknownSchema(t *testing.T) {
	c, err := influx2otel.NewLineProtocolToOtelMetrics(new(common.NoopLogger))
	require.NoError(t, err)

	b := c.NewBatch()
	err = b.AddPoint("cpu",
		map[string]string{
			"container.name":       "42",
			"otel.library.name":    "My Library",
			"otel.library.version": "latest",
			"cpu":                  "cpu4",
			"host":                 "777348dc6343",
		},
		map[string]interface{}{
			"usage_user":   0.10090817356207936,
			"usage_system": 0.3027245206862381,
			"some_int_key": int64(7),
		},
		time.Unix(0, 1395066363000000123),
		common.InfluxMetricValueTypeUntyped)
	require.NoError(t, err)

	expect := pmetric.NewMetrics()
	rm := expect.ResourceMetrics().AppendEmpty()
	rm.Resource().Attributes().PutStr("container.name", "42")
	ilMetrics := rm.ScopeMetrics().AppendEmpty()
	ilMetrics.Scope().SetName("My Library")
	ilMetrics.Scope().SetVersion("latest")
	m := ilMetrics.Metrics().AppendEmpty()
	m.SetName("cpu_usage_user")
	m.SetEmptyGauge()
	dp := m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("cpu", "cpu4")
	dp.Attributes().PutStr("host", "777348dc6343")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(0.10090817356207936)
	m = ilMetrics.Metrics().AppendEmpty()
	m.SetName("cpu_usage_system")
	m.SetEmptyGauge()
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("cpu", "cpu4")
	dp.Attributes().PutStr("host", "777348dc6343")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetDoubleValue(0.3027245206862381)
	m = ilMetrics.Metrics().AppendEmpty()
	m.SetName("cpu_some_int_key")
	m.SetEmptyGauge()
	dp = m.Gauge().DataPoints().AppendEmpty()
	dp.Attributes().PutStr("cpu", "cpu4")
	dp.Attributes().PutStr("host", "777348dc6343")
	dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
	dp.SetIntValue(7)

	assertMetricsEqual(t, expect, b.GetMetrics())
}
