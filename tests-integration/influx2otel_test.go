package tests

import (
	"fmt"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/telegraf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func TestInflux2Otel(t *testing.T) {
	for i, mt := range metricTests {
		t.Run(fmt.Sprint(i), func(t *testing.T) {
			t.Run("otelcol", func(t *testing.T) {
				otelcolReceiverAddress, mockExporterFactory := setupOtelcolInfluxDBReceiver(t)

				response, err := http.Post(fmt.Sprintf("http://%s/write", otelcolReceiverAddress), "", strings.NewReader(mt.lp))
				require.NoError(t, err)
				require.Equal(t, 2, response.StatusCode/100)

				got := mockExporterFactory.consumedMetrics
				common.SortResourceMetrics(got.ResourceMetrics())

				expect := mt.otel
				common.SortResourceMetrics(expect.ResourceMetrics())

				assert.Equal(t, expect, got)
			})

			t.Run("telegraf", func(t *testing.T) {
				assertOtel2InfluxTelegraf(t, mt.lp, telegraf.Untyped, mt.otel)
			})
		})
	}
}

func TestInflux2Otel_nowtime(t *testing.T) {
	t.Run("otelcol", func(t *testing.T) {
		otelcolReceiverAddress, mockExporterFactory := setupOtelcolInfluxDBReceiver(t)

		lp := `
cpu_temp,foo=bar gauge=87.332
`

		response, err := http.Post(fmt.Sprintf("http://%s/write", otelcolReceiverAddress), "", strings.NewReader(lp))
		require.NoError(t, err)
		assert.Equal(t, 2, response.StatusCode/100)

		gotTime := mockExporterFactory.consumedMetrics.ResourceMetrics().At(0).ScopeMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Timestamp().AsTime()
		assert.WithinDuration(t, time.Now(), gotTime, time.Second)
	})
}

func TestInflux2Otel_unknownSchema(t *testing.T) {
	t.Run("telegraf", func(t *testing.T) {
		lp := `
cpu,cpu=cpu4,host=777348dc6343 usage_user=0.10090817356207936,usage_system=0.3027245206862381,usage_iowait=0,invalid="ignored" 1395066363000000123
`

		expect := pmetric.NewMetrics()
		metrics := expect.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
		metric := metrics.AppendEmpty()
		metric.SetName("cpu_usage_iowait")
		metric.SetEmptyGauge()
		dp := metric.Gauge().DataPoints().AppendEmpty()
		dp.Attributes().PutString("cpu", "cpu4")
		dp.Attributes().PutString("host", "777348dc6343")
		dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
		dp.SetDoubleValue(0.0)
		metric = metrics.AppendEmpty()
		metric.SetName("cpu_usage_system")
		metric.SetEmptyGauge()
		dp = metric.Gauge().DataPoints().AppendEmpty()
		dp.Attributes().PutString("cpu", "cpu4")
		dp.Attributes().PutString("host", "777348dc6343")
		dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
		dp.SetDoubleValue(0.3027245206862381)
		metric = metrics.AppendEmpty()
		metric.SetName("cpu_usage_user")
		metric.SetEmptyGauge()
		dp = metric.Gauge().DataPoints().AppendEmpty()
		dp.Attributes().PutString("cpu", "cpu4")
		dp.Attributes().PutString("host", "777348dc6343")
		dp.SetTimestamp(pcommon.Timestamp(1395066363000000123))
		dp.SetDoubleValue(0.10090817356207936)

		assertOtel2InfluxTelegraf(t, lp, telegraf.Untyped, expect)
	})
}

func TestInflux2Otel_gaugeNonPrometheus(t *testing.T) {
	t.Run("telegraf", func(t *testing.T) {
		lp := `
swap,host=8eaaf6b73054 used_percent=1.5,total=1073737728i 1626302080000000000
`
		expect := pmetric.NewMetrics()
		metrics := expect.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
		metric := metrics.AppendEmpty()
		metric.SetName("swap_used_percent")
		metric.SetEmptyGauge()
		dp := metric.Gauge().DataPoints().AppendEmpty()
		dp.Attributes().PutString("host", "8eaaf6b73054")
		dp.SetTimestamp(pcommon.Timestamp(1626302080000000000))
		dp.SetDoubleValue(1.5)
		metric = metrics.AppendEmpty()
		metric.SetName("swap_total")
		metric.SetEmptyGauge()
		dp = metric.Gauge().DataPoints().AppendEmpty()
		dp.Attributes().PutString("host", "8eaaf6b73054")
		dp.SetTimestamp(pcommon.Timestamp(1626302080000000000))
		dp.SetIntValue(1073737728)

		assertOtel2InfluxTelegraf(t, lp, telegraf.Gauge, expect)
	})
}

func TestInflux2Otel_counterNonPrometheus(t *testing.T) {
	t.Run("telegraf", func(t *testing.T) {
		lp := `
swap,host=8eaaf6b73054 in=32768i,out=12021760i 1626302080000000000
`
		expect := pmetric.NewMetrics()
		metrics := expect.ResourceMetrics().AppendEmpty().ScopeMetrics().AppendEmpty().Metrics()
		metric := metrics.AppendEmpty()
		metric.SetName("swap_in")
		metric.SetEmptySum()
		metric.Sum().SetIsMonotonic(true)
		metric.Sum().SetAggregationTemporality(pmetric.MetricAggregationTemporalityCumulative)
		dp := metric.Sum().DataPoints().AppendEmpty()
		dp.Attributes().PutString("host", "8eaaf6b73054")
		dp.SetTimestamp(pcommon.Timestamp(1626302080000000000))
		dp.SetIntValue(32768)
		metric = metrics.AppendEmpty()
		metric.SetName("swap_out")
		metric.SetEmptySum()
		metric.Sum().SetIsMonotonic(true)
		metric.Sum().SetAggregationTemporality(pmetric.MetricAggregationTemporalityCumulative)
		dp = metric.Sum().DataPoints().AppendEmpty()
		dp.Attributes().PutString("host", "8eaaf6b73054")
		dp.SetTimestamp(pcommon.Timestamp(1626302080000000000))
		dp.SetIntValue(12021760)

		assertOtel2InfluxTelegraf(t, lp, telegraf.Counter, expect)
	})
}
