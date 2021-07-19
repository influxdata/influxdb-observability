package tests

import (
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/influxdata/telegraf"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/pdata"
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

		gotTime := mockExporterFactory.consumedMetrics.ResourceMetrics().At(0).InstrumentationLibraryMetrics().At(0).Metrics().At(0).Gauge().DataPoints().At(0).Timestamp().AsTime()
		assert.WithinDuration(t, time.Now(), gotTime, time.Second)
	})
}

func TestInflux2Otel_unknownSchema(t *testing.T) {
	t.Run("telegraf", func(t *testing.T) {
		lp := `
cpu,cpu=cpu4,host=777348dc6343 usage_user=0.10090817356207936,usage_system=0.3027245206862381,usage_iowait=0,invalid="ignored" 1395066363000000123
`

		expect := pdata.NewMetrics()
		metrics := expect.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics().AppendEmpty().Metrics()
		metric := metrics.AppendEmpty()
		metric.SetName("cpu_usage_iowait")
		metric.SetDataType(pdata.MetricDataTypeGauge)
		dp := metric.Gauge().DataPoints().AppendEmpty()
		dp.LabelsMap().Insert("cpu", "cpu4")
		dp.LabelsMap().Insert("host", "777348dc6343")
		dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
		dp.SetValue(0.0)
		metric = metrics.AppendEmpty()
		metric.SetName("cpu_usage_system")
		metric.SetDataType(pdata.MetricDataTypeGauge)
		dp = metric.Gauge().DataPoints().AppendEmpty()
		dp.LabelsMap().Insert("cpu", "cpu4")
		dp.LabelsMap().Insert("host", "777348dc6343")
		dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
		dp.SetValue(0.3027245206862381)
		metric = metrics.AppendEmpty()
		metric.SetName("cpu_usage_user")
		metric.SetDataType(pdata.MetricDataTypeGauge)
		dp = metric.Gauge().DataPoints().AppendEmpty()
		dp.LabelsMap().Insert("cpu", "cpu4")
		dp.LabelsMap().Insert("host", "777348dc6343")
		dp.SetTimestamp(pdata.Timestamp(1395066363000000123))
		dp.SetValue(0.10090817356207936)

		assertOtel2InfluxTelegraf(t, lp, telegraf.Untyped, expect)
	})
}

func TestInflux2Otel_gaugeNonPrometheus(t *testing.T) {
	t.Run("telegraf", func(t *testing.T) {
		lp := `
swap,host=8eaaf6b73054 used_percent=1.5,total=1073737728i 1626302080000000000
`
		expect := pdata.NewMetrics()
		metrics := expect.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics().AppendEmpty().Metrics()
		metric := metrics.AppendEmpty()
		metric.SetName("swap_used_percent")
		metric.SetDataType(pdata.MetricDataTypeGauge)
		dp := metric.Gauge().DataPoints().AppendEmpty()
		dp.LabelsMap().Insert("host", "8eaaf6b73054")
		dp.SetTimestamp(pdata.Timestamp(1626302080000000000))
		dp.SetValue(1.5)
		metric = metrics.AppendEmpty()
		metric.SetName("swap_total")
		metric.SetDataType(pdata.MetricDataTypeGauge)
		dp = metric.Gauge().DataPoints().AppendEmpty()
		dp.LabelsMap().Insert("host", "8eaaf6b73054")
		dp.SetTimestamp(pdata.Timestamp(1626302080000000000))
		dp.SetValue(1073737728)

		assertOtel2InfluxTelegraf(t, lp, telegraf.Gauge, expect)
	})
}

func TestInflux2Otel_counterNonPrometheus(t *testing.T) {
	t.Run("telegraf", func(t *testing.T) {
		lp := `
swap,host=8eaaf6b73054 in=32768i,out=12021760i 1626302080000000000
`
		expect := pdata.NewMetrics()
		metrics := expect.ResourceMetrics().AppendEmpty().InstrumentationLibraryMetrics().AppendEmpty().Metrics()
		metric := metrics.AppendEmpty()
		metric.SetName("swap_in")
		metric.SetDataType(pdata.MetricDataTypeSum)
		metric.Sum().SetIsMonotonic(true)
		metric.Sum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
		dp := metric.Sum().DataPoints().AppendEmpty()
		dp.LabelsMap().Insert("host", "8eaaf6b73054")
		dp.SetTimestamp(pdata.Timestamp(1626302080000000000))
		dp.SetValue(32768)
		metric = metrics.AppendEmpty()
		metric.SetName("swap_out")
		metric.SetDataType(pdata.MetricDataTypeSum)
		metric.Sum().SetIsMonotonic(true)
		metric.Sum().SetAggregationTemporality(pdata.AggregationTemporalityCumulative)
		dp = metric.Sum().DataPoints().AppendEmpty()
		dp.LabelsMap().Insert("host", "8eaaf6b73054")
		dp.SetTimestamp(pdata.Timestamp(1626302080000000000))
		dp.SetValue(12021760)

		assertOtel2InfluxTelegraf(t, lp, telegraf.Counter, expect)
	})
}
