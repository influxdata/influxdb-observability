package influx2otel_test

import (
	"testing"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/otlp"
	"go.opentelemetry.io/collector/model/pdata"
)

func assertResourceMetricsEqual(t *testing.T, expect, got pdata.ResourceMetricsSlice) {
	t.Helper()

	common.SortResourceMetrics(expect)
	expectMetrics := pdata.NewMetrics()
	expect.CopyTo(expectMetrics.ResourceMetrics())
	expectJSON, err := otlp.NewJSONMetricsMarshaler().MarshalMetrics(expectMetrics)
	require.NoError(t, err)

	common.SortResourceMetrics(got)
	gotMetrics := pdata.NewMetrics()
	got.CopyTo(gotMetrics.ResourceMetrics())
	gotJSON, err := otlp.NewJSONMetricsMarshaler().MarshalMetrics(gotMetrics)
	require.NoError(t, err)

	assert.JSONEq(t, string(expectJSON), string(gotJSON))
}
