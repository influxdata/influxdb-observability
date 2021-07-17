package influx2otel_test

import (
	"testing"

	"github.com/influxdata/influxdb-observability/common"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/model/otlp"
	"go.opentelemetry.io/collector/model/pdata"
)

func assertMetricsEqual(t *testing.T, expect, got pdata.Metrics) {
	t.Helper()

	common.SortResourceMetrics(expect.ResourceMetrics())
	expectJSON, err := otlp.NewJSONMetricsMarshaler().MarshalMetrics(expect)
	require.NoError(t, err)

	common.SortResourceMetrics(got.ResourceMetrics())
	gotJSON, err := otlp.NewJSONMetricsMarshaler().MarshalMetrics(got)
	require.NoError(t, err)

	assert.JSONEq(t, string(expectJSON), string(gotJSON))
}
