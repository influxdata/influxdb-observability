package influx2otel_test

import (
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pmetric"

	"github.com/influxdata/influxdb-observability/common"
)

func assertMetricsEqual(t *testing.T, expect, got pmetric.Metrics) {
	t.Helper()

	common.SortResourceMetrics(expect.ResourceMetrics())
	expectJSON, err := pmetric.NewJSONMarshaler().MarshalMetrics(expect)
	require.NoError(t, err)

	common.SortResourceMetrics(got.ResourceMetrics())
	gotJSON, err := pmetric.NewJSONMarshaler().MarshalMetrics(got)
	require.NoError(t, err)

	assert.JSONEq(t, string(expectJSON), string(gotJSON))
}
