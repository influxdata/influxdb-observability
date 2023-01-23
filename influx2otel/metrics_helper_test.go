package influx2otel_test

import (
	"testing"

	"github.com/open-telemetry/opentelemetry-collector-contrib/pkg/pdatatest/pmetrictest"
	"github.com/stretchr/testify/assert"
	"go.opentelemetry.io/collector/pdata/pmetric"
)

func assertMetricsEqual(t *testing.T, expect, got pmetric.Metrics) {
	t.Helper()

	assert.NoError(t,
		pmetrictest.CompareMetrics(expect, got,
			pmetrictest.IgnoreMetricDataPointsOrder(),
			pmetrictest.IgnoreMetricsOrder(),
			pmetrictest.IgnoreResourceMetricsOrder(),
			pmetrictest.IgnoreScopeMetricsOrder(),
			pmetrictest.IgnoreSummaryDataPointValueAtQuantileSliceOrder(),
		),
	)
}
