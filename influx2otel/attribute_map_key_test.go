package influx2otel

import (
	"testing"

	"github.com/stretchr/testify/require"
	"go.opentelemetry.io/collector/pdata/pcommon"
)

func TestAttributeMapKey(t *testing.T) {
	first := pcommon.NewMap()
	first.PutStr("service.name", "api")
	first.PutStr("region", "west")

	second := pcommon.NewMap()
	second.PutStr("region", "west")
	second.PutStr("service.name", "api")

	require.Equal(t, attributeMapKey(first), attributeMapKey(second))

	boundaryA := pcommon.NewMap()
	boundaryA.PutStr("a", "bc")
	boundaryB := pcommon.NewMap()
	boundaryB.PutStr("ab", "c")
	require.NotEqual(t, attributeMapKey(boundaryA), attributeMapKey(boundaryB))
}
