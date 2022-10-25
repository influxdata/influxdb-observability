package tests

import (
	"net"
	"testing"

	"github.com/influxdata/influxdb/v2/models"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

func findOpenTCPPort(t *testing.T) int {
	t.Helper()
	l, err := net.Listen("tcp", "127.0.0.1:0")
	require.NoError(t, err)
	port := l.Addr().(*net.TCPAddr).Port
	require.NoError(t, l.Close())
	return port
}

func assertLineprotocolEqual(t *testing.T, expect, got string) bool {
	t.Helper()

	expectPoints := parseLineProtocol(t, expect)
	gotPoints := parseLineProtocol(t, got)
	return assert.Equal(t, expectPoints, gotPoints)
}

func parseLineProtocol(t *testing.T, line string) map[string]map[string][]models.Fields {
	points, err := models.ParsePointsString(line)
	require.NoError(t, err)
	fieldsByTagsByMeasurement := make(map[string]map[string][]models.Fields)
	for _, point := range points {
		measurementName := string(point.Name())
		fieldsByTags := fieldsByTagsByMeasurement[measurementName]
		if fieldsByTags == nil {
			fieldsByTagsByMeasurement[measurementName] = make(map[string][]models.Fields)
			fieldsByTags = fieldsByTagsByMeasurement[measurementName]
		}

		tags := point.Tags().String()
		fields, err := point.Fields()
		require.NoError(t, err)
		fieldsByTags[tags] = append(fieldsByTags[tags], fields)
	}
	return fieldsByTagsByMeasurement
}
