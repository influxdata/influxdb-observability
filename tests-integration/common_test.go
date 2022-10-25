package tests

import (
	"net"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"

	"github.com/influxdata/influxdb/v2/models"
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

	expectedPoints, err := parseLineProtocol(expect)
	if err != nil {
		t.Error(err)
	}

	actualPoints, err := parseLineProtocol(got)
	if err != nil {
		t.Error(err)
	}

	sameLength := assert.Len(t, actualPoints, len(expectedPoints))
	if !sameLength {
		return sameLength
	}

	// order of LP within the batch is not guaranteed, so we cannot do pairwise comparison
	// instead, we create a map of Point::HashID() -> Point and compare the two maps

	expectedMap := pointMap(expectedPoints)
	actualMap := pointMap(actualPoints)

	equality := make(map[uint64]bool, len(expectedMap))
	for k, v := range expectedMap {
		equality[k] = assertPointEqual(t, v, actualMap[k])
	}

	return assert.NotContains(t, equality, false)
}

func pointMap(points []models.Point) map[uint64]models.Point {
	m := make(map[uint64]models.Point, len(points))

	for _, p := range points {
		m[p.HashID()] = p
	}

	return m
}

func assertPointEqual(t *testing.T, expected, actual models.Point) bool {
	actualTs := actual.Time()
	expectedTs := expected.Time()
	actualTags := actual.Tags()
	expectedTags := expected.Tags()
	actualFields, _ := actual.Fields()
	expectedFields, _ := expected.Fields()

	timeMatch := assert.Equal(t, expectedTs, actualTs)
	tagsMatch := assert.ElementsMatch(t, expectedTags, actualTags)
	fieldsMatch := assert.Equal(t, expectedFields, actualFields)

	return timeMatch && tagsMatch && fieldsMatch
}

func parseLineProtocol(line string) ([]models.Point, error) {
	points, err := models.ParsePointsString(line)
	return points, err
}
