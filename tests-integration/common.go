package tests

import (
	"fmt"
	"net"
	"regexp"
	"sort"
	"strings"
	"testing"

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
	return assert.Equal(t, cleanupLP(expect), cleanupLP(got))
}

func cleanupLP(s string) []string {
	lines := strings.Split(s, "\n")
	var cleanLines []string
	for _, line := range lines {
		line = strings.TrimSpace(line)
		if len(line) == 0 {
			continue
		}
		cleanLines = append(cleanLines, sortFields(line))
	}
	sort.Strings(cleanLines)
	return cleanLines
}

func sortFields(line string) string {
	fieldsIndexes := regexp.MustCompile(`^\s*(\S+)\s+(\S+)\s*(\d*)\s*$`).FindStringSubmatchIndex(line)
	if len(fieldsIndexes) != 8 {
		panic(fmt.Sprint(len(fieldsIndexes), line))
	}
	fieldsSlice := strings.Split(line[fieldsIndexes[4]:fieldsIndexes[5]], ",")
	sort.Strings(fieldsSlice)
	return line[fieldsIndexes[2]:fieldsIndexes[3]] + " " + strings.Join(fieldsSlice, ",") + " " + line[fieldsIndexes[6]:fieldsIndexes[7]]
}
