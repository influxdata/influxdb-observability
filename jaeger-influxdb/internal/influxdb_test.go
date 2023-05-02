package internal

import (
	"fmt"
	"strings"
	"testing"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
)

func TestComposeHostPortFromAddr(t *testing.T) {
	logger := zap.NewNop()

	for _, testCase := range []struct {
		influxdbAddr  string
		disableTLS    bool
		expectedValue string
		expectError   bool
	}{
		{"host.tld:80", false, "host.tld:80", false},
		{"host.tld:80", true, "host.tld:80", false},
		{"host:80", false, "host:80", false},
		{"host:80", true, "host:80", false},
		{"host.tld", false, "host.tld:443", false},
		{"host.tld", true, "host.tld:443", false},
		{"host", false, "host:443", false},
		{"host", true, "host:443", false},
		{"http://host:80", true, "host:80", false},
		{"http://host", true, "host:443", false},
		{"http://host:", true, "host:443", false},
		{"grpc://host:80", true, "host:80", false},
		{"grpc+tcp://host:80", true, "host:80", false},
		{"https://host:80", false, "host:80", false},
		{"https://host", false, "host:443", false},
		{"https://host:", false, "host:443", false},
		{"grpc+tls://host:80", false, "host:80", false},

		{":80", false, "", true},
		{":80", true, "", true},
		{"host:", false, "", true},
		{"host:", true, "", true},
		{":", false, "", true},
		{":", true, "", true},
		{"", false, "", true},
		{"", true, "", true},
		{"://", true, "", true},
		{"://", false, "", true},
		{"//", true, "", true},
		{"//", false, "", true},
		{"http://host", false, "", true},
		{"http://:80", true, "", true},
		{"http://:", true, "", true},
		{"http://", true, "", true},
		{"https://host", true, "", true},
		{"https://:80", false, "", true},
		{"https://:", false, "", true},
		{"https://", false, "", true},
	} {
		t.Run(fmt.Sprintf("%s--%v", strings.ReplaceAll(testCase.influxdbAddr, "://", "_"), testCase.disableTLS), func(t *testing.T) {
			testCase := testCase
			actualValue, actualErr := composeHostPortFromAddr(logger, testCase.influxdbAddr, testCase.disableTLS)
			assert.Equal(t, testCase.expectedValue, actualValue)
			if testCase.expectError {
				assert.Error(t, actualErr)
			} else {
				assert.NoError(t, actualErr)
			}
		})
	}
}
