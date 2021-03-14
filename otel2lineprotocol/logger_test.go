package otel2lineprotocol

import (
	"errors"
	"testing"

	"github.com/stretchr/testify/assert"
)

var _ Logger = (*mockErrorLogger)(nil)

type mockErrorLogger struct {
	lastMsg string
	lastKV  []interface{}
}

func (m *mockErrorLogger) Debug(msg string, kv ...interface{}) {
	m.lastMsg = msg
	m.lastKV = kv
}

func TestErrorLogger(t *testing.T) {
	var mel mockErrorLogger
	el := errorLogger{Logger: &mel}

	el.Debug("foo", "bar", "baz")
	assert.Equal(t, mel.lastMsg, "foo")
	assert.Equal(t, mel.lastKV, []interface{}{"bar", "baz"})

	el.Debug("foo", errors.New(""))
	assert.Equal(t, mel.lastKV, []interface{}{"error", errors.New("")})
}
