package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestResourceNamespace(t *testing.T) {
	assert.False(t, resourceNamespace.MatchString("foo"))
	assert.False(t, resourceNamespace.MatchString("foo.bar"))
	assert.True(t, resourceNamespace.MatchString("service.name"))
	assert.True(t, resourceNamespace.MatchString("service.foo"))
	assert.True(t, resourceNamespace.MatchString("faas.instance"))
	assert.False(t, resourceNamespace.MatchString("faas.execution"))
}
