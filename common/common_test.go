package common

import (
	"github.com/stretchr/testify/assert"
	"testing"
)

func TestResourceNamespace(t *testing.T) {
	assert.False(t, ResourceNamespace.MatchString("foo"))
	assert.False(t, ResourceNamespace.MatchString("foo.bar"))
	assert.True(t, ResourceNamespace.MatchString("service.name"))
	assert.True(t, ResourceNamespace.MatchString("service.foo"))
	assert.True(t, ResourceNamespace.MatchString("faas.instance"))
	assert.False(t, ResourceNamespace.MatchString("faas.execution"))
}
