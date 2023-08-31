package otel2influx

import (
	"go.opentelemetry.io/collector/pdata/pcommon"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"
)

func ResourceToTags(resource pcommon.Resource, tags map[string]string) map[string]string {
	resource.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k != "" {
			tags[k] = v.AsString()
		}
		return true
	})
	return tags
}

func InstrumentationScopeToTags(instrumentationScope pcommon.InstrumentationScope, tags map[string]string) map[string]string {
	if instrumentationScope.Name() != "" {
		tags[semconv.OtelLibraryName] = instrumentationScope.Name()
	}
	if instrumentationScope.Version() != "" {
		tags[semconv.OtelLibraryVersion] = instrumentationScope.Version()
	}
	instrumentationScope.Attributes().Range(func(k string, v pcommon.Value) bool {
		if k != "" {
			tags[k] = v.AsString()
		}
		return true
	})
	return tags
}
