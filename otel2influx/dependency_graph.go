package otel2influx

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/ptrace"
)

type DependencyGraph interface {
	Start(ctx context.Context, host component.Host) error
	ReportSpan(ctx context.Context, span ptrace.Span, resource pcommon.Resource)
	Shutdown(ctx context.Context) error
}
