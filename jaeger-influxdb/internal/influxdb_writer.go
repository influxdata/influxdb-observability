package internal

import (
	"context"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
)

var _ spanstore.Writer = (*influxdbWriter)(nil)

type influxdbWriter struct {
}

func (iw *influxdbWriter) WriteSpan(ctx context.Context, span *model.Span) error {
	//TODO implement me
	panic("implement me")
}
