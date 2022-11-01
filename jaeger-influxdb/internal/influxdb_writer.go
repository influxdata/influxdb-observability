package internal

import (
	"context"
	"errors"

	"github.com/golang/groupcache/lru"
	influxdbapi "github.com/influxdata/influxdb-client-go/v2/api"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.uber.org/zap"
)

var _ spanstore.Writer = (*influxdbWriterNoop)(nil)
var _ spanstore.Writer = (*influxdbWriterArchive)(nil)

type influxdbWriterNoop struct {
	logger *zap.Logger
}

func (iwn *influxdbWriterNoop) WriteSpan(_ context.Context, _ *model.Span) error {
	iwn.logger.Debug("no-op WriteSpan called")
	return errors.New("WriteSpan is not implemented in this context")
}

type influxdbWriterArchive struct {
	logger                                                     *zap.Logger
	queryAPI                                                   influxdbapi.QueryAPI
	recentTraces                                               *lru.Cache
	bucketName                                                 string
	tableSpans, tableLogs, tableSpanLinks                      string
	tableSpansArchive, tableLogsArchive, tableSpanLinksArchive string
}

func (iwa *influxdbWriterArchive) WriteSpan(ctx context.Context, span *model.Span) error {
	if _, found := iwa.recentTraces.Get(span.TraceID.High ^ span.TraceID.Low); found {
		return nil
	}

	query := archiveTraceDetails(iwa.bucketName, iwa.tableSpans, iwa.tableSpansArchive, span.TraceID)
	iwa.logger.Warn(query)
	if result, err := iwa.queryAPI.Query(ctx, query); err != nil {
		return err
	} else if err = result.Close(); err != nil {
		return err
	}

	query = archiveTraceDetails(iwa.bucketName, iwa.tableLogs, iwa.tableLogsArchive, span.TraceID)
	iwa.logger.Warn(query)
	if result, err := iwa.queryAPI.Query(ctx, query); err != nil {
		return err
	} else if err = result.Close(); err != nil {
		return err
	}

	query = archiveTraceDetails(iwa.bucketName, iwa.tableSpanLinks, iwa.tableSpanLinksArchive, span.TraceID)
	iwa.logger.Warn(query)
	if result, err := iwa.queryAPI.Query(ctx, query); err != nil {
		return err
	} else if err = result.Close(); err != nil {
		return err
	}

	iwa.recentTraces.Add(span.TraceID.High^span.TraceID.Low, struct{}{})

	return nil
}
