package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	influxdb2 "github.com/influxdata/influxdb-client-go/v2"
	"github.com/influxdata/influxdb-client-go/v2/domain"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	_ "github.com/lib/pq"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var _ shared.StoragePlugin = (*InfluxdbStorage)(nil)
var _ shared.ArchiveStoragePlugin = (*InfluxdbStorage)(nil)

type InfluxdbStorage struct {
	logger    *zap.Logger
	closeFunc func() error

	reader        *influxdbReader
	archiveReader *influxdbReader
	archiveWriter *influxdbWriter
}

func NewInfluxdbStorage(ctx context.Context, config *Config) (*InfluxdbStorage, error) {
	var influxdbClientHost string
	if config.InfluxdbAddr == "" {
		return nil, errors.New("InfluxDB address unspecified")
	} else if strings.Contains(config.InfluxdbAddr, ":") {
		influxdbHost, influxdbPort, err := net.SplitHostPort(config.InfluxdbAddr)
		if err != nil || influxdbHost == "" {
			return nil, fmt.Errorf("invalid InfluxDB address '%s': %w", config.InfluxdbAddr, err)
		}
		if influxdbPort == "" {
			influxdbClientHost = influxdbHost
		}
		influxdbClientHost = net.JoinHostPort(influxdbHost, influxdbPort)
	} else {
		influxdbClientHost = config.InfluxdbAddr
	}

	clientURL := (&url.URL{Scheme: "https", Host: influxdbClientHost}).String()
	options := influxdb2.DefaultOptions()
	options.HTTPOptions().SetHTTPRequestTimeout(uint(config.InfluxdbTimeout.Seconds()))
	client := influxdb2.NewClientWithOptions(clientURL, config.InfluxdbToken, options)
	defer client.Close()

	var bucket *domain.Bucket
	var err error
	if config.InfluxdbBucketid != "" {
		bucket, err = client.BucketsAPI().FindBucketByID(ctx, config.InfluxdbBucketid)
	} else if config.InfluxdbBucketname != "" {
		bucket, err = client.BucketsAPI().FindBucketByName(ctx, config.InfluxdbBucketname)
	} else {
		err = errors.New("provide bucket ID or bucket name via flags")
	}
	if err != nil {
		return nil, err
	}

	dsn := fmt.Sprintf(
		"host='%s' port=%d user='' password='%s' database='%s' passfile='' servicefile='' sslmode=verify-full",
		influxdbClientHost, 5432, config.InfluxdbToken, *bucket.Id)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	logger := LoggerFromContext(ctx)
	return &InfluxdbStorage{
		logger:    logger,
		closeFunc: db.Close,
		reader: &influxdbReader{
			logger:               logger.With(zap.String("influxdb", "reader")),
			db:                   db,
			tableSpans:           "spans",
			tableLogs:            "logs",
			tableSpanLinks:       "span-links",
			tableDependencyLinks: "jaeger-dependencylinks",
		},
		archiveReader: &influxdbReader{
			logger:         logger.With(zap.String("influxdb", "archive-reader")),
			db:             db,
			tableSpans:     "archive-spans",
			tableLogs:      "archive-logs",
			tableSpanLinks: "archive-span-links",
		},
		archiveWriter: nil,
	}, nil
}

/*
func newFlightsqlClient(addr, token, bucketid string) (*flightsql.Client, error) {
	middlewares := []flight.ClientMiddleware{tokenMiddleware(token, bucketid)}
	flightsqlClient, err := flightsql.NewClient(addr, nil, middlewares)
	if err != nil {
		return nil, err
	}
	return flightsqlClient, nil
}

func tokenMiddleware(token, bucketID string) flight.ClientMiddleware {
	contextWithTokenAndBucketid := func(ctx context.Context) context.Context {
		return grpcMetadata.AppendToOutgoingContext(ctx,
			"authorization", fmt.Sprintf("Token %s", token),
			"bucket-id", bucketID)
	}
	return flight.ClientMiddleware{
		Stream: func(ctx context.Context, desc *grpc.StreamDesc, cc *grpc.ClientConn, method string, streamer grpc.Streamer, opts ...grpc.CallOption) (grpc.ClientStream, error) {
			ctx = contextWithTokenAndBucketid(ctx)
			return streamer(ctx, desc, cc, method, opts...)
		},
		Unary: func(ctx context.Context, method string, req, reply interface{}, cc *grpc.ClientConn, invoker grpc.UnaryInvoker, opts ...grpc.CallOption) error {
			ctx = contextWithTokenAndBucketid(ctx)
			return invoker(ctx, method, req, reply, cc, opts...)
		},
	}
}
*/

func (i *InfluxdbStorage) SpanReader() spanstore.Reader {
	return i.reader
}

func (i *InfluxdbStorage) SpanWriter() spanstore.Writer {
	panic("not implemented")
}

func (i *InfluxdbStorage) DependencyReader() dependencystore.Reader {
	return i.reader
}

func (i *InfluxdbStorage) ArchiveSpanReader() spanstore.Reader {
	return i.archiveReader
}

func (i *InfluxdbStorage) ArchiveSpanWriter() spanstore.Writer {
	return i.archiveWriter
}

func executeQuery(ctx context.Context, db *sql.DB, query string, f func(record map[string]interface{}) error) error {
	logger := LoggerFromContext(ctx)
	logger.Info(query)
	rows, err := db.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer func() { _ = rows.Close() }()

	columns, err := rows.Columns()
	if err != nil {
		return err
	}
	m := make(map[string]interface{}, len(columns))

	for rows.Next() {
		dest := make([]interface{}, len(columns))
		destP := make([]interface{}, len(columns))
		for i := range dest {
			destP[i] = &dest[i]
		}
		if err = rows.Scan(destP...); err != nil {
			return err
		}
		for i, columnName := range columns {
			v := destP[i].(*interface{})
			m[columnName] = *v
		}
		if err = f(m); err != nil {
			return err
		}
	}

	return multierr.Combine(rows.Err(), rows.Close())
}
