package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/url"
	"strings"

	"github.com/golang/groupcache/lru"
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

const (
	tableSpans                 = "spans"
	tableSpansArchive          = "archive-spans"
	tableLogs                  = "logs"
	tableLogsArchive           = "archive-logs"
	tableSpanLinks             = "span-links"
	tableSpanLinksArchive      = "archive-span-links"
	tableJaegerDependencyLinks = "jaeger-dependencylinks"
)

type InfluxdbStorage struct {
	logger *zap.Logger

	client influxdb2.Client
	db     *sql.DB

	reader           spanstore.Reader
	readerDependency dependencystore.Reader
	writer           spanstore.Writer
	readerArchive    spanstore.Reader
	writerArchive    spanstore.Writer
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
	if ok, err := client.Ping(ctx); err != nil || !ok {
		return nil, fmt.Errorf("failed to ping InfluxDB: %w", err)
	}

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

	sslmode := "verify-full"
	if config.InfluxdbTLSDisable {
		sslmode = "disable"
	}

	dsn := fmt.Sprintf(
		"host='%s' port=%d user='' password='%s' database='%s' passfile='' servicefile='' sslmode=%s",
		influxdbClientHost, 5432, config.InfluxdbToken, *bucket.Id, sslmode)
	db, err := sql.Open("postgres", dsn)
	if err != nil {
		return nil, err
	}

	logger := LoggerFromContext(ctx)
	reader := &influxdbReader{
		logger:         logger.With(zap.String("influxdb", "reader")),
		db:             db,
		tableSpans:     tableSpans,
		tableLogs:      tableLogs,
		tableSpanLinks: tableSpanLinks,
	}
	readerDependency := &influxdbDependencyReader{
		logger:               logger.With(zap.String("influxdb", "reader-dependency")),
		ir:                   reader,
		tableDependencyLinks: tableJaegerDependencyLinks,
	}
	writer := &influxdbWriterNoop{
		logger: logger.With(zap.String("influxdb", "reader-dependency")),
	}
	readerArchive := &influxdbReader{
		logger:         logger.With(zap.String("influxdb", "reader-archive")),
		db:             db,
		tableSpans:     tableSpansArchive,
		tableLogs:      tableLogsArchive,
		tableSpanLinks: tableSpanLinksArchive,
	}
	writerArchive := &influxdbWriterArchive{
		logger:                logger.With(zap.String("influxdb", "writer-archive")),
		queryAPI:              client.QueryAPI(*bucket.OrgID),
		recentTraces:          lru.New(100),
		bucketName:            bucket.Name,
		tableSpans:            tableSpans,
		tableLogs:             tableLogs,
		tableSpanLinks:        tableSpanLinks,
		tableSpansArchive:     tableSpansArchive,
		tableLogsArchive:      tableLogsArchive,
		tableSpanLinksArchive: tableSpanLinksArchive,
	}

	return &InfluxdbStorage{
		logger:           logger,
		client:           client,
		db:               db,
		reader:           reader,
		readerDependency: readerDependency,
		writer:           writer,
		readerArchive:    readerArchive,
		writerArchive:    writerArchive,
	}, nil
}

func (i *InfluxdbStorage) Close() error {
	i.client.Close()
	return i.db.Close()
}

func (i *InfluxdbStorage) SpanReader() spanstore.Reader {
	return i.reader
}

func (i *InfluxdbStorage) DependencyReader() dependencystore.Reader {
	return i.readerDependency
}

func (i *InfluxdbStorage) SpanWriter() spanstore.Writer {
	return i.writer
}

func (i *InfluxdbStorage) ArchiveSpanReader() spanstore.Reader {
	return i.readerArchive
}

func (i *InfluxdbStorage) ArchiveSpanWriter() spanstore.Writer {
	return i.writerArchive
}

func executeQuery(ctx context.Context, db *sql.DB, query string, f func(record map[string]interface{}) error) error {
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
