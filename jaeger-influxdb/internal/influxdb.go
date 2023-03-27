package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"strings"

	"github.com/apache/arrow-adbc/go/adbc"
	"github.com/apache/arrow-adbc/go/adbc/driver/flightsql"
	_ "github.com/apache/arrow-adbc/go/adbc/sqldriver/flightsql"
	"github.com/golang/groupcache/lru"
	"github.com/jaegertracing/jaeger/plugin/storage/grpc/shared"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.uber.org/multierr"
	"go.uber.org/zap"
)

var _ shared.StoragePlugin = (*InfluxdbStorage)(nil)
var _ shared.ArchiveStoragePlugin = (*InfluxdbStorage)(nil)

const (
	tableSpans                 = "spans"
	tableLogs                  = "logs"
	tableSpanLinks             = "span-links"
	tableJaegerDependencyLinks = "jaeger-dependencylinks"
)

type InfluxdbStorage struct {
	logger *zap.Logger

	db               *sql.DB
	reader           spanstore.Reader
	readerDependency dependencystore.Reader
	writer           spanstore.Writer

	dbArchive     *sql.DB
	readerArchive spanstore.Reader
	writerArchive spanstore.Writer
}

func NewInfluxdbStorage(ctx context.Context, config *Config) (*InfluxdbStorage, error) {
	logger := LoggerFromContext(ctx)

	influxdbAddr, err := composeHostPortFromAddr(config.InfluxdbAddr)
	if err != nil {
		return nil, err
	}
	if config.InfluxdbBucket == "" {
		return nil, fmt.Errorf("influxdb-bucket not specified, either by flag or env var")
	}
	if config.InfluxdbBucket == config.InfluxdbBucketArchive {
		return nil, fmt.Errorf("primary bucket and archive bucket must be different, but both are set to '%s'", config.InfluxdbBucket)
	}
	if config.InfluxdbBucketArchive == "" {
		logger.Warn("influxdb-bucket-archive not specified, so trace archiving is disabled")
	}

	uriScheme := "grpc+tls"
	if config.InfluxdbTLSDisable {
		uriScheme = "grpc+tcp"
	}
	dsn := strings.Join([]string{
		fmt.Sprintf("%s=%s://%s/", adbc.OptionKeyURI, uriScheme, influxdbAddr),
		fmt.Sprintf("%s=Bearer %s", flightsql.OptionAuthorizationHeader, config.InfluxdbToken),
		fmt.Sprintf("%s=%s", flightsql.OptionRPCCallHeaderPrefix+"bucket-name", config.InfluxdbBucket),
	}, " ; ")

	db, err := sql.Open("flightsql", dsn)
	if err != nil {
		row := db.QueryRowContext(ctx, "SELECT 1")
		var v int
		err = multierr.Combine(row.Scan(&v))
		if err == nil && v != 1 {
			err = errors.New("failed to ping database")
		}
	}
	if err != nil {
		return nil, fmt.Errorf("failed to contact InfluxDB query service: %w", err)
	}

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
		logger: logger.With(zap.String("influxdb", "writer")),
	}

	var readerArchive spanstore.Reader
	var writerArchive spanstore.Writer
	var dbArchive *sql.DB

	if config.InfluxdbBucketArchive != "" {
		dsnArchive := strings.Join([]string{
			fmt.Sprintf("%s=%s://%s/", adbc.OptionKeyURI, uriScheme, influxdbAddr),
			fmt.Sprintf("%s=Bearer %s", flightsql.OptionAuthorizationHeader, config.InfluxdbToken),
			fmt.Sprintf("%s=%s", flightsql.OptionRPCCallHeaderPrefix+"bucket-name", config.InfluxdbBucketArchive),
		}, " ; ")

		dbArchive, err = sql.Open("flightsql", dsnArchive)
		if err != nil {
			return nil, err
		}

		readerArchive = &influxdbReader{
			logger: logger.With(zap.String("influxdb", "reader-archive")),

			db:             dbArchive,
			tableSpans:     tableSpans,
			tableLogs:      tableLogs,
			tableSpanLinks: tableSpanLinks,
		}
		writerArchive = &influxdbWriterArchive{
			logger:       logger.With(zap.String("influxdb", "writer-archive")),
			recentTraces: lru.New(100),
			httpClient:   &http.Client{Timeout: config.InfluxdbTimeout},
			authToken:    config.InfluxdbToken,

			dbSrc:             db,
			bucketNameSrc:     config.InfluxdbBucket,
			tableSpansSrc:     tableSpans,
			tableLogsSrc:      tableLogs,
			tableSpanLinksSrc: tableSpanLinks,

			writeURLArchive:       composeWriteURL(influxdbAddr, config.InfluxdbBucketArchive),
			bucketNameArchive:     config.InfluxdbBucketArchive,
			tableSpansArchive:     tableSpans,
			tableLogsArchive:      tableLogs,
			tableSpanLinksArchive: tableSpanLinks,
		}
	}

	return &InfluxdbStorage{
		logger: logger,

		db:               db,
		reader:           reader,
		readerDependency: readerDependency,
		writer:           writer,

		dbArchive:     dbArchive,
		readerArchive: readerArchive,
		writerArchive: writerArchive,
	}, nil
}

func (i *InfluxdbStorage) Close() error {
	err := i.db.Close()
	if i.dbArchive != nil {
		err = multierr.Append(err, i.dbArchive.Close())
	}
	return err
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

	rowValues := make([]interface{}, len(columns))
	for i := range rowValues {
		rowValues[i] = new(interface{})
	}

	for rows.Next() {
		if err = rows.Scan(rowValues[:]...); err != nil {
			return err
		}
		for i, columnName := range columns {
			v := rowValues[i].(*interface{})
			if v == nil || *v == nil {
				delete(m, columnName)
			} else {
				m[columnName] = *v
			}
		}
		if err = f(m); err != nil {
			return err
		}
	}

	return multierr.Combine(rows.Err(), rows.Close())
}

func composeWriteURL(influxdbClientHost, influxdbBucket string) string {
	writeURL := &url.URL{Scheme: "https", Host: influxdbClientHost, Path: "/api/v2/write"}

	queryValues := writeURL.Query()
	queryValues.Set("precision", "ns")
	queryValues.Set("bucket", influxdbBucket)
	writeURL.RawQuery = queryValues.Encode()

	return writeURL.String()
}

func composeHostPortFromAddr(influxdbAddr string) (string, error) {
	if influxdbAddr == "" {
		return "", errors.New("influxdb-addr not specified, either by flag or env var")
	}
	if !strings.Contains(influxdbAddr, ":") {
		return influxdbAddr + ":443", nil
	}
	_, _, err := net.SplitHostPort(influxdbAddr)
	if err != nil {
		return "", fmt.Errorf("influxdb-addr value is invalid '%s': %w", influxdbAddr, err)
	}
	return influxdbAddr, nil
}
