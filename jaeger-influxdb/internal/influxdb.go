package internal

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"regexp"
	"strings"
	"time"

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
	tableSpans     = "spans"
	tableLogs      = "logs"
	tableSpanLinks = "span-links"

	tableSpanMetricsCalls            = "calls__sum"                                               // https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.78.0/connector/spanmetricsconnector
	tableSpanMetricsDuration         = "duration_ms_histogram"                                    // https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.78.0/connector/spanmetricsconnector
	tableServiceGraphRequestCount    = "traces_service_graph_request_total__sum"                  // https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.78.0/connector/servicegraphconnector
	tableServiceGraphRequestDuration = "traces_service_graph_request_duration_seconds__histogram" // https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.78.0/connector/servicegraphconnector
	columnServiceGraphClient         = "client"
	columnServiceGraphServer         = "server"
	columnServiceGraphCount          = "value_cumulative_monotonic_int"

	uriSchemeSecure    = "grpc+tls"
	uriSchemeNotSecure = "grpc+tcp"
)

type InfluxdbStorage struct {
	logger *zap.Logger

	queryTimeout time.Duration

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

	influxdbAddr, err := composeHostPortFromAddr(logger, config.InfluxdbAddr, config.InfluxdbTLSDisable)
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

	is := &InfluxdbStorage{
		logger:       logger,
		queryTimeout: config.InfluxdbTimeout,
	}

	uriScheme := uriSchemeSecure
	if config.InfluxdbTLSDisable {
		uriScheme = uriSchemeNotSecure
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
		executeQuery:   is.executeQuery,
		db:             db,
		tableSpans:     tableSpans,
		tableLogs:      tableLogs,
		tableSpanLinks: tableSpanLinks,
	}
	readerDependency := &influxdbDependencyReader{
		logger: logger.With(zap.String("influxdb", "reader-dependency")),
		ir:     reader,
	}
	writer := &influxdbWriterNoop{
		logger: logger.With(zap.String("influxdb", "writer")),
	}

	is.db = db
	is.reader = reader
	is.readerDependency = readerDependency
	is.writer = writer

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
			logger:         logger.With(zap.String("influxdb", "reader-archive")),
			executeQuery:   is.executeQuery,
			db:             dbArchive,
			tableSpans:     tableSpans,
			tableLogs:      tableLogs,
			tableSpanLinks: tableSpanLinks,
		}
		writerArchive = &influxdbWriterArchive{
			logger:       logger.With(zap.String("influxdb", "writer-archive")),
			executeQuery: is.executeQuery,
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

		is.dbArchive = dbArchive
		is.readerArchive = readerArchive
		is.writerArchive = writerArchive
	}

	return is, nil
}

func (is *InfluxdbStorage) Close() error {
	err := is.db.Close()
	if is.dbArchive != nil {
		err = multierr.Append(err, is.dbArchive.Close())
	}
	return err
}

func (is *InfluxdbStorage) SpanReader() spanstore.Reader {
	return is.reader
}

func (is *InfluxdbStorage) DependencyReader() dependencystore.Reader {
	return is.readerDependency
}

func (is *InfluxdbStorage) SpanWriter() spanstore.Writer {
	return is.writer
}

func (is *InfluxdbStorage) ArchiveSpanReader() spanstore.Reader {
	return is.readerArchive
}

func (is *InfluxdbStorage) ArchiveSpanWriter() spanstore.Writer {
	return is.writerArchive
}

func (is *InfluxdbStorage) executeQuery(ctx context.Context, db *sql.DB, query string, f func(record map[string]interface{}) error) error {
	ctx, cancel := context.WithTimeout(ctx, is.queryTimeout)
	defer cancel()

	is.logger.Debug("executing query", zap.String("query", query))

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

func composeHostPortFromAddr(logger *zap.Logger, influxdbAddr string, notSecureFlagHint bool) (string, error) {
	errInvalid := func(err error) error {
		if err == nil {
			return fmt.Errorf("influxdb-addr value is invalid '%s'", influxdbAddr)
		}
		return fmt.Errorf("influxdb-addr value is invalid '%s': %w", influxdbAddr, err)
	}
	hostPort := influxdbAddr

	if hostPort == "" {
		return "", errInvalid(nil)
	}

	reValidURL := regexp.MustCompile(`^(?:([\w+-]*):)?//([\w.-]*)(?::(\w*))?/?$`)

	if parts := reValidURL.FindStringSubmatch(hostPort); len(parts) == 4 {
		// Forgive format scheme://host:port, but not unconditionally
		scheme, host, port := parts[1], parts[2], parts[3]

		validURLSchemes := map[string]bool{
			"http":             true,
			"grpc":             true,
			uriSchemeNotSecure: true,
			"https":            false,
			uriSchemeSecure:    false,
		}

		if notSecureURLScheme, found := validURLSchemes[scheme]; !found || notSecureURLScheme != notSecureFlagHint {
			return "", errInvalid(fmt.Errorf("URL scheme '%s' is not recognized", scheme))
		}
		if host == "" {
			return "", errInvalid(errors.New("host is missing"))
		}
		if port == "" {
			hostPort = host
		} else {
			hostPort = net.JoinHostPort(host, port)
		}
		if scheme == "http" || scheme == "https" {
			logger.Warn(fmt.Sprintf("influxdb-addr value '%s' will be handled as '%s'", influxdbAddr, hostPort))
		}
	}

	if !strings.Contains(hostPort, ":") {
		// If no port specified, assume default port
		hostPort += ":443"
	}

	if host, port, err := net.SplitHostPort(hostPort); err == nil {
		switch {
		case host == "":
			return "", errInvalid(errors.New("host is missing"))
		case port == "":
			return "", errInvalid(errors.New("port is missing"))
		default:
			return net.JoinHostPort(host, port), nil
		}
	} else {
		return "", errInvalid(err)
	}
}
