package internal

import (
	"bytes"
	"context"
	"database/sql"
	"errors"
	"fmt"
	"io"
	"net/http"
	"sync"
	"time"

	"github.com/golang/groupcache/lru"
	"github.com/influxdata/line-protocol/v2/lineprotocol"
	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/spanstore"
	"go.uber.org/multierr"
	"go.uber.org/zap"

	"github.com/influxdata/influxdb-observability/common"
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
	logger         *zap.Logger
	recentTraces   *lru.Cache
	recentTracesMu sync.Mutex
	httpClient     *http.Client

	dbSrc                                                         *sql.DB
	bucketNameSrc, tableSpansSrc, tableLogsSrc, tableSpanLinksSrc string

	writeURLArchive                                                               string
	bucketNameArchive, tableSpansArchive, tableLogsArchive, tableSpanLinksArchive string
}

func scanRowToMap(columnTypes []*sql.ColumnType, rowScannable []interface{}, rows *sql.Rows) (map[string]interface{}, error) {
	if len(columnTypes) == 0 {
		var err error
		columnTypes, err = rows.ColumnTypes()
		if err != nil {
			return nil, err
		}
		rowScannable = make([]interface{}, len(columnTypes))
		for i := range rowScannable {
			rowScannable[i] = new(interface{})
		}
	}

	if err := rows.Scan(rowScannable...); err != nil {
		return nil, err
	}

	m := make(map[string]interface{}, len(columnTypes))
	for i, ct := range columnTypes {
		v := *(rowScannable[i].(*interface{}))
		m[ct.Name()] = v
	}

	return m, nil
}

func (iwa *influxdbWriterArchive) WriteSpan(ctx context.Context, span *model.Span) error {
	iwa.recentTracesMu.Lock()
	if _, found := iwa.recentTraces.Get(span.TraceID.High ^ span.TraceID.Low); found {
		iwa.recentTracesMu.Unlock()
		return nil
	}
	iwa.recentTraces.Add(span.TraceID.High^span.TraceID.Low, struct{}{})
	iwa.recentTracesMu.Unlock()

	lpEncoder := new(lineprotocol.Encoder)
	lpEncoder.SetLax(true)
	lpEncoder.SetPrecision(lineprotocol.Nanosecond)

	// trace spans

	query := queryGetTraceSpans(iwa.tableSpansSrc, span.TraceID)
	iwa.logger.Warn(query)
	rows, err := iwa.dbSrc.QueryContext(ctx, query)
	if err != nil {
		return err
	}
	defer rows.Close()

	var ct []*sql.ColumnType
	var rs []interface{}

	for rows.Next() {
		row, err := scanRowToMap(ct, rs, rows)
		if err != nil {
			return err
		}

		lpEncoder.StartLine(iwa.tableSpansArchive)
		var tagCount int
		for _, k := range []string{common.AttributeTraceID, common.AttributeSpanID} {
			if stringValue, ok := row[k].(string); ok {
				lpEncoder.AddTag(k, stringValue)
				tagCount++
			} else {
				iwa.logger.Sugar().Warn("expected column %s to have type string but got %T", k, row[k])
			}
		}
		if tagCount != 2 {
			return fmt.Errorf("expected 2 tags, but got %d; should find columns %s, %s",
				tagCount, common.AttributeTraceID, common.AttributeSpanID)
		}
		for k, v := range row {
			switch k {
			case common.AttributeTraceID, common.AttributeSpanID, common.AttributeTime:
			default:
				if v == nil {
					continue
				}
				if fieldValue, ok := lineprotocol.NewValue(v); ok {
					lpEncoder.AddField(k, fieldValue)
				} else {
					iwa.logger.Sugar().Warn("failed to cast column %s (%T) to line protocol field value", k, v)
				}
			}
		}
		foundTime := false
		if v, ok := row[common.AttributeTime]; ok && v != nil {
			if timeValue, ok := v.(time.Time); ok {
				foundTime = true
				lpEncoder.EndLine(timeValue)
			} else {
				iwa.logger.Sugar().Warn("expected column %s to have type time but got %T", common.AttributeTime, v)
			}
		}
		if !foundTime {
			return fmt.Errorf("time value not found in row")
		}
	}
	if err = multierr.Combine(rows.Err(), rows.Close()); err != nil {
		return err
	}

	// trace events

	query = queryGetTraceEvents(iwa.tableLogsSrc, span.TraceID)
	iwa.logger.Warn(query)
	rows, err = iwa.dbSrc.QueryContext(ctx, query)
	if err != nil {
		iwa.logger.Error("failed to query logs (span events) table", zap.Error(err))
	} else {
		defer rows.Close()

		ct = ct[:]
		rs = rs[:]

		for rows.Next() {
			row, err := scanRowToMap(ct, rs, rows)
			if err != nil {
				return err
			}

			lpEncoder.StartLine(iwa.tableLogsArchive)
			var tagCount int
			for _, k := range []string{common.AttributeTraceID, common.AttributeSpanID} {
				if stringValue, ok := row[k].(string); ok {
					lpEncoder.AddTag(k, stringValue)
					tagCount++
				} else {
					iwa.logger.Sugar().Warn("expected column %s to have type string but got %T", k, row[k])
				}
			}
			if tagCount != 2 {
				return fmt.Errorf("expected 2 tags, but got %d; should find columns %s, %s",
					tagCount, common.AttributeTraceID, common.AttributeSpanID)
			}
			for k, v := range row {
				switch k {
				case common.AttributeTraceID, common.AttributeSpanID, common.AttributeTime:
				default:
					if v == nil {
						continue
					}
					if fieldValue, ok := lineprotocol.NewValue(v); ok {
						lpEncoder.AddField(k, fieldValue)
					} else {
						iwa.logger.Sugar().Warn("failed to cast column %s (%T) to line protocol field value", k, v)
					}
				}
			}
			foundTime := false
			if v, ok := row[common.AttributeTime]; ok && v != nil {
				if timeValue, ok := v.(time.Time); ok {
					foundTime = true
					lpEncoder.EndLine(timeValue)
				} else {
					iwa.logger.Sugar().Warn("expected column %s to have type time but got %T", common.AttributeTime, v)
				}
			}
			if !foundTime {
				return fmt.Errorf("time value not found in row")
			}
		}
		if err = multierr.Combine(rows.Err(), rows.Close()); err != nil {
			return err
		}
	}

	// trace span links

	query = queryGetTraceLinks(iwa.tableSpanLinksSrc, span.TraceID)
	iwa.logger.Warn(query)
	rows, err = iwa.dbSrc.QueryContext(ctx, query)
	if err != nil {
		iwa.logger.Error("failed to query span links table", zap.Error(err))
	} else {
		defer rows.Close()

		ct = ct[:]
		rs = rs[:]

		for rows.Next() {
			row, err := scanRowToMap(ct, rs, rows)
			if err != nil {
				return err
			}

			lpEncoder.StartLine(iwa.tableSpanLinksArchive)
			var tagCount int
			for _, k := range []string{common.AttributeTraceID, common.AttributeSpanID, common.AttributeLinkedTraceID, common.AttributeLinkedSpanID} {
				if stringValue, ok := row[k].(string); ok {
					lpEncoder.AddTag(k, stringValue)
					tagCount++
				} else {
					iwa.logger.Sugar().Warn("expected column %s to have type string but got %T", k, row[k])
				}
			}
			if tagCount != 4 {
				return fmt.Errorf("expected 4 tags, but got %d; should find columns %s, %s, %s, %s",
					tagCount, common.AttributeTraceID, common.AttributeSpanID, common.AttributeLinkedTraceID, common.AttributeLinkedSpanID)
			}
			for k, v := range row {
				switch k {
				case common.AttributeTraceID, common.AttributeSpanID, common.AttributeLinkedTraceID, common.AttributeLinkedSpanID, common.AttributeTime:
				default:
					if v == nil {
						continue
					}
					if fieldValue, ok := lineprotocol.NewValue(v); ok {
						lpEncoder.AddField(k, fieldValue)
					} else {
						iwa.logger.Sugar().Warn("failed to cast column %s (%T) to line protocol field value", k, v)
					}
				}
			}
			foundTime := false
			if v, ok := row[common.AttributeTime]; ok && v != nil {
				if timeValue, ok := v.(time.Time); ok {
					foundTime = true
					lpEncoder.EndLine(timeValue)
				} else {
					iwa.logger.Sugar().Warn("expected column %s to have type time but got %T", common.AttributeTime, v)
				}
			}
			if !foundTime {
				return fmt.Errorf("time value not found in row")
			}
		}
		if err = multierr.Combine(rows.Err(), rows.Close()); err != nil {
			return err
		}
	}

	if err = lpEncoder.Err(); err != nil {
		return err
	}

	req, err := http.NewRequestWithContext(ctx, http.MethodPost, iwa.writeURLArchive, bytes.NewReader(lpEncoder.Bytes()))
	if err != nil {
		return err
	}
	if res, err := iwa.httpClient.Do(req); err != nil {
		return err
	} else if body, err := io.ReadAll(res.Body); err != nil {
		return err
	} else if err = res.Body.Close(); err != nil {
		return err
	} else if res.StatusCode/100 != 2 {
		return fmt.Errorf("line protocol write returned %q %q", res.Status, string(body))
	}

	return nil
}
