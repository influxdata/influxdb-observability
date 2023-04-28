package internal

import (
	"context"
	"time"

	"github.com/jaegertracing/jaeger/model"
	"github.com/jaegertracing/jaeger/storage/dependencystore"
	"go.uber.org/zap"
)

var _ dependencystore.Reader = (*influxdbDependencyReader)(nil)

type influxdbDependencyReader struct {
	logger *zap.Logger

	executeQuery func(ctx context.Context, query string, f func(record map[string]interface{}) error) error
}

func (idr *influxdbDependencyReader) GetDependencies(ctx context.Context, endTs time.Time, lookback time.Duration) ([]model.DependencyLink, error) {
	var dependencyLinks []model.DependencyLink

	f := func(record map[string]interface{}) error {
		var parentService string
		if v, found := record[columnServiceGraphClient]; !found || v == nil {
			idr.logger.Warn("parent service not found in dependency link")
			return nil
		} else {
			parentService = v.(string)
		}
		var childService string
		if v, found := record[columnServiceGraphServer]; !found || v == nil {
			idr.logger.Warn("child service not found in dependency link")
			return nil
		} else {
			childService = v.(string)
		}
		var calls int64
		if v, found := record[columnServiceGraphCount]; !found || v == nil {
			idr.logger.Warn("calls not found in dependency link")
			return nil
		} else {
			calls = v.(int64)
		}

		dependencyLinks = append(dependencyLinks, model.DependencyLink{
			Parent:    parentService,
			Child:     childService,
			CallCount: uint64(calls),
		})

		return nil
	}

	err := idr.executeQuery(ctx, queryGetDependencies(endTs, lookback), f)
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}
	return dependencyLinks, nil
}
