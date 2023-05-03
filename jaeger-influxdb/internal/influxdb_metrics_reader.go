package internal

import (
	"context"
	"fmt"
	"time"

	"github.com/gogo/protobuf/proto"
	"github.com/gogo/protobuf/types"
	"github.com/jaegertracing/jaeger/proto-gen/api_v2/metrics"
	"github.com/jaegertracing/jaeger/storage/metricsstore"
	semconv "go.opentelemetry.io/collector/semconv/v1.16.0"
	"go.uber.org/zap"
	"golang.org/x/exp/slices"
)

var _ metricsstore.Reader = (*influxdbMetricsReader)(nil)

const (
	minStep = time.Nanosecond
)

type influxdbMetricsReader struct {
	logger *zap.Logger

	executeQuery func(ctx context.Context, query string, f func(record map[string]interface{}) error) error

	tableSpans, tableLogs, tableSpanLinks string
}

// GetLatencies gets the latency metrics for a specific quantile (e.g. 0.99) and list of services
// grouped by service and optionally grouped by operation.
func (imr *influxdbMetricsReader) GetLatencies(ctx context.Context, params *metricsstore.LatenciesQueryParameters) (*metrics.MetricFamily, error) {
	imr.logger.Info("GetLatencies called")
	mf, err := imr.getMetric(ctx, spanMetricQueryTypeLatencies, params.BaseQueryParameters, params.Quantile)
	if err == nil {
		imr.logger.Sugar().Infof("GetLatencies(%v) returned %d metrics", params.GroupByOperation, len(mf.Metrics))
	}
	return mf, err
}

// GetCallRates gets the call rate metrics for a given list of services grouped by service
// and optionally grouped by operation.
func (imr *influxdbMetricsReader) GetCallRates(ctx context.Context, params *metricsstore.CallRateQueryParameters) (*metrics.MetricFamily, error) {
	imr.logger.Info("GetCallRates called")
	mf, err := imr.getMetric(ctx, spanMetricQueryTypeCallRates, params.BaseQueryParameters, 0)
	if err == nil {
		imr.logger.Sugar().Infof("GetCallRates(%v) returned %d metrics", params.GroupByOperation, len(mf.Metrics))
	}
	return mf, err
}

// GetErrorRates gets the error rate metrics for a given list of services grouped by service
// and optionally grouped by operation.
func (imr *influxdbMetricsReader) GetErrorRates(ctx context.Context, params *metricsstore.ErrorRateQueryParameters) (*metrics.MetricFamily, error) {
	imr.logger.Info("GetErrorRates called")
	mf, err := imr.getMetric(ctx, spanMetricQueryTypeErrorRates, params.BaseQueryParameters, 0)
	if err == nil {
		imr.logger.Sugar().Infof("GetErrorRates(%v) returned %d metrics", params.GroupByOperation, len(mf.Metrics))
	}
	return mf, err
}

func (imr *influxdbMetricsReader) getMetric(ctx context.Context, queryType spanMetricQueryType, params metricsstore.BaseQueryParameters, quantile float64) (*metrics.MetricFamily, error) {
	mf := &metrics.MetricFamily{
		Type: metrics.MetricType_GAUGE,
	}
	switch queryType {
	case spanMetricQueryTypeLatencies:
		if params.GroupByOperation {
			mf.Name = "service_operation_latencies"
			mf.Help = fmt.Sprintf("%.2fth quantile latency, grouped by service", quantile)
		} else {
			mf.Name = "service_latencies"
			mf.Help = fmt.Sprintf("%.2fth quantile latency, grouped by service & operation", quantile)
		}
	case spanMetricQueryTypeCallRates:
		if params.GroupByOperation {
			mf.Name = "service_operation_call_rate"
			mf.Help = "calls/sec, grouped by service"
		} else {
			mf.Name = "service_call_rate"
			mf.Help = "calls/sec, grouped by service & operation"
		}
	case spanMetricQueryTypeErrorRates:
		if params.GroupByOperation {
			mf.Name = "service_operation_error_rate"
			mf.Help = "error rate, computed as a fraction of errors/sec over calls/sec, grouped by service"
		} else {
			mf.Name = "service_error_rate"
			mf.Help = "error rate, computed as a fraction of errors/sec over calls/sec, grouped by service & operation"
		}
	default:
		panic("unrecognized query type")
	}

	const (
		columnAliasService    = "service"
		columnAliasOperation  = "operation"
		columnAliasTimeBucket = "t"
		columnAliasValue      = "value"
	)

	query := querySpanMetrics(imr.tableSpans, queryType, params, quantile)
	err := imr.executeQuery(ctx, query, func(record map[string]interface{}) error {
		serviceName, ok := record[columnAliasService].(string)
		if !ok {
			return fmt.Errorf("unexpected type %T for %s", record[columnAliasService], columnAliasService)
		}
		operationName := ""
		if params.GroupByOperation {
			if operationName, ok = record[columnAliasOperation].(string); !ok {
				return fmt.Errorf("unexpected type %T for %s", record[columnAliasOperation], columnAliasOperation)
			}
		}
		value, ok := record[columnAliasValue].(float64)
		if !ok {
			return fmt.Errorf("unexpected type %T for %s", record[columnAliasValue], columnAliasValue)
		}
		timestamp, ok := record[columnAliasTimeBucket].(time.Time)
		if !ok {
			return fmt.Errorf("unexpected type %T for %s", record[columnAliasTimeBucket], columnAliasTimeBucket)
		}

		labels := buildLabels(serviceName, operationName)
		var metric *metrics.Metric
		if len(mf.Metrics) == 0 || !slices.EqualFunc(mf.Metrics[len(mf.Metrics)-1].Labels, labels, func(a, b *metrics.Label) bool { return proto.Equal(a, b) }) {
			metric = &metrics.Metric{
				Labels: labels,
			}
			mf.Metrics = append(mf.Metrics, metric)
		} else {
			metric = mf.Metrics[len(mf.Metrics)-1]
		}
		metric.MetricPoints = append(metric.MetricPoints, &metrics.MetricPoint{
			Value:     &metrics.MetricPoint_GaugeValue{GaugeValue: &metrics.GaugeValue{Value: &metrics.GaugeValue_DoubleValue{DoubleValue: value}}},
			Timestamp: &types.Timestamp{Seconds: timestamp.Unix(), Nanos: int32(timestamp.Nanosecond())},
		})
		return nil
	})
	if err != nil && !isTableNotFound(err) { // ignore table not found (schema-on-write)
		return nil, err
	}

	if len(mf.Metrics) == 0 {
		for _, serviceName := range params.ServiceNames {
			mf.Metrics = append(mf.Metrics, &metrics.Metric{
				Labels: []*metrics.Label{
					{Name: semconv.AttributeServiceName, Value: serviceName},
				},
				MetricPoints: []*metrics.MetricPoint{
					{
						Value:     &metrics.MetricPoint_GaugeValue{GaugeValue: &metrics.GaugeValue{Value: &metrics.GaugeValue_DoubleValue{DoubleValue: 0}}},
						Timestamp: &types.Timestamp{Seconds: params.EndTime.Unix(), Nanos: int32(params.EndTime.Nanosecond())},
					},
				},
			})
		}
	}

	return mf, nil
}

func buildLabels(serviceName, operationName string) []*metrics.Label {
	if operationName == "" {
		return []*metrics.Label{
			{Name: semconv.AttributeServiceName, Value: serviceName},
		}
	}
	return []*metrics.Label{
		{Name: semconv.AttributeServiceName, Value: serviceName},
		{Name: "operation", Value: operationName},
	}
}

// GetMinStepDuration gets the min time resolution supported by the backing metrics store,
// e.g. 10s means the backend can only return data points that are at least 10s apart, not closer.
func (imr *influxdbMetricsReader) GetMinStepDuration(_ context.Context, _ *metricsstore.MinStepDurationQueryParameters) (time.Duration, error) {
	return minStep, nil
}
