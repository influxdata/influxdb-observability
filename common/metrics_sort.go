package common

import (
	"fmt"
	"go.opentelemetry.io/collector/pdata/pmetric"
	"sort"
)

func SortResourceMetrics(rm pmetric.ResourceMetricsSlice) {
	for i := 0; i < rm.Len(); i++ {
		r := rm.At(i)
		for j := 0; j < r.ScopeMetrics().Len(); j++ {
			il := r.ScopeMetrics().At(j)
			for k := 0; k < il.Metrics().Len(); k++ {
				m := il.Metrics().At(k)
				switch m.DataType() {
				case pmetric.MetricDataTypeGauge:
					for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
						m.Gauge().DataPoints().At(l).Attributes().Sort()
					}
				case pmetric.MetricDataTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						m.Sum().DataPoints().At(l).Attributes().Sort()
					}
				case pmetric.MetricDataTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						sortBuckets(m.Histogram().DataPoints().At(l))
						m.Histogram().DataPoints().At(l).Attributes().Sort()
					}
				case pmetric.MetricDataTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						m.Summary().DataPoints().At(l).Attributes().Sort()
						m.Summary().DataPoints().At(l).QuantileValues().Sort(func(a, b pmetric.ValueAtQuantile) bool {
							return a.Quantile() < b.Quantile()
						})
					}
				default:
					panic(fmt.Sprintf("unsupported metric data type %d", m.DataType()))
				}
			}
			il.Metrics().Sort(func(a, b pmetric.Metric) bool {
				return a.Name() < b.Name()
			})
		}
		r.ScopeMetrics().Sort(func(a, b pmetric.ScopeMetrics) bool {
			if a.Scope().Name() == b.Scope().Name() {
				return a.Scope().Version() < b.Scope().Version()
			}
			return a.Scope().Name() < b.Scope().Name()
		})
		r.Resource().Attributes().Sort()
	}
}

func sortBuckets(hdp pmetric.HistogramDataPoint) {
	buckets := make(sortableBuckets, len(hdp.MExplicitBounds()))
	for i := range hdp.MExplicitBounds() {
		buckets[i] = sortableBucket{hdp.MBucketCounts()[i], hdp.MExplicitBounds()[i]}
	}
	sort.Sort(buckets)
	for i, bucket := range buckets {
		hdp.MBucketCounts()[i], hdp.MExplicitBounds()[i] = bucket.count, bucket.bound
	}
}

type sortableBucket struct {
	count uint64
	bound float64
}

type sortableBuckets []sortableBucket

func (s sortableBuckets) Len() int {
	return len(s)
}

func (s sortableBuckets) Less(i, j int) bool {
	return s[i].bound < s[j].bound
}

func (s sortableBuckets) Swap(i, j int) {
	s[i].count, s[j].count = s[j].count, s[i].count
	s[i].bound, s[j].bound = s[j].bound, s[i].bound
}
