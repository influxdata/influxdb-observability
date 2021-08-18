package common

import (
	"fmt"
	"sort"

	"go.opentelemetry.io/collector/model/pdata"
)

func SortResourceMetrics(rm pdata.ResourceMetricsSlice) {
	for i := 0; i < rm.Len(); i++ {
		r := rm.At(i)
		for j := 0; j < r.InstrumentationLibraryMetrics().Len(); j++ {
			il := r.InstrumentationLibraryMetrics().At(j)
			for k := 0; k < il.Metrics().Len(); k++ {
				m := il.Metrics().At(k)
				switch m.DataType() {
				case pdata.MetricDataTypeGauge:
					for l := 0; l < m.Gauge().DataPoints().Len(); l++ {
						m.Gauge().DataPoints().At(l).Attributes().Sort()
					}
				case pdata.MetricDataTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						m.Sum().DataPoints().At(l).Attributes().Sort()
					}
				case pdata.MetricDataTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						sortBuckets(m.Histogram().DataPoints().At(l))
						m.Histogram().DataPoints().At(l).Attributes().Sort()
					}
				case pdata.MetricDataTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						m.Summary().DataPoints().At(l).Attributes().Sort()
						m.Summary().DataPoints().At(l).QuantileValues().Sort(func(a, b pdata.ValueAtQuantile) bool {
							return a.Quantile() < b.Quantile()
						})
					}
				default:
					panic(fmt.Sprintf("unsupported metric data type %d", m.DataType()))
				}
			}
			il.Metrics().Sort(func(a, b pdata.Metric) bool {
				return a.Name() < b.Name()
			})
		}
		r.InstrumentationLibraryMetrics().Sort(func(a, b pdata.InstrumentationLibraryMetrics) bool {
			if a.InstrumentationLibrary().Name() == b.InstrumentationLibrary().Name() {
				return a.InstrumentationLibrary().Version() < b.InstrumentationLibrary().Version()
			}
			return a.InstrumentationLibrary().Name() < b.InstrumentationLibrary().Name()
		})
		r.Resource().Attributes().Sort()
	}
}

func sortBuckets(hdp pdata.HistogramDataPoint) {
	buckets := make(sortableBuckets, len(hdp.ExplicitBounds()))
	for i := range hdp.ExplicitBounds() {
		buckets[i] = sortableBucket{hdp.BucketCounts()[i], hdp.ExplicitBounds()[i]}
	}
	sort.Sort(buckets)
	for i, bucket := range buckets {
		hdp.BucketCounts()[i], hdp.ExplicitBounds()[i] = bucket.count, bucket.bound
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
