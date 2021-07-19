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
						m.Gauge().DataPoints().At(l).LabelsMap().Sort()
					}
				case pdata.MetricDataTypeSum:
					for l := 0; l < m.Sum().DataPoints().Len(); l++ {
						m.Sum().DataPoints().At(l).LabelsMap().Sort()
					}
				case pdata.MetricDataTypeHistogram:
					for l := 0; l < m.Histogram().DataPoints().Len(); l++ {
						sortBuckets(m.Histogram().DataPoints().At(l))
						m.Histogram().DataPoints().At(l).LabelsMap().Sort()
					}
				case pdata.MetricDataTypeSummary:
					for l := 0; l < m.Summary().DataPoints().Len(); l++ {
						m.Summary().DataPoints().At(l).LabelsMap().Sort()
						// TODO: Uncomment after https://github.com/open-telemetry/opentelemetry-collector/pull/3671
						// m.Summary().DataPoints().At(l).QuantileValues().Sort(func(i, j int) bool {
						// 	left := m.Summary().DataPoints().At(l).QuantileValues().At(i)
						// 	right := m.Summary().DataPoints().At(l).QuantileValues().At(j)
						// 	return left.Quantile() < right.Quantile()
						// })
					}
				default:
					panic(fmt.Sprintf("unsupported metric data type %d", m.DataType()))
				}
			}
			// TODO: Uncomment after https://github.com/open-telemetry/opentelemetry-collector/pull/3671
			// il.Metrics().Sort(func(i, j int) bool {
			// 	return il.Metrics().At(i).Name() < il.Metrics().At(j).Name()
			// })
		}
		// TODO: Uncomment after https://github.com/open-telemetry/opentelemetry-collector/pull/3671
		// r.InstrumentationLibraryMetrics().Sort(func(i, j int) bool {
		// 	left := r.InstrumentationLibraryMetrics().At(i).InstrumentationLibrary()
		// 	right := r.InstrumentationLibraryMetrics().At(j).InstrumentationLibrary()
		// 	if left.Name() == right.Name() {
		// 		return left.Version() < right.Version()
		// 	}
		// 	return left.Name() < right.Name()
		// })
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
