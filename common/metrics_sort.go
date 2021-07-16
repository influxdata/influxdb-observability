package common

import (
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
						// TODO sort QuantileValues by Quantile
						// sort.Slice(d.QuantileValues, func(i, j int) bool {
						// 	return d.QuantileValues[i].Quantile < d.QuantileValues[j].Quantile
						// })
					}
				}
			}
			// TODO sort metrics by name
			// sort.Slice(il.Metrics, func(i, j int) bool {
			// 	return il.Metrics[i].Name < il.Metrics[j].Name
			// })
		}
		// TODO sort ILMs by name,version
		// sort.Slice(r.InstrumentationLibraryMetrics, func(i, j int) bool {
		// 	if r.InstrumentationLibraryMetrics[i].InstrumentationLibrary.Name == r.InstrumentationLibraryMetrics[j].InstrumentationLibrary.Name {
		// 		return r.InstrumentationLibraryMetrics[i].InstrumentationLibrary.Version < r.InstrumentationLibraryMetrics[j].InstrumentationLibrary.Version
		// 	}
		// 	return r.InstrumentationLibraryMetrics[i].InstrumentationLibrary.Name < r.InstrumentationLibraryMetrics[j].InstrumentationLibrary.Name
		// })
		r.Resource().Attributes().Sort()
	}
	// TODO sort resource attributes by attribute key
	// sort.Slice(rm, func(i, j int) bool {
	// 	return ResourceAttributesToKey(rm[i].Resource.Attributes) < ResourceAttributesToKey(rm[j].Resource.Attributes)
	// })
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
