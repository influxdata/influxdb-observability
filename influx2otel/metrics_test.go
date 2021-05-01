package influx2otel_test

import (
	"sort"
	"strings"

	otlpcommon "github.com/influxdata/influxdb-observability/otlp/common/v1"
	otlpmetrics "github.com/influxdata/influxdb-observability/otlp/metrics/v1"
)

func sortResourceMetrics(rm []*otlpmetrics.ResourceMetrics) {
	for _, r := range rm {
		for _, il := range r.InstrumentationLibraryMetrics {
			for _, m := range il.Metrics {
				switch data := m.Data.(type) {
				case *otlpmetrics.Metric_DoubleGauge:
					for _, d := range data.DoubleGauge.DataPoints {
						sort.Slice(d.Labels, func(i, j int) bool {
							return d.Labels[i].Key < d.Labels[j].Key
						})
					}
				case *otlpmetrics.Metric_DoubleSum:
					for _, d := range data.DoubleSum.DataPoints {
						sort.Slice(d.Labels, func(i, j int) bool {
							return d.Labels[i].Key < d.Labels[j].Key
						})
					}
				case *otlpmetrics.Metric_DoubleHistogram:
					for _, d := range data.DoubleHistogram.DataPoints {
						sortBuckets(d.BucketCounts, d.ExplicitBounds)
						sort.Slice(d.Labels, func(i, j int) bool {
							return d.Labels[i].Key < d.Labels[j].Key
						})
					}
				case *otlpmetrics.Metric_DoubleSummary:
					for _, d := range data.DoubleSummary.DataPoints {
						sort.Slice(d.Labels, func(i, j int) bool {
							return d.Labels[i].Key < d.Labels[j].Key
						})
						sort.Slice(d.QuantileValues, func(i, j int) bool {
							return d.QuantileValues[i].Quantile < d.QuantileValues[j].Quantile
						})
					}
				}
			}
			sort.Slice(il.Metrics, func(i, j int) bool {
				return il.Metrics[i].Name < il.Metrics[j].Name
			})
		}
		sort.Slice(r.InstrumentationLibraryMetrics, func(i, j int) bool {
			if r.InstrumentationLibraryMetrics[i].InstrumentationLibrary.Name == r.InstrumentationLibraryMetrics[j].InstrumentationLibrary.Name {
				return r.InstrumentationLibraryMetrics[i].InstrumentationLibrary.Version < r.InstrumentationLibraryMetrics[j].InstrumentationLibrary.Version
			}
			return r.InstrumentationLibraryMetrics[i].InstrumentationLibrary.Name < r.InstrumentationLibraryMetrics[j].InstrumentationLibrary.Name
		})
		sort.Slice(r.Resource.Attributes, func(i, j int) bool {
			return r.Resource.Attributes[i].Key < r.Resource.Attributes[j].Key
		})
	}
	sort.Slice(rm, func(i, j int) bool {
		return resourceAttributesToKey(rm[i].Resource.Attributes) < resourceAttributesToKey(rm[j].Resource.Attributes)
	})
}

func sortBuckets(bucketCounts []uint64, explicitBounds []float64) {
	buckets := make(sortableBuckets, len(explicitBounds))
	for i := range explicitBounds {
		buckets[i] = sortableBucket{bucketCounts[i], explicitBounds[i]}
	}
	sort.Sort(buckets)
	for i, bucket := range buckets {
		bucketCounts[i], explicitBounds[i] = bucket.count, bucket.bound
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

func resourceAttributesToKey(rAttributes []*otlpcommon.KeyValue) string {
	var key strings.Builder
	for _, kv := range rAttributes {
		key.WriteString(kv.Key)
		key.WriteByte(':')
	}
	return key.String()
}
