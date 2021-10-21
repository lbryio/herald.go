package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
)

var (
	HistogramBuckets = []float64{0.005, 0.025, 0.05, 0.1, 0.25, 0.4, 1, 2, 5, 10, 20, 60, 120, 300}
	// These mirror counters from the python code
	RequestsCount = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "requests_count",
		Help: "Total number of searches",
	}, []string{"method"})
	// These are unique to the go code
	ErrorsCounter = promauto.NewCounterVec(prometheus.CounterOpts{
		Name: "errors",
		Help: "Number of errors by type",
	}, []string{"error_type"})
	QueryTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "query_time",
		Help: "Histogram of query times",
		Buckets: HistogramBuckets,
	}, []string{"method"})
)

