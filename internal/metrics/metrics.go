package metrics

import (
	"github.com/lbryio/hub/meta"
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
	SessionCount = promauto.NewGauge(prometheus.GaugeOpts{
		Name: "session_count",
		Help: "Number of client sessions",
		ConstLabels: map[string]string{
			"version": meta.Version,
		},
	})
	// These are unique to the go code
	PingsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "pings",
		Help: "Number of pings",
	})
	ZeroChannelsCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "zero_channels_counter",
		Help: "Number of times zero channels were returned in getUniqueChannels",
	})
	NoRepostedCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "no_reposted_counter",
		Help: "Number of times zero reposted were returned in getClaimsForRepost",
	})
	GetUniqueChannelsErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "get_unique_channels_error_counter",
		Help: "Number of errors",
	})
	JsonErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "json_error_counter",
		Help: "JSON parsing errors",
	})
	MgetErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "mget_error_counter",
		Help: "Mget errors",
	})
	SearchErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "search_error_counter",
		Help: "Number of errors",
	})
	FatalErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fatal_error_counter",
		Help: "Number of errors",
	})
	QueryTime = promauto.NewHistogramVec(prometheus.HistogramOpts{
		Name: "query_time",
		Help: "Histogram of query times",
		Buckets: HistogramBuckets,
	}, []string{"method"})
)

