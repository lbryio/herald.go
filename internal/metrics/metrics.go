package metrics

import (
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"time"
)

var (
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
	SearchCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "searche_counter",
		Help: "Total number of searches",
	})
	ClientCreationErrorCounter =  promauto.NewCounter(prometheus.CounterOpts{
		Name: "client_creation_error_counter",
		Help: "Number of errors",
	})
	SearchErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "search_error_counter",
		Help: "Number of errors",
	})
	FatalErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "fatal_error_counter",
		Help: "Number of errors",
	})
	ErrorCounter = promauto.NewCounter(prometheus.CounterOpts{
		Name: "error_counter",
		Help: "Number of errors",
	})
	QueryTime = promauto.NewSummary(prometheus.SummaryOpts{
		MaxAge: time.Hour,
		Name: "query_time",
		Help: "hourly summary of query time",
	})
)

