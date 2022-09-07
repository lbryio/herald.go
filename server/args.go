package server

import (
	"log"
	"os"
	"strings"

	"github.com/akamensky/argparse"
	pb "github.com/lbryio/herald.go/protobuf/go"
)

const (
	ServeCmd  = iota
	SearchCmd = iota
	DBCmd     = iota
)

// Args struct contains the arguments to the hub server.
type Args struct {
	CmdType                     int
	Host                        string
	Port                        string
	DBPath                      string
	EsHost                      string
	EsPort                      string
	PrometheusPort              string
	NotifierPort                string
	JSONRPCPort                 string
	EsIndex                     string
	RefreshDelta                int
	CacheTTL                    int
	PeerFile                    string
	Country                     string
	BlockingChannelIds          []string
	FilteringChannelIds         []string
	Debug                       bool
	DisableEs                   bool
	DisableLoadPeers            bool
	DisableStartPrometheus      bool
	DisableStartUDP             bool
	DisableWritePeers           bool
	DisableFederation           bool
	DisableRocksDBRefresh       bool
	DisableResolve              bool
	DisableBlockingAndFiltering bool
	DisableStartNotifier        bool
	DisableStartJSONRPC         bool
}

const (
	DefaultHost                        = "0.0.0.0"
	DefaultPort                        = "50051"
	DefaultDBPath                      = "/mnt/d/data/snapshot_1072108/lbry-rocksdb/" // FIXME
	DefaultEsHost                      = "http://localhost"
	DefaultEsIndex                     = "claims"
	DefaultEsPort                      = "9200"
	DefaultPrometheusPort              = "2112"
	DefaultNotifierPort                = "18080"
	DefaultJSONRPCPort                 = "50001"
	DefaultRefreshDelta                = 5
	DefaultCacheTTL                    = 5
	DefaultPeerFile                    = "peers.txt"
	DefaultCountry                     = "US"
	DefaultDisableLoadPeers            = false
	DefaultDisableStartPrometheus      = false
	DefaultDisableStartUDP             = false
	DefaultDisableWritePeers           = false
	DefaultDisableFederation           = false
	DefaultDisableRockDBRefresh        = false
	DefaultDisableResolve              = false
	DefaultDisableBlockingAndFiltering = false
	DisableStartNotifier               = false
	DisableStartJSONRPC                = false
)

var (
	DefaultBlockingChannelIds  = []string{}
	DefaultFilteringChannelIds = []string{}
)

// GetEnvironment takes the environment variables as an array of strings
// and a getkeyval function to turn it into a map.
func GetEnvironment(data []string, getkeyval func(item string) (key, val string)) map[string]string {
	items := make(map[string]string)
	for _, item := range data {
		key, val := getkeyval(item)
		items[key] = val
	}
	return items
}

// GetEnvironmentStandard gets the environment variables as a map.
func GetEnvironmentStandard() map[string]string {
	return GetEnvironment(os.Environ(), func(item string) (key, val string) {
		splits := strings.Split(item, "=")
		key = splits[0]
		val = splits[1]
		return
	})
}

// ParseArgs parses the command line arguments when started the hub server.
func ParseArgs(searchRequest *pb.SearchRequest) *Args {

	environment := GetEnvironmentStandard()
	parser := argparse.NewParser("hub", "hub server and client")

	serveCmd := parser.NewCommand("serve", "start the hub server")
	searchCmd := parser.NewCommand("search", "claim search")
	dbCmd := parser.NewCommand("db", "db testing")

	host := parser.String("", "rpchost", &argparse.Options{Required: false, Help: "RPC host", Default: DefaultHost})
	port := parser.String("", "rpcport", &argparse.Options{Required: false, Help: "RPC port", Default: DefaultPort})
	dbPath := parser.String("", "db-path", &argparse.Options{Required: false, Help: "RocksDB path", Default: DefaultDBPath})
	esHost := parser.String("", "eshost", &argparse.Options{Required: false, Help: "elasticsearch host", Default: DefaultEsHost})
	esPort := parser.String("", "esport", &argparse.Options{Required: false, Help: "elasticsearch port", Default: DefaultEsPort})
	prometheusPort := parser.String("", "prometheus-port", &argparse.Options{Required: false, Help: "prometheus port", Default: DefaultPrometheusPort})
	notifierPort := parser.String("", "notifier-port", &argparse.Options{Required: false, Help: "notifier port", Default: DefaultNotifierPort})
	jsonRPCPort := parser.String("", "json-rpc-port", &argparse.Options{Required: false, Help: "JSON RPC port", Default: DefaultJSONRPCPort})
	esIndex := parser.String("", "esindex", &argparse.Options{Required: false, Help: "elasticsearch index name", Default: DefaultEsIndex})
	refreshDelta := parser.Int("", "refresh-delta", &argparse.Options{Required: false, Help: "elasticsearch index refresh delta in seconds", Default: DefaultRefreshDelta})
	cacheTTL := parser.Int("", "cachettl", &argparse.Options{Required: false, Help: "Cache TTL in minutes", Default: DefaultCacheTTL})
	peerFile := parser.String("", "peerfile", &argparse.Options{Required: false, Help: "Initial peer file for federation", Default: DefaultPeerFile})
	country := parser.String("", "country", &argparse.Options{Required: false, Help: "Country this node is running in. Default US.", Default: DefaultCountry})
	blockingChannelIds := parser.StringList("", "blocking-channel-ids", &argparse.Options{Required: false, Help: "Blocking channel ids", Default: DefaultBlockingChannelIds})
	filteringChannelIds := parser.StringList("", "filtering-channel-ids", &argparse.Options{Required: false, Help: "Filtering channel ids", Default: DefaultFilteringChannelIds})

	debug := parser.Flag("", "debug", &argparse.Options{Required: false, Help: "enable debug logging", Default: false})
	disableEs := parser.Flag("", "disable-es", &argparse.Options{Required: false, Help: "Disable elastic search, for running/testing independently", Default: false})
	disableLoadPeers := parser.Flag("", "disable-load-peers", &argparse.Options{Required: false, Help: "Disable load peers from disk at startup", Default: DefaultDisableLoadPeers})
	disableStartPrometheus := parser.Flag("", "disable-start-prometheus", &argparse.Options{Required: false, Help: "Disable start prometheus server", Default: DefaultDisableStartPrometheus})
	disableStartUdp := parser.Flag("", "disable-start-udp", &argparse.Options{Required: false, Help: "Disable start UDP ping server", Default: DefaultDisableStartUDP})
	disableWritePeers := parser.Flag("", "disable-write-peers", &argparse.Options{Required: false, Help: "Disable write peer to disk as we learn about them", Default: DefaultDisableWritePeers})
	disableFederation := parser.Flag("", "disable-federation", &argparse.Options{Required: false, Help: "Disable server federation", Default: DefaultDisableFederation})
	disableRocksDBRefresh := parser.Flag("", "disable-rocksdb-refresh", &argparse.Options{Required: false, Help: "Disable rocksdb refreshing", Default: DefaultDisableRockDBRefresh})
	disableResolve := parser.Flag("", "disable-resolve", &argparse.Options{Required: false, Help: "Disable resolve endpoint (and rocksdb loading)", Default: DefaultDisableRockDBRefresh})
	disableBlockingAndFiltering := parser.Flag("", "disable-blocking-and-filtering", &argparse.Options{Required: false, Help: "Disable blocking and filtering of channels and streams", Default: DefaultDisableBlockingAndFiltering})
	disableStartNotifier := parser.Flag("", "disable-start-notifier", &argparse.Options{Required: false, Help: "Disable start notifier", Default: DisableStartNotifier})
	disableStartJSONRPC := parser.Flag("", "disable-start-jsonrpc", &argparse.Options{Required: false, Help: "Disable start jsonrpc endpoint", Default: DisableStartJSONRPC})

	text := parser.String("", "text", &argparse.Options{Required: false, Help: "text query"})
	name := parser.String("", "name", &argparse.Options{Required: false, Help: "name"})
	claimType := parser.String("", "claim_type", &argparse.Options{Required: false, Help: "claim_type"})
	id := parser.String("", "id", &argparse.Options{Required: false, Help: "id"})
	author := parser.String("", "author", &argparse.Options{Required: false, Help: "author"})
	title := parser.String("", "title", &argparse.Options{Required: false, Help: "title"})
	description := parser.String("", "description", &argparse.Options{Required: false, Help: "description"})
	channelId := parser.String("", "channel_id", &argparse.Options{Required: false, Help: "channel id"})
	channelIds := parser.StringList("", "channel_ids", &argparse.Options{Required: false, Help: "channel ids"})

	// Now parse the arguments
	err := parser.Parse(os.Args)
	if err != nil {
		log.Fatalln(parser.Usage(err))
	}

	args := &Args{
		CmdType:                     SearchCmd,
		Host:                        *host,
		Port:                        *port,
		DBPath:                      *dbPath,
		EsHost:                      *esHost,
		EsPort:                      *esPort,
		PrometheusPort:              *prometheusPort,
		NotifierPort:                *notifierPort,
		JSONRPCPort:                 *jsonRPCPort,
		EsIndex:                     *esIndex,
		RefreshDelta:                *refreshDelta,
		CacheTTL:                    *cacheTTL,
		PeerFile:                    *peerFile,
		Country:                     *country,
		BlockingChannelIds:          *blockingChannelIds,
		FilteringChannelIds:         *filteringChannelIds,
		Debug:                       *debug,
		DisableEs:                   *disableEs,
		DisableLoadPeers:            *disableLoadPeers,
		DisableStartPrometheus:      *disableStartPrometheus,
		DisableStartUDP:             *disableStartUdp,
		DisableWritePeers:           *disableWritePeers,
		DisableFederation:           *disableFederation,
		DisableRocksDBRefresh:       *disableRocksDBRefresh,
		DisableResolve:              *disableResolve,
		DisableBlockingAndFiltering: *disableBlockingAndFiltering,
		DisableStartNotifier:        *disableStartNotifier,
		DisableStartJSONRPC:         *disableStartJSONRPC,
	}

	if esHost, ok := environment["ELASTIC_HOST"]; ok {
		args.EsHost = esHost
	}

	if !strings.HasPrefix(args.EsHost, "http") {
		args.EsHost = "http://" + args.EsHost
	}

	if esPort, ok := environment["ELASTIC_PORT"]; ok {
		args.EsPort = esPort
	}

	if prometheusPort, ok := environment["GOHUB_PROMETHEUS_PORT"]; ok {
		args.PrometheusPort = prometheusPort
	}

	/*
	   Verify no invalid argument combinations
	*/
	if len(*channelIds) > 0 && *channelId != "" {
		log.Fatal("Cannot specify both channel_id and channel_ids")
	}

	if serveCmd.Happened() {
		args.CmdType = ServeCmd
	} else if searchCmd.Happened() {
		args.CmdType = SearchCmd
	} else if dbCmd.Happened() {
		args.CmdType = DBCmd
	}

	if *text != "" {
		searchRequest.Text = *text
	}
	if *name != "" {
		searchRequest.ClaimName = *name
	}
	if *claimType != "" {
		searchRequest.ClaimType = []string{*claimType}
	}
	if *id != "" {
		searchRequest.ClaimId = &pb.InvertibleField{Invert: false, Value: []string{*id}}
	}
	if *author != "" {
		searchRequest.Author = *author
	}
	if *title != "" {
		searchRequest.Title = *title
	}
	if *description != "" {
		searchRequest.Description = *description
	}
	if *channelId != "" {
		searchRequest.ChannelId = &pb.InvertibleField{Invert: false, Value: []string{*channelId}}
	}
	if len(*channelIds) > 0 {
		searchRequest.ChannelId = &pb.InvertibleField{Invert: false, Value: *channelIds}
	}

	return args
}
