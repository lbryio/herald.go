package server

import (
	"log"
	"os"
	"strings"

	"github.com/akamensky/argparse"
	pb "github.com/lbryio/hub/protobuf/go"
)

const (
	ServeCmd  = iota
	SearchCmd = iota
)

// Args struct contains the arguments to the hub server.
type Args struct {
	CmdType           int
	Host              string
	Port              string
	EsHost            string
	EsPort            string
	PrometheusPort    string
	EsIndex           string
	RefreshDelta      int
	CacheTTL          int
	PeerFile          string
	Country           string
	DisableEs         bool
	Debug             bool
	LoadPeers         bool
	StartPrometheus   bool
	StartUDP          bool
	WritePeers        bool
	DisableFederation bool
}

const (
	DefaultHost              = "0.0.0.0"
	DefaultPort              = "50051"
	DefaultEsHost            = "http://localhost"
	DefaultEsIndex           = "claims"
	DefaultEsPort            = "9200"
	DefaultPrometheusPort    = "2112"
	DefaultRefreshDelta      = 5
	DefaultCacheTTL          = 5
	DefaultPeerFile          = "peers.txt"
	DefaultCountry           = "US"
	DefaultLoadPeers         = true
	DefaultStartPrometheus   = true
	DefaultStartUDP          = true
	DefaultWritePeers        = true
	DefaultDisableFederation = false
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

	host := parser.String("", "rpchost", &argparse.Options{Required: false, Help: "RPC host", Default: DefaultHost})
	port := parser.String("", "rpcport", &argparse.Options{Required: false, Help: "RPC port", Default: DefaultPort})
	esHost := parser.String("", "eshost", &argparse.Options{Required: false, Help: "elasticsearch host", Default: DefaultEsHost})
	esPort := parser.String("", "esport", &argparse.Options{Required: false, Help: "elasticsearch port", Default: DefaultEsPort})
	prometheusPort := parser.String("", "prometheus-port", &argparse.Options{Required: false, Help: "prometheus port", Default: DefaultPrometheusPort})
	esIndex := parser.String("", "esindex", &argparse.Options{Required: false, Help: "elasticsearch index name", Default: DefaultEsIndex})
	refreshDelta := parser.Int("", "refresh-delta", &argparse.Options{Required: false, Help: "elasticsearch index refresh delta in seconds", Default: DefaultRefreshDelta})
	cacheTTL := parser.Int("", "cachettl", &argparse.Options{Required: false, Help: "Cache TTL in minutes", Default: DefaultCacheTTL})
	peerFile := parser.String("", "peerfile", &argparse.Options{Required: false, Help: "Initial peer file for federation", Default: DefaultPeerFile})
	country := parser.String("", "country", &argparse.Options{Required: false, Help: "Country this node is running in. Default US.", Default: DefaultCountry})

	debug := parser.Flag("", "debug", &argparse.Options{Required: false, Help: "enable debug logging", Default: false})
	disableEs := parser.Flag("", "disable-es", &argparse.Options{Required: false, Help: "Disable elastic search, for running/testing independently", Default: false})
	loadPeers := parser.Flag("", "load-peers", &argparse.Options{Required: false, Help: "load peers from disk at startup", Default: DefaultLoadPeers})
	startPrometheus := parser.Flag("", "start-prometheus", &argparse.Options{Required: false, Help: "Start prometheus server", Default: DefaultStartPrometheus})
	startUdp := parser.Flag("", "start-udp", &argparse.Options{Required: false, Help: "Start UDP ping server", Default: DefaultStartUDP})
	writePeers := parser.Flag("", "write-peers", &argparse.Options{Required: false, Help: "Write peer to disk as we learn about them", Default: DefaultWritePeers})
	disableFederation := parser.Flag("", "disable-federation", &argparse.Options{Required: false, Help: "Disable server federation", Default: DefaultDisableFederation})

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
		CmdType:           SearchCmd,
		Host:              *host,
		Port:              *port,
		EsHost:            *esHost,
		EsPort:            *esPort,
		PrometheusPort:    *prometheusPort,
		EsIndex:           *esIndex,
		RefreshDelta:      *refreshDelta,
		CacheTTL:          *cacheTTL,
		PeerFile:          *peerFile,
		Country:           *country,
		DisableEs:         *disableEs,
		Debug:             *debug,
		LoadPeers:         *loadPeers,
		StartPrometheus:   *startPrometheus,
		StartUDP:          *startUdp,
		WritePeers:        *writePeers,
		DisableFederation: *disableFederation,
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
