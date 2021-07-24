package server

import (
	"context"
	"fmt"
	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/olivere/elastic/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promauto"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/protobuf/types/known/wrapperspb"
	"log"
	"net/http"
	"regexp"
	"time"
)

type Server struct {
	GrpcServer   *grpc.Server
	Args 	     *Args
	MultiSpaceRe *regexp.Regexp
	WeirdCharsRe *regexp.Regexp
	EsClient     *elastic.Client
	Servers      []*FederatedServer
	pb.UnimplementedHubServer
}

type FederatedServer struct {
	Address string
	Port string
	Ts time.Time
	Ping int //?
}

const majorVersion = 0

const (
	ServeCmd = iota
	SearchCmd = iota
	GetblockCmd = iota
	GetblockHeaderCmd = iota
	SubscribeHeaderCmd = iota
)

var (
	myCounters = map[string]prometheus.Metric{
		"pings": promauto.NewCounter(prometheus.CounterOpts{
			Name: "pings",
			Help: "Number of pings",
		}),
		"header_subs": promauto.NewCounter(prometheus.CounterOpts{
			Name: "header_subs",
			Help: "Number of header subs",
		}),
		"zero_channels_counter": promauto.NewCounter(prometheus.CounterOpts{
			Name: "zero_channels_counter",
			Help: "Number of times zero channels were returned in getUniqueChanne;s",
		}),
		"no_reposted_counter": promauto.NewCounter(prometheus.CounterOpts{
			Name: "no_reposted_counter",
			Help: "Number of times zero reposted were returned in getClaimsForRepost",
		}),
		"get_unique_channels_errors": promauto.NewCounter(prometheus.CounterOpts{
			Name: "get_unique_channels_errors",
			Help: "Number of errors",
		}),
		"json_errors": promauto.NewCounter(prometheus.CounterOpts{
			Name: "json_errors",
			Help: "JSON parsing errors",
		}),
		"mget_errors": promauto.NewCounter(prometheus.CounterOpts{
			Name: "mget_errors",
			Help: "Mget errors",
		}),
		"searches": promauto.NewCounter(prometheus.CounterOpts{
			Name: "searches",
			Help: "Total number of searches",
		}),
		"client_creation_errors": promauto.NewCounter(prometheus.CounterOpts{
			Name: "client_creation_errors",
			Help: "Number of errors",
		}),
		"search_errors": promauto.NewCounter(prometheus.CounterOpts{
			Name: "search_errors",
			Help: "Number of errors",
		}),
		"fatal_errors": promauto.NewCounter(prometheus.CounterOpts{
			Name: "fatal_errors",
			Help: "Number of errors",
		}),
		"errors": promauto.NewCounter(prometheus.CounterOpts{
			Name: "errors",
			Help: "Number of errors",
		}),
		"query_time": promauto.NewSummary(prometheus.SummaryOpts{
			MaxAge: time.Hour,
			Name: "query_time",
			Help: "hourly summary of query time",
		}),
	}
)



type Args struct {
	// TODO Make command types an enum
	CmdType int
	Host string
	Port string
	EsHost string
	EsPort string
	Dev bool
}

func getVersion(alphaBeta string) string {
	strPortion := time.Now().Format("2006.01.02")
	majorVersionDate := fmt.Sprintf("v%d.%s", majorVersion, strPortion)
	if len(alphaBeta) > 0 {
		return fmt.Sprintf("%s-%s", majorVersionDate, alphaBeta)
	}
	return majorVersionDate
}

/*
	'blockchain.block.get_chunk'
	'blockchain.block.get_header'
	'blockchain.estimatefee'
	'blockchain.relayfee'
	'blockchain.scripthash.get_balance'
	'blockchain.scripthash.get_history'
	'blockchain.scripthash.get_mempool'
	'blockchain.scripthash.listunspent'
	'blockchain.scripthash.subscribe'
	'blockchain.transaction.broadcast'
	'blockchain.transaction.get'
	'blockchain.transaction.get_batch'
	'blockchain.transaction.info'
	'blockchain.transaction.get_merkle'
	'server.add_peer'
	'server.banner'
	'server.payment_address'
	'server.donation_address'
	'server.features'
	'server.peers.subscribe'
	'server.version'
	'blockchain.transaction.get_height'
	'blockchain.claimtrie.search'
	'blockchain.claimtrie.resolve'
	'blockchain.claimtrie.getclaimsbyids'
	'blockchain.block.get_server_height'
	'mempool.get_fee_histogram'
	'blockchain.block.headers'
	'server.ping'
	'blockchain.headers.subscribe'
	'blockchain.address.get_balance'
	'blockchain.address.get_history'
	'blockchain.address.get_mempool'
	'blockchain.address.listunspent'
	'blockchain.address.subscribe'
	'blockchain.address.unsubscribe'
*/

func MakeHubServer(args *Args) *Server {
	grpcServer := grpc.NewServer(grpc.NumStreamWorkers(10))

	multiSpaceRe, err := regexp.Compile("\\s{2,}")
	if err != nil {
		log.Fatal(err)
	}

	weirdCharsRe, err := regexp.Compile("[#!~]")
	if err != nil {
		log.Fatal(err)
	}
	self := &FederatedServer{
		Address: "127.0.0.1",
		Port: args.Port,
		Ts: time.Now(),
		Ping: 0,
	}
	servers := make([]*FederatedServer, 10)
	servers = append(servers, self)
	s := &Server {
		GrpcServer: grpcServer,
		Args: args,
		MultiSpaceRe: multiSpaceRe,
		WeirdCharsRe: weirdCharsRe,
		Servers: servers,
	}

	return s
}

func (s *Server) RecordMetrics(typ string, data interface{}) {
	return
	/*
	metric := myCounters[typ]
	if typ != "query_time" {
		counter := *(*prometheus.Counter)(unsafe.Pointer(&metric))
		counter.Inc()
	}
	summary := *(*prometheus.Summary)(unsafe.Pointer(&metric))
	summary.Observe(float64(*(*int64)(unsafe.Pointer(&data))))
	 */
}


func (s *Server) PromethusEndpoint(port string, endpoint string) error {
	http.Handle("/" + endpoint, promhttp.Handler())
	log.Println(fmt.Sprintf("listening on :%s /%s", port, endpoint))
	err := http.ListenAndServe(":" + port, nil)
	if err != nil {
		return err
	}
	log.Fatalln("Shouldn't happen??!?!")
	return nil
}

func (s *Server) EHLO(context context.Context, args *FederatedServer) (*FederatedServer, error) {
	s.RecordMetrics("federated_servers", nil)
	s.Servers = append(s.Servers, args)

	return s.Servers[0], nil
}

func (s *Server) Ping(context context.Context, args *pb.NoParamsThisIsSilly) (*wrapperspb.StringValue, error) {
	s.RecordMetrics("pings", nil)
	return &wrapperspb.StringValue{Value: "Hello, wolrd!"}, nil
}

func (s *Server) Version(context context.Context, args *pb.NoParamsThisIsSilly) (*wrapperspb.StringValue, error) {
	return &wrapperspb.StringValue{Value: getVersion("beta")}, nil
}
