package server

import (
	"context"
	"fmt"
	"log"
	"os"
	"regexp"

	"net/http"
	"time"

	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/olivere/elastic/v7"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

type Server struct {
	GrpcServer   *grpc.Server
	Args         *Args
	MultiSpaceRe *regexp.Regexp
	WeirdCharsRe *regexp.Regexp
	EsClient     *elastic.Client
	Servers      []*FederatedServer
	pb.UnimplementedHubServer
}

type FederatedServer struct {
	Address string
	Port    string
	Ts      time.Time
	Ping    int //?
}

const majorVersion = 0

const (
	ServeCmd  = iota
	SearchCmd = iota
)

type Args struct {
	// TODO Make command types an enum
	CmdType int
	Host    string
	Port    string
	EsHost  string
	EsPort  string
	EsIndex string
	Debug   bool
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

	multiSpaceRe, err := regexp.Compile(`\s{2,}`)
	if err != nil {
		log.Fatal(err)
	}

	weirdCharsRe, err := regexp.Compile("[#!~]")
	if err != nil {
		log.Fatal(err)
	}
	self := &FederatedServer{
		Address: "127.0.0.1",
		Port:    args.Port,
		Ts:      time.Now(),
		Ping:    0,
	}
	servers := make([]*FederatedServer, 10)
	servers = append(servers, self)

	esUrl := args.EsHost + ":" + args.EsPort
	opts := []elastic.ClientOptionFunc{elastic.SetSniff(false), elastic.SetURL(esUrl)}
	if args.Debug {
		opts = append(opts, elastic.SetTraceLog(log.New(os.Stderr, "[[ELASTIC]]", 0)))
	}
	client, err := elastic.NewClient(opts...)
	if err != nil {
		log.Fatal(err)
	}
	s := &Server{
		GrpcServer:   grpcServer,
		Args:         args,
		MultiSpaceRe: multiSpaceRe,
		WeirdCharsRe: weirdCharsRe,
		EsClient:     client,
	}

	return s
}

func (s *Server) PromethusEndpoint(port string, endpoint string) error {
	http.Handle("/"+endpoint, promhttp.Handler())
	log.Println(fmt.Sprintf("listening on :%s /%s", port, endpoint))
	err := http.ListenAndServe(":"+port, nil)
	if err != nil {
		return err
	}
	log.Fatalln("Shouldn't happen??!?!")
	return nil
}

func (s *Server) Hello(context context.Context, args *FederatedServer) (*FederatedServer, error) {
	s.Servers = append(s.Servers, args)

	return s.Servers[0], nil
}

func (s *Server) Ping(context context.Context, args *pb.EmptyMessage) (*pb.StringValue, error) {
	return &pb.StringValue{Value: "Hello, world!"}, nil
}

func (s *Server) Version(context context.Context, args *pb.EmptyMessage) (*pb.StringValue, error) {
	return &pb.StringValue{Value: getVersion("beta")}, nil
}
