package server

import (
	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/olivere/elastic/v7"
	"google.golang.org/grpc"
	"log"
	"regexp"
)

type Server struct {
	GrpcServer   *grpc.Server
	Args 	     *Args
	MultiSpaceRe *regexp.Regexp
	WeirdCharsRe *regexp.Regexp
	EsClient     *elastic.Client
	pb.UnimplementedHubServer
}

type Args struct {
	Serve bool
	Host string
	Port string
	EsHost string
	EsPort string
	Dev bool
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
	grpcServer := grpc.NewServer()

	multiSpaceRe, err := regexp.Compile("\\s{2,}")
	if err != nil {
		log.Fatal(err)
	}

	weirdCharsRe, err := regexp.Compile("[#!~]")
	if err != nil {
		log.Fatal(err)
	}

	s := &Server {
		GrpcServer: grpcServer,
		Args: args,
		MultiSpaceRe: multiSpaceRe,
		WeirdCharsRe: weirdCharsRe,
	}

	return s
}