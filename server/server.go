package server

import (
	"context"
	pb "github.com/lbryio/hub/protobuf/go"
	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

type Server struct {
	pb.UnimplementedHubServer
}

type Args struct {
	Serve bool
	Port string
	User string
	Pass string
}

type AccessDeniedErr struct {}
func (AccessDeniedErr) Error() string {
	return "Username or password incorrect."
}

type EmptyMetadataErr struct {}
func (EmptyMetadataErr) Error() string {
	return "No username or password specified."
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

func MakeHubServer(args Args) *grpc.Server {
	authorize := makeAuthorizeFunc(args.User, args.Pass)
	return grpc.NewServer(
		grpc.StreamInterceptor(makeStreamInterceptor(authorize)),
		grpc.UnaryInterceptor(makeUnaryInterceptor(authorize)),
	)
}

func makeStreamInterceptor(authorize func(context.Context) error) func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
	return func(srv interface{}, stream grpc.ServerStream, info *grpc.StreamServerInfo, handler grpc.StreamHandler) error {
		if err := authorize(stream.Context()); err != nil {
			return err
		}

		return handler(srv, stream)
	}
}


func makeUnaryInterceptor(authorize func(context.Context) error) func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
	return func(ctx context.Context, req interface{}, info *grpc.UnaryServerInfo, handler grpc.UnaryHandler) (interface{}, error) {
		if err := authorize(ctx); err != nil {
			return nil, err
		}

		return handler(ctx, req)
	}
}

func makeAuthorizeFunc(username string, password string) func(context.Context) error {

	return func(ctx context.Context) error {
		if md, ok := metadata.FromIncomingContext(ctx); ok {
			if len(md["username"]) > 0 && md["username"][0] == username &&
				len(md["password"]) > 0 && md["password"][0] == password {
				return nil
			}

			return AccessDeniedErr{}
		}

		return EmptyMetadataErr{}
	}
}
