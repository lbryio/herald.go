package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"log"
	"net/http"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/ReneKroon/ttlcache/v2"
	"github.com/lbryio/hub/internal/metrics"
	"github.com/lbryio/hub/meta"
	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/olivere/elastic/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
)

type Server struct {
	GrpcServer   	 *grpc.Server
	Args         	 *Args
	MultiSpaceRe 	 *regexp.Regexp
	WeirdCharsRe 	 *regexp.Regexp
	EsClient     	 *elastic.Client
	Servers      	 map[string]*FederatedServer
	QueryCache   	 *ttlcache.Cache
	S256		 	 *hash.Hash
	LastRefreshCheck time.Time
	RefreshDelta     time.Duration
	NumESRefreshes   int64
	PeerSubs		 sync.Map
	peerChannel      chan *peerAddMsg
	pb.UnimplementedHubServer
}


func getVersion() string {
	return meta.Version
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

// MakeHubServer takes the arguments given to a hub when it's started and
// initializes everything. It loads information about previously known peers,
// creates needed internal data structures, and initializes goroutines.
func MakeHubServer(ctx context.Context, args *Args) *Server {
	grpcServer := grpc.NewServer(grpc.NumStreamWorkers(10))

	peerChannel := make(chan *peerAddMsg)

	multiSpaceRe, err := regexp.Compile(`\s{2,}`)
	if err != nil {
		log.Fatal(err)
	}

	weirdCharsRe, err := regexp.Compile("[#!~]")
	if err != nil {
		log.Fatal(err)
	}

	servers := loadPeers(args)

	var client *elastic.Client
	if !args.DisableEs {
		esUrl := args.EsHost + ":" + args.EsPort
		opts := []elastic.ClientOptionFunc{
			elastic.SetSniff(true),
			elastic.SetSnifferTimeoutStartup(time.Second * 60),
			elastic.SetSnifferTimeout(time.Second * 60),
			elastic.SetURL(esUrl),
		}
		if args.Debug {
			opts = append(opts, elastic.SetTraceLog(log.New(os.Stderr, "[[ELASTIC]]", 0)))
		}
		client, err = elastic.NewClient(opts...)
		if err != nil {
			log.Fatal(err)
		}
	} else {
		client = nil
	}

	cache := ttlcache.NewCache()
	err = cache.SetTTL(time.Duration(args.CacheTTL) * time.Minute)
	if err != nil {
		log.Fatal(err)
	}
	s256 := sha256.New()
	var refreshDelta = time.Second * time.Duration(args.RefreshDelta)
	if args.Debug {
		refreshDelta = time.Second * 0
	}

	s := &Server{
		GrpcServer:       grpcServer,
		Args:         	  args,
		MultiSpaceRe: 	  multiSpaceRe,
		WeirdCharsRe: 	  weirdCharsRe,
		EsClient:     	  client,
		QueryCache:   	  cache,
		S256:         	  &s256,
		LastRefreshCheck: time.Now(),
		RefreshDelta: 	  refreshDelta,
		NumESRefreshes:   0,
		Servers: 		  servers,
		PeerSubs:         sync.Map{},
		peerChannel:      peerChannel,
	}

	// Start up our background services
	if args.StartPeerAdder {
		go s.peerAdder(ctx)
	}
	if args.StartPrometheus {
		go s.prometheusEndpoint(s.Args.PrometheusPort, "metrics")
	}
	if args.StartUDP {
		go func() {
			err := UDPServer(args)
			if err != nil {
				log.Println("UDP Server failed!", err)
			}
		}()
	}

	return s
}

// prometheusEndpoint is a goroutine which start up a prometheus endpoint
// for this hub to allow for metric tracking.
func (s *Server) prometheusEndpoint(port string, endpoint string) {
	http.Handle("/"+endpoint, promhttp.Handler())
	log.Println(fmt.Sprintf("listening on :%s /%s", port, endpoint))
	err := http.ListenAndServe(":"+port, nil)
	log.Fatalln("Shouldn't happen??!?!", err)
}

// Hello is a grpc endpoint to allow another hub to tell us about itself.
// The passed message includes information about the other hub, and all
// of its peers which are added to the knowledge of this hub.
func (s *Server) Hello(ctx context.Context, args *pb.HelloMessage) (*pb.HelloMessage, error) {
	port := args.Port
	host := args.Host
	server := &FederatedServer{
		Address: host,
		Port: port,
		Ts: time.Now(),
	}
	log.Println(server)

	s.addPeer(&pb.ServerMessage{Address: host, Port: port}, false)
	s.mergeFederatedServers(args.Servers)
	s.writePeers()
	s.notifyPeerSubs(server)

	return s.makeHelloMessage(), nil
}

// PeerSubscribe adds a peer hub to the list of subscribers to update about
// new peers.
func (s *Server) PeerSubscribe(ctx context.Context, in *pb.ServerMessage) (*pb.StringValue, error) {
	peer := &FederatedServer{
		Address: in.Address,
		Port:    in.Port,
		Ts:      time.Now(),
	}

	s.PeerSubs.Store(peerKey(in), peer)

	return &pb.StringValue{Value: "Success"}, nil
}

// PeerSubscribeStreaming is a streaming grpc endpoint that allows another hub to
// subscribe to this hub for peer updates. This first loops through all the
// peers that this hub knows about and sends them to the connecting hub,
// it then stores the peer, along with a channel to indicate when it's finished
// and it's stream context in our map of peer subs. Finally, it waits on the
// context to finish, or a message in the done channel. This function cannot
// exit while the peer is subscribe or the context will die. Communicating with
// the peer is handled by the peerAdder goroutine in federation.go, which
// listens on a channel of new peers and notifies all our connected peers when
// we find out about a new one.
func (s *Server) PeerSubscribeStreaming(in *pb.ServerMessage, stream pb.Hub_PeerSubscribeStreamingServer) error {
	for _, server := range s.Servers {
		err := stream.Send(&pb.ServerMessage{
			Address: server.Address,
			Port:    server.Port,
		})
		if err != nil {
			return err
		}
	}

	done := make(chan bool)
	ctx := stream.Context()
	s.PeerSubs.Store(peerKey(in), &sub{stream: stream, done: done})
	for {
		select {
		case <-done:
			log.Println("Removing client:", in)
			return nil
		case <-ctx.Done():
			log.Println("Client disconnected: ", in)
			return nil
		}
	}
}

// AddPeer is a grpc endpoint to tell this hub about another hub in the network.
func (s *Server) AddPeer(ctx context.Context, args *pb.ServerMessage) (*pb.StringValue, error) {
	s.addPeer(args, true)
	return &pb.StringValue{Value: "Success!"}, nil
}

// Ping is a grpc endpoint that returns a short message.
func (s *Server) Ping(ctx context.Context, args *pb.EmptyMessage) (*pb.StringValue, error) {
	metrics.RequestsCount.With(prometheus.Labels{"method": "ping"}).Inc()
	return &pb.StringValue{Value: "Hello, world!"}, nil
}

// Version is a grpc endpoint to get this hub's version.
func (s *Server) Version(ctx context.Context, args *pb.EmptyMessage) (*pb.StringValue, error) {
	metrics.RequestsCount.With(prometheus.Labels{"method": "version"}).Inc()
	return &pb.StringValue{Value: getVersion()}, nil
}
