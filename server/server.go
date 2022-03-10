package server

import (
	"context"
	"crypto/sha256"
	"fmt"
	"hash"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"sync"
	"time"

	"github.com/ReneKroon/ttlcache/v2"
	"github.com/lbryio/hub/db"
	"github.com/lbryio/hub/internal/metrics"
	"github.com/lbryio/hub/meta"
	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/olivere/elastic/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	GrpcServer       *grpc.Server
	Args             *Args
	MultiSpaceRe     *regexp.Regexp
	WeirdCharsRe     *regexp.Regexp
	DB               *db.ReadOnlyDBColumnFamily
	EsClient         *elastic.Client
	QueryCache       *ttlcache.Cache
	S256             *hash.Hash
	LastRefreshCheck time.Time
	RefreshDelta     time.Duration
	NumESRefreshes   int64
	PeerServers      map[string]*Peer
	PeerServersMut   sync.RWMutex
	NumPeerServers   *int64
	PeerSubs         map[string]*Peer
	PeerSubsMut      sync.RWMutex
	NumPeerSubs      *int64
	ExternalIP       net.IP
	DBCleanup        func()
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

func (s *Server) PeerSubsLoadOrStore(peer *Peer) (actual *Peer, loaded bool) {
	key := peer.peerKey()
	s.PeerSubsMut.RLock()
	if actual, ok := s.PeerSubs[key]; ok {
		s.PeerSubsMut.RUnlock()
		return actual, true
	} else {
		s.PeerSubsMut.RUnlock()
		s.PeerSubsMut.Lock()
		s.PeerSubs[key] = peer
		s.PeerSubsMut.Unlock()
		return peer, false
	}
}

func (s *Server) PeerServersLoadOrStore(peer *Peer) (actual *Peer, loaded bool) {
	key := peer.peerKey()
	s.PeerServersMut.RLock()
	if actual, ok := s.PeerServers[key]; ok {
		s.PeerServersMut.RUnlock()
		return actual, true
	} else {
		s.PeerServersMut.RUnlock()
		s.PeerServersMut.Lock()
		s.PeerServers[key] = peer
		s.PeerServersMut.Unlock()
		return peer, false
	}
}

func (s *Server) Run() {
	l, err := net.Listen("tcp", ":"+s.Args.Port)
	if err != nil {
		log.Fatalf("failed to listen: %v", err)
	}

	pb.RegisterHubServer(s.GrpcServer, s)
	reflection.Register(s.GrpcServer)

	log.Printf("Server.Run() #### listening on %s\n", l.Addr().String())
	log.Printf("%#v\n", s.Args)
	if err := s.GrpcServer.Serve(l); err != nil {
		log.Fatalf("failed to serve: %v", err)
	}
}

// MakeHubServer takes the arguments given to a hub when it's started and
// initializes everything. It loads information about previously known peers,
// creates needed internal data structures, and initializes goroutines.
func MakeHubServer(ctx context.Context, args *Args) *Server {
	grpcServer := grpc.NewServer(grpc.NumStreamWorkers(10))

	multiSpaceRe, err := regexp.Compile(`\s{2,}`)
	if err != nil {
		log.Fatal(err)
	}

	weirdCharsRe, err := regexp.Compile("[#!~]")
	if err != nil {
		log.Fatal(err)
	}

	var client *elastic.Client = nil
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

	numPeers := new(int64)
	*numPeers = 0
	numSubs := new(int64)
	*numSubs = 0

	//TODO: is this the right place to load the db?
	var myDB *db.ReadOnlyDBColumnFamily
	var dbCleanup = func() {}
	if !args.DisableResolve {
		myDB, dbCleanup, err = db.GetProdDB(args.DBPath, "readonlytmp")
		if err != nil {
			// Can't load the db, fail loudly
			log.Fatalln(err)
		}
	}

	s := &Server{
		GrpcServer:       grpcServer,
		Args:             args,
		MultiSpaceRe:     multiSpaceRe,
		WeirdCharsRe:     weirdCharsRe,
		DB:               myDB,
		EsClient:         client,
		QueryCache:       cache,
		S256:             &s256,
		LastRefreshCheck: time.Now(),
		RefreshDelta:     refreshDelta,
		NumESRefreshes:   0,
		PeerServers:      make(map[string]*Peer),
		PeerServersMut:   sync.RWMutex{},
		NumPeerServers:   numPeers,
		PeerSubs:         make(map[string]*Peer),
		PeerSubsMut:      sync.RWMutex{},
		NumPeerSubs:      numSubs,
		ExternalIP:       net.IPv4(127, 0, 0, 1),
		DBCleanup:        dbCleanup,
	}

	// Start up our background services
	if !args.DisableResolve && !args.DisableRocksDBRefresh {
		db.RunDetectChanges(myDB)
	}
	if !args.DisableStartPrometheus {
		go s.prometheusEndpoint(s.Args.PrometheusPort, "metrics")
	}
	if !args.DisableStartUDP {
		go func() {
			err := UDPServer(args)
			if err != nil {
				log.Println("UDP Server failed!", err)
			}
		}()
	}
	// Load peers from disk and subscribe to one if there are any
	if !args.DisableLoadPeers {
		go func() {
			err := s.loadPeers()
			if err != nil {
				log.Println(err)
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
	metrics.RequestsCount.With(prometheus.Labels{"method": "hello"}).Inc()
	port := args.Port
	host := args.Host
	newPeer := &Peer{
		Address:  host,
		Port:     port,
		LastSeen: time.Now(),
	}
	log.Println(newPeer)

	err := s.addPeer(newPeer, false, true)
	// They just contacted us, so this shouldn't happen
	if err != nil {
		log.Println(err)
	}
	s.mergePeers(args.Servers)
	s.writePeers()
	s.notifyPeerSubs(newPeer)

	return s.makeHelloMessage(), nil
}

// PeerSubscribe adds a peer hub to the list of subscribers to update about
// new peers.
func (s *Server) PeerSubscribe(ctx context.Context, in *pb.ServerMessage) (*pb.StringValue, error) {
	metrics.RequestsCount.With(prometheus.Labels{"method": "peer_subscribe"}).Inc()
	var msg = "Success"
	peer := &Peer{
		Address:  in.Address,
		Port:     in.Port,
		LastSeen: time.Now(),
	}

	if _, loaded := s.PeerSubsLoadOrStore(peer); !loaded {
		s.incNumSubs()
		metrics.PeersSubscribed.Inc()
	} else {
		msg = "Already subscribed"
	}

	return &pb.StringValue{Value: msg}, nil
}

// AddPeer is a grpc endpoint to tell this hub about another hub in the network.
func (s *Server) AddPeer(ctx context.Context, args *pb.ServerMessage) (*pb.StringValue, error) {
	metrics.RequestsCount.With(prometheus.Labels{"method": "add_peer"}).Inc()
	var msg = "Success"
	newPeer := &Peer{
		Address:  args.Address,
		Port:     args.Port,
		LastSeen: time.Now(),
	}
	err := s.addPeer(newPeer, true, true)
	if err != nil {
		log.Println(err)
		msg = "Failed"
	}
	return &pb.StringValue{Value: msg}, err
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

/*
   async def claimtrie_resolve(self, *urls) -> str:
       sorted_urls = tuple(sorted(urls))
       self.session_manager.urls_to_resolve_count_metric.inc(len(sorted_urls))
       try:
           if sorted_urls in self.session_manager.resolve_outputs_cache:
               return self.session_manager.resolve_outputs_cache[sorted_urls]
           rows, extra = [], []
           for url in urls:
               if url not in self.session_manager.resolve_cache:
                   self.session_manager.resolve_cache[url] = await self._cached_resolve_url(url)
               stream, channel, repost, reposted_channel = self.session_manager.resolve_cache[url]
               if isinstance(channel, ResolveCensoredError):
                   rows.append(channel)
                   extra.append(channel.censor_row)
               elif isinstance(stream, ResolveCensoredError):
                   rows.append(stream)
                   extra.append(stream.censor_row)
               elif channel and not stream:
                   rows.append(channel)
                   # print("resolved channel", channel.name.decode())
                   if repost:
                       extra.append(repost)
                   if reposted_channel:
                       extra.append(reposted_channel)
               elif stream:
                   # print("resolved stream", stream.name.decode())
                   rows.append(stream)
                   if channel:
                       # print("and channel", channel.name.decode())
                       extra.append(channel)
                   if repost:
                       extra.append(repost)
                   if reposted_channel:
                       extra.append(reposted_channel)
               await asyncio.sleep(0)
           self.session_manager.resolve_outputs_cache[sorted_urls] = result = await self.loop.run_in_executor(
               None, Outputs.to_base64, rows, extra, 0, None, None
           )
           return result
       finally:
           self.session_manager.resolved_url_count_metric.inc(len(sorted_urls))
*/

func ResolveResultToOutput(res *db.ResolveResult) *pb.Output {
	claimMeta := &pb.ClaimMeta{
		ShortUrl:         res.ShortUrl,
		Reposted:         uint32(res.Reposted),
		IsControlling:    res.IsControlling,
		CreationHeight:   res.CreationHeight,
		ExpirationHeight: res.ExpirationHeight,
		EffectiveAmount:  res.EffectiveAmount,
		SupportAmount:    res.SupportAmount,
	}

	claim := &pb.Output_Claim{
		Claim: claimMeta,
	}

	output := &pb.Output{
		TxHash: res.TxHash,
		Nout:   uint32(res.Position),
		Height: res.Height,
		Meta:   claim,
	}

	return output
}

func ExpandedResolveResultToOutput(res *db.ExpandedResolveResult) ([]*pb.Output, []*pb.Output, error) {
	// FIXME: figure out the handling of rows and extra properly
	// FIXME: want to return empty list or nil when extraTxos is empty?
	txos := make([]*pb.Output, 0)
	extraTxos := make([]*pb.Output, 0)
	// Errors
	if x := res.Channel.GetError(); x != nil {
		outputErr := &pb.Output_Error{
			Error: &pb.Error{
				Text: x.Error.Error(),
				Code: 0, //FIXME
			},
		}
		res := &pb.Output{Meta: outputErr}
		txos = append(txos, res)
		return txos, nil, nil
	}
	if x := res.Stream.GetError(); x != nil {
		outputErr := &pb.Output_Error{
			Error: &pb.Error{
				Text: x.Error.Error(),
				Code: 0, //FIXME
			},
		}
		res := &pb.Output{Meta: outputErr}
		txos = append(txos, res)
		return txos, nil, nil
	}

	// Not errors
	var channel, stream, repost, repostedChannel *db.ResolveResult

	channel = res.Channel.GetResult()
	stream = res.Stream.GetResult()
	repost = res.Repost.GetResult()
	repostedChannel = res.RepostedChannel.GetResult()

	if channel != nil && stream == nil {
		// Channel
		output := ResolveResultToOutput(channel)
		txos = append(txos, output)

		if repost != nil {
			output := ResolveResultToOutput(repost)
			extraTxos = append(extraTxos, output)
		}
		if repostedChannel != nil {
			output := ResolveResultToOutput(repostedChannel)
			extraTxos = append(extraTxos, output)
		}

		return txos, extraTxos, nil
	} else if stream != nil {
		output := ResolveResultToOutput(channel)
		txos = append(txos, output)
		if channel != nil {
			output := ResolveResultToOutput(stream)
			extraTxos = append(extraTxos, output)
		}
		if repost != nil {
			output := ResolveResultToOutput(repost)
			extraTxos = append(extraTxos, output)
		}
		if repostedChannel != nil {
			output := ResolveResultToOutput(repostedChannel)
			extraTxos = append(extraTxos, output)
		}

		return txos, extraTxos, nil
	}

	return nil, nil, nil
}

func (s *Server) Resolve(ctx context.Context, args *pb.StringValue) (*pb.Outputs, error) {
	metrics.RequestsCount.With(prometheus.Labels{"method": "resolve"}).Inc()

	res := db.Resolve(s.DB, args.Value)
	txos, extraTxos, err := ExpandedResolveResultToOutput(res)
	if err != nil {
		return nil, err
	}

	return &pb.Outputs{
		Txos:         txos,
		ExtraTxos:    extraTxos,
		Total:        uint32(len(txos) + len(extraTxos)),
		Offset:       0,   //TODO
		Blocked:      nil, //TODO
		BlockedTotal: 0,   //TODO
	}, nil
}
