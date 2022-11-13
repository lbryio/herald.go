package server

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"hash"
	"io/ioutil"
	"log"
	"net"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"

	"github.com/ReneKroon/ttlcache/v2"
	"github.com/lbryio/herald.go/db"
	"github.com/lbryio/herald.go/internal/metrics"
	"github.com/lbryio/herald.go/meta"
	pb "github.com/lbryio/herald.go/protobuf/go"
	"github.com/lbryio/lbcd/chaincfg"
	"github.com/lbryio/lbry.go/v3/extras/stop"
	"github.com/olivere/elastic/v7"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	logrus "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

type Server struct {
	GrpcServer       *grpc.Server
	Args             *Args
	MultiSpaceRe     *regexp.Regexp
	WeirdCharsRe     *regexp.Regexp
	DB               *db.ReadOnlyDBColumnFamily
	Chain            *chaincfg.Params
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
	HeightSubs       map[net.Addr]net.Conn
	HeightSubsMut    sync.RWMutex
	NotifierChan     chan interface{}
	Grp              *stop.Group
	notiferListener  *net.TCPListener
	sessionManager   *sessionManager
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

// PeerSubsLoadOrStore thread safe load or store for peer subs
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

// PeerServersLoadOrStore thread safe load or store for peer servers
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

// Run "main" function for starting the server. This blocks.
func (s *Server) Run() {
	address := ":" + strconv.Itoa(s.Args.Port)
	l, err := net.Listen("tcp", address)
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

func (s *Server) Stop() {
	log.Println("Shutting down server...")

	if s.EsClient != nil {
		log.Println("Stopping es client...")
		s.EsClient.Stop()
	}
	if s.GrpcServer != nil {
		log.Println("Stopping grpc server...")
		s.GrpcServer.GracefulStop()
	}
	log.Println("Stopping other server threads...")
	s.Grp.StopAndWait()
	if s.DB != nil {
		log.Println("Stopping database connection...")
		s.DB.Shutdown()
	}

	log.Println("Returning from Stop...")
}

func LoadDatabase(args *Args, grp *stop.Group) (*db.ReadOnlyDBColumnFamily, error) {
	tmpName, err := ioutil.TempDir("", "go-lbry-hub")
	if err != nil {
		logrus.Info(err)
		log.Fatal(err)
	}
	logrus.Info("tmpName", tmpName)
	if err != nil {
		logrus.Info(err)
	}
	myDB, err := db.GetProdDB(args.DBPath, tmpName, grp)
	if err != nil {
		// Can't load the db, fail loudly
		logrus.Info(err)
		log.Fatalln(err)
	}

	if myDB.LastState != nil {
		logrus.Infof("DB version: %v", myDB.LastState.DBVersion)
		logrus.Infof("height: %v", myDB.LastState.Height)
		logrus.Infof("genesis: %v", myDB.LastState.Genesis.String())
		logrus.Infof("tip: %v", myDB.LastState.Tip.String())
		logrus.Infof("tx count: %v", myDB.LastState.TxCount)
	}

	blockingChannelHashes := make([][]byte, 0, 10)
	blockingIds := make([]string, 0, 10)
	filteringChannelHashes := make([][]byte, 0, 10)
	filteringIds := make([]string, 0, 10)

	for _, id := range args.BlockingChannelIds {
		hash, err := hex.DecodeString(id)
		if err != nil {
			logrus.Warn("Invalid channel id: ", id)
			continue
		}
		blockingChannelHashes = append(blockingChannelHashes, hash)
		blockingIds = append(blockingIds, id)
	}

	for _, id := range args.FilteringChannelIds {
		hash, err := hex.DecodeString(id)
		if err != nil {
			logrus.Warn("Invalid channel id: ", id)
			continue
		}
		filteringChannelHashes = append(filteringChannelHashes, hash)
		filteringIds = append(filteringIds, id)
	}

	myDB.BlockingChannelHashes = blockingChannelHashes
	myDB.FilteringChannelHashes = filteringChannelHashes

	if len(filteringIds) > 0 {
		logrus.Infof("filtering claims reposted by channels: %+s", filteringIds)
	}
	if len(blockingIds) > 0 {
		logrus.Infof("blocking claims reposted by channels: %+s", blockingIds)
	}

	return myDB, nil
}

// MakeHubServer takes the arguments given to a hub when it's started and
// initializes everything. It loads information about previously known peers,
// creates needed internal data structures, and initializes goroutines.
func MakeHubServer(grp *stop.Group, args *Args) *Server {
	grpcServer := grpc.NewServer(grpc.NumStreamWorkers(0))

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
	if !args.DisableResolve {
		myDB, err = LoadDatabase(args, grp)
		if err != nil {
			logrus.Warning(err)
		}
	}

	// Determine which chain to use based on db and cli values
	dbChain := (*chaincfg.Params)(nil)
	if myDB != nil && myDB.LastState != nil {
		// The chain params can be inferred from DBStateValue.
		switch myDB.LastState.Genesis.Hash {
		case *chaincfg.MainNetParams.GenesisHash:
			dbChain = &chaincfg.MainNetParams
		case *chaincfg.TestNet3Params.GenesisHash:
			dbChain = &chaincfg.TestNet3Params
		case *chaincfg.RegressionNetParams.GenesisHash:
			dbChain = &chaincfg.RegressionNetParams
		}
	}
	cliChain := (*chaincfg.Params)(nil)
	if args.Chain != nil {
		switch *args.Chain {
		case chaincfg.MainNetParams.Name:
			cliChain = &chaincfg.MainNetParams
		case chaincfg.TestNet3Params.Name, "testnet":
			cliChain = &chaincfg.TestNet3Params
		case chaincfg.RegressionNetParams.Name:
			cliChain = &chaincfg.RegressionNetParams
		}
	}
	chain := chaincfg.MainNetParams
	if dbChain != nil && cliChain != nil {
		if dbChain != cliChain {
			logrus.Warnf("network: %v (from db) conflicts with %v (from cli)", dbChain.Name, cliChain.Name)
		}
		chain = *dbChain
	} else if dbChain != nil {
		chain = *dbChain
	} else if cliChain != nil {
		chain = *cliChain
	}
	logrus.Infof("network: %v", chain.Name)

	args.GenesisHash = chain.GenesisHash.String()

	sessionGrp := stop.New(grp)

	s := &Server{
		GrpcServer:       grpcServer,
		Args:             args,
		MultiSpaceRe:     multiSpaceRe,
		WeirdCharsRe:     weirdCharsRe,
		DB:               myDB,
		Chain:            &chain,
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
		HeightSubs:       make(map[net.Addr]net.Conn),
		HeightSubsMut:    sync.RWMutex{},
		NotifierChan:     make(chan interface{}, 1),
		Grp:              grp,
		sessionManager:   nil,
	}
	// FIXME: HACK
	s.sessionManager = newSessionManager(s, myDB, args, sessionGrp, &chain)

	// Start up our background services
	if !args.DisableResolve && !args.DisableRocksDBRefresh {
		logrus.Info("Running detect changes")
		myDB.RunDetectChanges(s.NotifierChan)
	}
	if !args.DisableBlockingAndFiltering {
		myDB.RunGetBlocksAndFilters()
	}
	if !args.DisableStartPrometheus {
		go s.prometheusEndpoint(s.Args.PrometheusPort, "metrics")
	}
	if !args.DisableStartUDP {
		go func() {
			err := s.UDPServer(s.Args.Port)
			if err != nil {
				logrus.Errorf("UDP Server (%d) failed! %v", s.Args.Port, err)
			}
		}()
		if s.Args.JSONRPCPort != 0 {
			go func() {
				err := s.UDPServer(s.Args.JSONRPCPort)
				if err != nil {
					logrus.Errorf("UDP Server (%d) failed! %v", s.Args.JSONRPCPort, err)
				}
			}()
		}
	}
	if !args.DisableStartNotifier {
		go func() {
			err := s.NotifierServer()
			if err != nil {
				log.Println("Notifier Server failed!", err)
			}
		}()
		go func() {
			err := s.RunNotifier()
			if err != nil {
				log.Println("RunNotifier failed!", err)
			}
		}()
	}
	if !args.DisableStartJSONRPC {
		go func() {
			err := s.StartJsonRPC()
			if err != nil {
				log.Println("JSONRPC Server failed!", err)
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

func (s *Server) Height(ctx context.Context, args *pb.EmptyMessage) (*pb.UInt32Value, error) {
	metrics.RequestsCount.With(prometheus.Labels{"method": "height"}).Inc()
	if s.DB != nil {
		return &pb.UInt32Value{Value: s.DB.LastState.Height}, nil
	} else {
		return &pb.UInt32Value{Value: 0}, nil
	}
}

// HeightSubscribe takes a height to wait for the server to reach and waits until it reaches that
// height or higher and returns the current height. If the db is off it will return 0.
func (s *Server) HeightSubscribe(arg *pb.UInt32Value, stream pb.Hub_HeightSubscribeServer) error {
	metrics.RequestsCount.With(prometheus.Labels{"method": "height"}).Inc()
	if s.DB != nil {
		want := arg.Value
		for s.DB.LastState.Height < want {
			if s.DB.LastState.Height >= want {
				err := stream.Send(&pb.UInt32Value{Value: s.DB.LastState.Height})
				if err != nil {
					return err
				}
				return nil
			}
			time.Sleep(time.Millisecond * 100)
		}
	} else {
		if err := stream.Send(&pb.UInt32Value{Value: 0}); err != nil {
			return err
		}
	}
	return nil
}

// HeightHashSubscribe takes a height to wait for the server to reach and waits until it reaches that
// height or higher and returns the current height. If the db is off it will return 0.
func (s *Server) HeightHashSubscribe() error {
	metrics.RequestsCount.With(prometheus.Labels{"method": "height_hash"}).Inc()

	return nil
}

// Resolve is the gRPC endpoint for resolve.
func (s *Server) Resolve(ctx context.Context, args *pb.StringArray) (*pb.Outputs, error) {
	return InternalResolve(args.Value, s.DB)
}

// InternalResolve takes an array of urls and resolves them to their transactions.
func InternalResolve(urls []string, DB *db.ReadOnlyDBColumnFamily) (*pb.Outputs, error) {
	if DB == nil {
		return nil, errors.New("db is nil")
		// return nil, nil
	}
	metrics.RequestsCount.With(prometheus.Labels{"method": "resolve"}).Inc()

	allTxos := make([]*pb.Output, 0)
	allExtraTxos := make([]*pb.Output, 0)

	for _, url := range urls {
		res := DB.Resolve(url)
		txos, extraTxos, err := res.ToOutputs()
		if err != nil {
			return nil, err
		}
		// TODO: there may be a more efficient way to do this.
		allTxos = append(allTxos, txos...)
		allExtraTxos = append(allExtraTxos, extraTxos...)
	}

	res := &pb.Outputs{
		Txos:         allTxos,
		ExtraTxos:    allExtraTxos,
		Total:        uint32(len(allTxos) + len(allExtraTxos)),
		Offset:       0,   //TODO
		Blocked:      nil, //TODO
		BlockedTotal: 0,   //TODO
	}

	logrus.Warn(res)

	return res, nil
}
