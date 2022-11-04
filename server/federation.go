package server

import (
	"bufio"
	"context"
	"math"
	"net"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lbryio/herald.go/internal/metrics"
	pb "github.com/lbryio/herald.go/protobuf/go"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

// Peer holds relevant information about peers that we know about.
type Peer struct {
	Address  string
	Port     string
	LastSeen time.Time
}

var (
	localHosts = map[string]bool{
		"127.0.0.1": true,
		"0.0.0.0":   true,
		"localhost": true,
		"<nil>":     true, // Empty net.IP turned into a string
	}
)

// peerKey takes a peer and returns the key that for that peer
// in our peer table.
func peerKey(peer *Peer) string {
	return peer.Address + ":" + peer.Port
}

// peerKey is a function on a FederatedServer struct to return the key for that
// peer is out peer table.
func (peer *Peer) peerKey() string {
	return peer.Address + ":" + peer.Port
}

func (s *Server) incNumPeers() {
	atomic.AddInt64(s.NumPeerServers, 1)
}

func (s *Server) decNumPeers() {
	atomic.AddInt64(s.NumPeerServers, -1)
}

func (s *Server) getNumPeers() int64 {
	return *s.NumPeerServers
}

func (s *Server) incNumSubs() {
	atomic.AddInt64(s.NumPeerSubs, 1)
}

func (s *Server) decNumSubs() {
	atomic.AddInt64(s.NumPeerSubs, -1)
}

func (s *Server) getNumSubs() int64 {
	return *s.NumPeerSubs
}

// getAndSetExternalIp detects the server's external IP and stores it.
func (s *Server) getAndSetExternalIp(ip, port string) error {
	pong, err := UDPPing(ip, port)
	if err != nil {
		return err
	}
	myIp := pong.DecodeAddress()
	log.Println("my ip: ", myIp)
	s.ExternalIP = myIp

	return nil
}

// loadPeers takes the arguments given to the hub at startup and loads the
// previously known peers from disk and verifies their existence before
// storing them as known peers. Returns a map of peerKey -> object
func (s *Server) loadPeers() error {
	peerFile := s.Args.PeerFile
	port := strconv.Itoa(s.Args.Port)

	// First we make sure our server has come up, so we can answer back to peers.
	var failures = 0
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	// log.Println("loadPeers #### waiting for server to come up")
retry:
	time.Sleep(time.Second * time.Duration(math.Pow(float64(failures), 2)))
	conn, err := grpc.DialContext(ctx,
		"0.0.0.0:"+port,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)

	if err != nil {
		if failures > 3 {
			log.Println("Warning! Our endpoint doesn't seem to have come up, didn't load peers")
			return err
		}
		failures += 1
		goto retry
	}
	if err = conn.Close(); err != nil {
		log.Println(err)
	}
	cancel()
	// log.Println("loadPeers #### Past checking for server to come up")

	f, err := os.Open(peerFile)
	if err != nil {
		// log.Println(err)
		return err
	}
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	var text []string
	for scanner.Scan() {
		text = append(text, scanner.Text())
	}
	err = f.Close()
	if err != nil {
		log.Println("peer file failed to close: ", err)
	}

	for _, line := range text {
		ipPort := strings.Split(line, ":")
		if len(ipPort) != 2 {
			log.Println("Malformed entry in peer file")
			continue
		}
		// If the peer is us, skip
		log.Println(ipPort)
		if ipPort[1] == port &&
			(localHosts[ipPort[0]] || ipPort[0] == s.ExternalIP.String()) {
			log.Println("Self peer, skipping ...")
			continue
		}

		newPeer := &Peer{
			Address:  ipPort[0],
			Port:     ipPort[1],
			LastSeen: time.Now(),
		}
		log.Printf("pinging peer %+v\n", newPeer)
		err = s.addPeer(newPeer, true, true)
		if err != nil {
			log.Println(err)
		}

	}

	log.Println("Returning from loadPeers")
	return nil
}

// subscribeToPeer subscribes us to a peer to we'll get updates about their
// known peers.
func (s *Server) subscribeToPeer(peer *Peer) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx,
		peer.Address+":"+peer.Port,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := &pb.ServerMessage{
		Address: s.ExternalIP.String(),
		Port:    strconv.Itoa(s.Args.Port),
	}

	c := pb.NewHubClient(conn)

	log.Printf("%s:%d subscribing to %+v\n", s.ExternalIP, s.Args.Port, peer)
	_, err = c.PeerSubscribe(ctx, msg)
	if err != nil {
		return err
	}

	return nil
}

// helloPeer takes a peer to say hello to and sends a hello message
// containing all the peers we know about and information about us.
// This is used to confirm existence of peers on start and let them
// know about us. Returns the response from the server on success,
// nil otherwise.
func (s *Server) helloPeer(peer *Peer) (*pb.HelloMessage, error) {
	log.Println("In helloPeer")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx,
		peer.Address+":"+peer.Port,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer conn.Close()

	c := pb.NewHubClient(conn)

	msg := &pb.HelloMessage{
		Port:    strconv.Itoa(s.Args.Port),
		Host:    s.ExternalIP.String(),
		Servers: []*pb.ServerMessage{},
	}

	log.Printf("%s:%d saying hello to %+v\n", s.ExternalIP, s.Args.Port, peer)
	res, err := c.Hello(ctx, msg)
	if err != nil {
		log.Println(err)
		return nil, err
	}

	log.Println(res)

	return res, nil
}

// writePeers writes our current known peers to disk.
func (s *Server) writePeers() {
	if s.Args.DisableWritePeers {
		return
	}
	f, err := os.Create(s.Args.PeerFile)
	if err != nil {
		log.Println(err)
		return
	}
	writer := bufio.NewWriter(f)

	for key := range s.PeerServers {
		line := key + "\n"
		_, err := writer.WriteString(line)
		if err != nil {
			log.Println(err)
		}
	}

	err = writer.Flush()
	if err != nil {
		log.Println(err)
	}
	err = f.Close()
	if err != nil {
		log.Println(err)
	}
}

// notifyPeer takes a peer to notify and a new peer we just learned about
// and informs the already known peer about the new peer.
func (s *Server) notifyPeer(peerToNotify *Peer, newPeer *Peer) error {
	if s.Args.DisableFederation {
		return nil
	}
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx,
		peerToNotify.Address+":"+peerToNotify.Port,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := &pb.ServerMessage{
		Address: newPeer.Address,
		Port:    newPeer.Port,
	}

	c := pb.NewHubClient(conn)

	_, err = c.AddPeer(ctx, msg)
	if err != nil {
		return err
	}

	return nil
}

// notifyPeerSubs takes a new peer server we just learned about and notifies
// all the peers that have subscribed to us about it.
func (s *Server) notifyPeerSubs(newPeer *Peer) {
	var unsubscribe []string
	s.PeerSubsMut.RLock()
	for key, peer := range s.PeerSubs {
		log.Printf("Notifying peer %s of new node %+v\n", key, newPeer)
		err := s.notifyPeer(peer, newPeer)
		if err != nil {
			log.Println("Failed to send data to ", key)
			log.Println(err)
			unsubscribe = append(unsubscribe, key)
		}
	}
	s.PeerSubsMut.RUnlock()

	s.PeerSubsMut.Lock()
	for _, key := range unsubscribe {
		if _, ok := s.PeerSubs[key]; ok {
			delete(s.PeerSubs, key)
			s.decNumSubs()
			metrics.PeersSubscribed.Dec()
		}
	}
	s.PeerSubsMut.Unlock()
}

// addPeer takes a new peer, optionally checks to see if they're online, and
// adds them to our list of peers. It will also optionally subscribe to it.
func (s *Server) addPeer(newPeer *Peer, ping bool, subscribe bool) error {
	if s.Args.DisableFederation {
		return nil
	}
	// First thing we get our external ip if we don't have it, otherwise we
	// could end up subscribed to our self, which is silly.
	nilIP := net.IP{}
	localIP1 := net.IPv4(127, 0, 0, 1)
	if s.ExternalIP.Equal(nilIP) || s.ExternalIP.Equal(localIP1) {
		err := s.getAndSetExternalIp(newPeer.Address, newPeer.Port)
		if err != nil {
			log.Println(err)
			log.Println("WARNING: can't determine external IP, continuing with ", s.Args.Host)
		}
	}

	if strconv.Itoa(s.Args.Port) == newPeer.Port &&
		(localHosts[newPeer.Address] || newPeer.Address == s.ExternalIP.String()) {
		log.Printf("%s:%d addPeer: Self peer, skipping...\n", s.ExternalIP, s.Args.Port)
		return nil
	}

	k := peerKey(newPeer)

	log.Printf("%s:%d adding peer %+v\n", s.ExternalIP, s.Args.Port, newPeer)
	if oldServer, loaded := s.PeerServersLoadOrStore(newPeer); !loaded {
		if ping {
			_, err := s.helloPeer(newPeer)
			if err != nil {
				s.PeerServersMut.Lock()
				delete(s.PeerServers, k)
				s.PeerServersMut.Unlock()
				return err
			}
		}

		s.incNumPeers()
		metrics.PeersKnown.Inc()
		s.writePeers()
		s.notifyPeerSubs(newPeer)
		// This is weird because we're doing grpc and jsonrpc here.
		// Do we still want to custom grpc?
		log.Warn("Sending peer to NotifierChan")
		s.NotifierChan <- newPeer

		// Subscribe to all our peers for now
		if subscribe {
			err := s.subscribeToPeer(newPeer)
			if err != nil {
				return err
			}
		}
	} else {
		oldServer.LastSeen = time.Now()
	}
	return nil
}

// mergePeers is an internal convenience function to add a list of
// peers.
func (s *Server) mergePeers(servers []*pb.ServerMessage) {
	for _, srvMsg := range servers {
		newPeer := &Peer{
			Address:  srvMsg.Address,
			Port:     srvMsg.Port,
			LastSeen: time.Now(),
		}
		err := s.addPeer(newPeer, false, true)
		// This shouldn't happen because we're not pinging them.
		if err != nil {
			log.Println(err)
		}
	}
}

// makeHelloMessage makes a message for this hub to call the Hello endpoint
// on another hub.
func (s *Server) makeHelloMessage() *pb.HelloMessage {
	servers := make([]*pb.ServerMessage, 0, 10)

	s.PeerServersMut.RLock()
	for _, peer := range s.PeerServers {
		servers = append(servers, &pb.ServerMessage{
			Address: peer.Address,
			Port:    peer.Port,
		})
	}
	s.PeerServersMut.RUnlock()

	return &pb.HelloMessage{
		Port:    strconv.Itoa(s.Args.Port),
		Host:    s.ExternalIP.String(),
		Servers: servers,
	}
}
