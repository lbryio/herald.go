package server

import (
	"bufio"
	"context"
	"log"
	"os"
	"strings"
	"sync/atomic"
	"time"

	"github.com/lbryio/hub/internal/metrics"
	pb "github.com/lbryio/hub/protobuf/go"
	"google.golang.org/grpc"
)

// FederatedServer hold relevant information about peers that we known about.
type FederatedServer struct {
	Address string
	Port    string
	Ts      time.Time
}

var (
	localHosts = map[string]bool{
		"127.0.0.1": true,
		"0.0.0.0": true,
		"localhost": true,
	}
)


// peerKey takes a ServerMessage object and returns the key that for that peer
// in our peer table.
func peerKey(msg *pb.ServerMessage) string {
	return msg.Address + ":" + msg.Port
}

// peerKey is a function on a FederatedServer struct to return the key for that
// peer is out peer table.
func (peer *FederatedServer) peerKey() string {
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

// loadPeers takes the arguments given to the hub at startup and loads the
// previously known peers from disk and verifies their existence before
// storing them as known peers. Returns a map of peerKey -> object
func (s *Server) loadPeers() error {
	peerFile := s.Args.PeerFile
	port := s.Args.Port

	f, err := os.Open(peerFile)
	if err != nil {
		log.Println(err)
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
		ipPort := strings.Split(line,":")
		if len(ipPort) != 2 {
			log.Println("Malformed entry in peer file")
			continue
		}
		// If the peer is us, skip
		log.Println(ipPort)
		if ipPort[1] == port && localHosts[ipPort[0]] {
			log.Println("Self peer, skipping ...")
			continue
		}
		srvMsg := &pb.ServerMessage{
			Address: ipPort[0],
			Port:    ipPort[1],
		}
		log.Printf("pinging peer %+v\n", srvMsg)
		err := s.addPeer(srvMsg, true)
		if err != nil {
			log.Println(err)
		}
	}

	log.Println("Returning from loadPeers")
	return nil
}


// getFastestPeer determines the fastest peer in its list of peers by sending
// out udp pings and seeing who responds first. This is currently not
// implemented and just returns the first peer.
func (s *Server) getFastestPeer() *FederatedServer {
	var fastestPeer *FederatedServer

	for _, v := range s.PeerServers {
		return v
	}

	return fastestPeer
}

// subscribeToFastestPeer is a convenience function to find and subscribe to
// the fastest peer we know about.
func (s *Server) subscribeToFastestPeer() {
	peer := s.getFastestPeer()
	if peer != nil {
		err := s.subscribeToPeer(peer)
		if err != nil {
			log.Println(err)
		}
	} else {
		log.Println("No peers found, not subscribed to any.")
	}
}

// subscribeToPeer subscribes us to a peer to we'll get updates about their
// known peers.
func (s *Server) subscribeToPeer(peer *FederatedServer) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx,
		peer.Address+":"+peer.Port,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := &pb.ServerMessage{
		Address: s.Args.Host,
		Port:    s.Args.Port,
	}

	c := pb.NewHubClient(conn)

	log.Printf("%s:%s subscribing to %+v\n", s.Args.Host, s.Args.Port, peer)
	_, err = c.PeerSubscribe(ctx, msg)
	if err != nil {
		return err
	}

	s.Subscribed = true

	return nil
}

// helloPeer takes a peer to say hello to and sends a hello message
// containing all the peers we know about and information about us.
// This is used to confirm existence of peers on start and let them
// know about us. Returns the response from the server on success,
// nil otherwise.
func (s *Server) helloPeer(server *FederatedServer) (*pb.HelloMessage, error) {
	log.Println("In helloPeer")
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx,
		server.Address+":"+server.Port,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Println(err)
		return nil, err
	}
	defer conn.Close()

	c := pb.NewHubClient(conn)

	msg := &pb.HelloMessage{
		Port: s.Args.Port,
		Host: s.Args.Host,
		Servers: []*pb.ServerMessage{},
	}

	log.Printf("%s:%s saying hello to %+v\n", s.Args.Host, s.Args.Port, server)
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
	if !s.Args.WritePeers {
		return
	}
	f, err := os.Create(s.Args.PeerFile)
	if err != nil {
		log.Println(err)
		return
	}
	writer := bufio.NewWriter(f)

	for key, _ := range s.PeerServers {
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
// and calls AddPeer on the first.
func notifyPeer(peerToNotify *FederatedServer, newPeer *FederatedServer) error {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	conn, err := grpc.DialContext(ctx,
		peerToNotify.Address+":"+peerToNotify.Port,
		grpc.WithInsecure(),
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
func (s *Server) notifyPeerSubs(newServer *FederatedServer) {
	var unsubscribe []string
	s.PeerSubsMut.RLock()
	for key, peer := range s.PeerSubs {
		log.Printf("Notifying peer %s of new node %+v\n", key, newServer)
		err := notifyPeer(peer, newServer)
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

// addPeer takes a new peer as a pb.ServerMessage, optionally checks to see
// if they're online, and adds them to our list of peer. If we're not currently
// subscribed to a peer, it will also subscribe to it.
func (s *Server) addPeer(msg *pb.ServerMessage, ping bool) error {
	if s.Args.Port == msg.Port &&
		(localHosts[msg.Address] || msg.Address == s.Args.Host) {
		log.Printf("%s:%s addPeer: Self peer, skipping...\n", s.Args.Host, s.Args.Port)
		return nil
	}
	k := peerKey(msg)
	newServer := &FederatedServer{
		Address: msg.Address,
		Port: msg.Port,
		Ts: time.Now(),
	}
	log.Printf("%s:%s adding peer %+v\n", s.Args.Host, s.Args.Port, msg)
	if oldServer, loaded := s.PeerServersLoadOrStore(newServer); !loaded {
		if ping {
			_, err := s.helloPeer(newServer)
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
		s.notifyPeerSubs(newServer)

		// Subscribe to all our peers for now
		err := s.subscribeToPeer(newServer)
		if err != nil {
			return err
		} else {
			s.Subscribed = true
		}
	} else {
		oldServer.Ts = time.Now()
	}
	return nil
}

// mergeFederatedServers is an internal convenience function to add a list of
// peers.
func (s *Server) mergeFederatedServers(servers []*pb.ServerMessage) {
	for _, srvMsg := range servers {
		err := s.addPeer(srvMsg, false)
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
			Port:	 peer.Port,
		})
	}
	s.PeerServersMut.RUnlock()

	return &pb.HelloMessage{
		Port: s.Args.Port,
		Host: s.Args.Host,
		Servers: servers,
	}
}
