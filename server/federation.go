package server

import (
	"bufio"
	"context"
	"log"
	"os"
	"strings"
	"time"

	pb "github.com/lbryio/hub/protobuf/go"
	"google.golang.org/grpc"
)

// peerAddMsg is an internal structure for use in the channel communicating
// to the peerAdder gorountine.
type peerAddMsg struct {
	msg  *pb.ServerMessage
	ping bool
}

// FederatedServer hold relevant information about peers that we known about.
type FederatedServer struct {
	Address string
	Port    string
	Ts      time.Time
}

// peerKey takes a ServerMessage object and returns the key that for that peer
// in our peer table.
func peerKey(msg *pb.ServerMessage) string {
	return msg.Address + ":" + msg.Port
}

// loadPeers takes the arguments given to the hub at startup and loads the
// previously known peers from disk and verifies their existence before
// storing them as known peers. Returns a map of peerKey -> object
func loadPeers(args *Args) map[string]*FederatedServer {
	localHosts := map[string]bool {
		"127.0.0.1": true,
		"0.0.0.0": true,
		"localhost": true,
	}
	servers := make(map[string]*FederatedServer)
	peerFile := args.PeerFile
	port := args.Port

	f, err := os.Open(peerFile)
	if err != nil {
		log.Println(err)
		return map[string]*FederatedServer{}
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
		log.Println(args)
		log.Println(ipPort)
		if ipPort[1] == port && localHosts[ipPort[0]] {
			log.Println("Self peer, skipping ...")
			continue
		}
		server := &FederatedServer{
			Address: ipPort[0],
			Port:    ipPort[1],
			Ts:      time.Now(),
		}
		log.Println("pinging peer", server)
		if helloPeer(server, args) {
			servers[line] = server
		}
	}

	log.Println("Returning from loadPeers")
	return servers
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

// helloPeer takes a peer to say hello to and sends a hello message
// containing all the peers we know about and information about us.
// This is used to confirm existence of peers on start and let them
// know about us. Returns true is call was successful, false otherwise.
func helloPeer(server *FederatedServer, args *Args) bool {
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
		return false
	}
	defer conn.Close()


	c := pb.NewHubClient(conn)

	msg := &pb.HelloMessage{
		Port: args.Port,
		Host: args.Host,
		Servers: []*pb.ServerMessage{},
	}
	res, err := c.Hello(ctx, msg)
	if err != nil {
		log.Println(err)
		return false
	}

	log.Println(res)

	return true
}

// writePeers writes our current known peers to disk.
func (s *Server) writePeers() {
	if !s.Args.WritePeers {
		return
	}
	failedCreat := "WARNING: Peer writer failed to create peer file, it's still running but may not be working!"
	failedWrite := "WARNING: Peer writer failed to write a line, it's still running but may not be working!"
	failedFlush := "WARNING: Peer writer failed to flush, it's still running but may not be working!"
	failedClose := "WARNING: Peer writer failed to close the peer file, it's still running but may not be working!"
	f, err := os.Create(s.Args.PeerFile)
	if err != nil {
		log.Println(failedCreat)
		log.Println(err)
	}
	writer := bufio.NewWriter(f)

	for _, peer := range s.Servers {
		line := peer.Address + ":" + peer.Port + "\n"
		_, err := writer.WriteString(line)
		if err != nil {
			log.Println(failedWrite)
			log.Println(err)
		}
	}

	err = writer.Flush()
	if err != nil {
		log.Println(failedFlush)
		log.Println(err)
	}
	err = f.Close()
	if err != nil {
		log.Println(failedClose)
		log.Println(err)
	}
}

// peerAdder is a goroutine which listens for new peers added and then
// optionally checks if they're online and adds them to our map of
// peers in a thread safe manner.
func (s *Server) peerAdder(ctx context.Context) {
	for {
		select {
			case chanMsg := <-s.peerChannel:
				msg := chanMsg.msg
				ping := chanMsg.ping

				k := msg.Address + ":" + msg.Port
				if _, ok := s.Servers[k]; !ok {
					newServer := &FederatedServer{
						Address: msg.Address,
						Port: msg.Port,
						Ts: time.Now(),
					}
					if !ping || helloPeer(newServer, s.Args) {
						s.Servers[k] = newServer
						s.writePeers()
						s.notifyPeerSubs(newServer)
					}
				} else {
					s.Servers[k].Ts = time.Now()
				}
			case <-ctx.Done():
				log.Println("context finished, peerAdder shutting down.")
				return
		}

	}
}

func (s *Server) notifyPeerSubs(newServer *FederatedServer) {
	var unsubscribe []string
	s.PeerSubs.Range(func(k, v interface{}) bool {
		key, ok := k.(string)
		if !ok {
			log.Println("Failed to cast subscriber key: ", v)
			return true
		}
		peer, ok := v.(*FederatedServer)
		if !ok {
			log.Println("Failed to cast subscriber value: ", v)
			return true
		}

		log.Printf("Notifying peer %s of new node %+v\n", key, newServer)
		err := notifyPeer(peer, newServer)
		if err != nil {
			log.Println("Failed to send data to ", key)
			log.Println(err)
			unsubscribe = append(unsubscribe, key)
		}
		return true
	})

	for _, key := range unsubscribe {
		s.PeerSubs.Delete(key)
	}
}

// addPeer is an internal function to add a peer to this hub.
func (s *Server) addPeer(msg *pb.ServerMessage, ping bool) {
	s.peerChannel <- &peerAddMsg{msg, ping}
}

// mergeFederatedServers is an internal convenience function to add a list of
// peers.
func (s *Server) mergeFederatedServers(servers []*pb.ServerMessage) {
	for _, srvMsg := range servers {
		s.peerChannel <- &peerAddMsg{srvMsg, false}
	}
}

// makeHelloMessage makes a message for this hub to call the Hello endpoint
// on another hub.
func (s *Server) makeHelloMessage() *pb.HelloMessage {
	n := len(s.Servers)
	servers := make([]*pb.ServerMessage, n)

	var i = 0
	for _, v := range s.Servers {
		servers[i] = &pb.ServerMessage{
			Address: v.Address,
			Port: v.Port,
		}
		i += 1
	}

	return &pb.HelloMessage{
		Port: s.Args.Port,
		Host: s.Args.Host,
		Servers: servers,
	}
}
