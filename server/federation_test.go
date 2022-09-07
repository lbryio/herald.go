package server_test

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/lbryio/herald.go/internal/metrics"
	pb "github.com/lbryio/herald.go/protobuf/go"
	server "github.com/lbryio/herald.go/server"
	dto "github.com/prometheus/client_model/go"
	"google.golang.org/grpc"
)

// lineCountFile takes a fileName and counts the number of lines in it.
func lineCountFile(fileName string) int {
	f, err := os.Open(fileName)
	defer f.Close()
	if err != nil {
		log.Println(err)
		return 0
	}
	scanner := bufio.NewScanner(f)
	scanner.Split(bufio.ScanLines)
	var lineCount = 0
	for scanner.Scan() {
		scanner.Text()
		lineCount = lineCount + 1
	}

	return lineCount
}

// removeFile removes a file.
func removeFile(fileName string) {
	err := os.Remove(fileName)
	if err != nil {
		log.Println(err)
	}
}

// makeDefaultArgs creates a default set of arguments for testing the server.
func makeDefaultArgs() *server.Args {
	args := &server.Args{
		CmdType:                     server.ServeCmd,
		Host:                        server.DefaultHost,
		Port:                        server.DefaultPort,
		DBPath:                      server.DefaultDBPath,
		EsHost:                      server.DefaultEsHost,
		EsPort:                      server.DefaultEsPort,
		PrometheusPort:              server.DefaultPrometheusPort,
		NotifierPort:                server.DefaultNotifierPort,
		JSONRPCPort:                 server.DefaultJSONRPCPort,
		EsIndex:                     server.DefaultEsIndex,
		RefreshDelta:                server.DefaultRefreshDelta,
		CacheTTL:                    server.DefaultCacheTTL,
		PeerFile:                    server.DefaultPeerFile,
		Country:                     server.DefaultCountry,
		DisableEs:                   true,
		Debug:                       true,
		DisableLoadPeers:            true,
		DisableStartPrometheus:      true,
		DisableStartUDP:             true,
		DisableWritePeers:           true,
		DisableRocksDBRefresh:       true,
		DisableResolve:              true,
		DisableBlockingAndFiltering: true,
		DisableStartNotifier:        true,
		DisableStartJSONRPC:         true,
	}

	return args
}

// TestAddPeer tests the ability to add peers
func TestAddPeer(t *testing.T) {
	ctx := context.Background()
	args := makeDefaultArgs()

	tests := []struct {
		name string
		want int
	}{
		{
			name: "Add 10 peers",
			want: 10,
		},
		{
			name: "Add 10 peers, 1 unique",
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hubServer := server.MakeHubServer(ctx, args)
			hubServer.ExternalIP = net.IPv4(0, 0, 0, 0)
			metrics.PeersKnown.Set(0)

			for i := 0; i < 10; i++ {
				var peer *server.Peer
				if strings.Contains(tt.name, "1 unique") {
					peer = &server.Peer{
						Address: "1.1.1.1",
						Port:    "50051",
					}
				} else {
					x := i + 1
					peer = &server.Peer{
						Address: fmt.Sprintf("%d.%d.%d.%d", x, x, x, x),
						Port:    "50051",
					}
				}
				//log.Printf("Adding peer %+v\n", msg)
				err := hubServer.AddPeerExported()(peer, false, false)
				if err != nil {
					log.Println(err)
				}
			}
			var m = &dto.Metric{}
			if err := metrics.PeersKnown.Write(m); err != nil {
				t.Errorf("Error getting metrics %+v\n", err)
			}
			got := int(*m.Gauge.Value)
			if got != tt.want {
				t.Errorf("len(server.PeerServers) = %d, want %d\n", got, tt.want)
			}
		})
	}

}

// TestPeerWriter tests that peers get written properly
func TestPeerWriter(t *testing.T) {
	ctx := context.Background()
	args := makeDefaultArgs()
	args.DisableWritePeers = false

	tests := []struct {
		name string
		want int
	}{
		{
			name: "Add 10 peers",
			want: 10,
		},
		{
			name: "Add 10 peers, 1 unique",
			want: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hubServer := server.MakeHubServer(ctx, args)
			hubServer.ExternalIP = net.IPv4(0, 0, 0, 0)

			for i := 0; i < 10; i++ {
				var peer *server.Peer
				if strings.Contains(tt.name, "1 unique") {
					peer = &server.Peer{
						Address: "1.1.1.1",
						Port:    "50051",
					}
				} else {
					x := i + 1
					peer = &server.Peer{
						Address: fmt.Sprintf("%d.%d.%d.%d", x, x, x, x),
						Port:    "50051",
					}
				}
				//log.Printf("Adding peer %+v\n", peer)
				err := hubServer.AddPeerExported()(peer, false, false)
				if err != nil {
					log.Println(err)
				}
			}
			//log.Println("Counting lines...")
			got := lineCountFile(hubServer.Args.PeerFile)
			if got != tt.want {
				t.Errorf("lineCountFile(peers.txt) = %d, want %d", got, tt.want)
			}
		})
	}

	removeFile(args.PeerFile)
}

// TestAddPeerEndpoint tests the ability to add peers
func TestAddPeerEndpoint(t *testing.T) {
	ctx := context.Background()
	args := makeDefaultArgs()
	args2 := makeDefaultArgs()
	args2.Port = "50052"

	tests := []struct {
		name          string
		wantServerOne int64
		wantServerTwo int64
	}{
		{
			// outside -> server1.AddPeer(server2, ping=true)  : server1 = 1, server2 = 0
			// server1 -> server2.Hello(server1)               : server1 = 1, server2 = 0
			// server2 -> server2.addPeer(server1, ping=false) : server1 = 1, server2 = 1
			// server2 -> server1.PeerSubscribe(server2)       : server1 = 1, server2 = 1
			// server1 <- server2.makeHelloMessage()           : server1 = 1, server2 = 1
			// server1.notifyPeer()                            : server1 = 1, server2 = 1
			// server1 -> server2.AddPeer(server2)             : server1 = 1, server2 = 1
			// server2 self peer, skipping                     : server1 = 1, server2 = 1
			// server1 -> server2.PeerSubscribe(server1)       : server1 = 1, server2 = 1
			name:          "Add 1 peer",
			wantServerOne: 1,
			wantServerTwo: 1,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hubServer := server.MakeHubServer(ctx, args)
			hubServer2 := server.MakeHubServer(ctx, args2)
			metrics.PeersKnown.Set(0)
			go hubServer.Run()
			go hubServer2.Run()
			//go hubServer.Run()
			conn, err := grpc.Dial("localhost:"+args.Port,
				grpc.WithInsecure(),
				grpc.WithBlock(),
			)
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}

			c := pb.NewHubClient(conn)

			msg := &pb.ServerMessage{
				Address: "0.0.0.0",
				Port:    "50052",
			}

			_, err = c.AddPeer(context.Background(), msg)
			if err != nil {
				log.Println(err)
			}

			hubServer.GrpcServer.GracefulStop()
			hubServer2.GrpcServer.GracefulStop()
			got1 := hubServer.GetNumPeersExported()()
			got2 := hubServer2.GetNumPeersExported()()
			if got1 != tt.wantServerOne {
				t.Errorf("len(hubServer.PeerServers) = %d, want %d\n", got1, tt.wantServerOne)
			}
			if got2 != tt.wantServerTwo {
				t.Errorf("len(hubServer2.PeerServers) = %d, want %d\n", got2, tt.wantServerTwo)
			}
		})
	}

}

// TestAddPeerEndpoint2 tests the ability to add peers
func TestAddPeerEndpoint2(t *testing.T) {
	ctx := context.Background()
	args := makeDefaultArgs()
	args2 := makeDefaultArgs()
	args3 := makeDefaultArgs()
	args2.Port = "50052"
	args3.Port = "50053"

	tests := []struct {
		name            string
		wantServerOne   int64
		wantServerTwo   int64
		wantServerThree int64
	}{
		{
			name:            "Add 2 peers",
			wantServerOne:   2,
			wantServerTwo:   2,
			wantServerThree: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hubServer := server.MakeHubServer(ctx, args)
			hubServer2 := server.MakeHubServer(ctx, args2)
			hubServer3 := server.MakeHubServer(ctx, args3)
			metrics.PeersKnown.Set(0)
			go hubServer.Run()
			go hubServer2.Run()
			go hubServer3.Run()
			conn, err := grpc.Dial("localhost:"+args.Port,
				grpc.WithInsecure(),
				grpc.WithBlock(),
			)
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}

			c := pb.NewHubClient(conn)

			msg := &pb.ServerMessage{
				Address: "0.0.0.0",
				Port:    "50052",
			}

			msg2 := &pb.ServerMessage{
				Address: "0.0.0.0",
				Port:    "50053",
			}

			_, err = c.AddPeer(context.Background(), msg)
			if err != nil {
				log.Println(err)
			}
			_, err = c.AddPeer(context.Background(), msg2)
			if err != nil {
				log.Println(err)
			}

			hubServer.GrpcServer.GracefulStop()
			hubServer2.GrpcServer.GracefulStop()
			hubServer3.GrpcServer.GracefulStop()
			got1 := hubServer.GetNumPeersExported()()
			got2 := hubServer2.GetNumPeersExported()()
			got3 := hubServer3.GetNumPeersExported()()
			if got1 != tt.wantServerOne {
				t.Errorf("len(hubServer.PeerServers) = %d, want %d\n", got1, tt.wantServerOne)
			}
			if got2 != tt.wantServerTwo {
				t.Errorf("len(hubServer2.PeerServers) = %d, want %d\n", got2, tt.wantServerTwo)
			}
			if got3 != tt.wantServerThree {
				t.Errorf("len(hubServer3.PeerServers) = %d, want %d\n", got3, tt.wantServerThree)
			}
		})
	}

}

// TestAddPeerEndpoint3 tests the ability to add peers
func TestAddPeerEndpoint3(t *testing.T) {
	ctx := context.Background()
	args := makeDefaultArgs()
	args2 := makeDefaultArgs()
	args3 := makeDefaultArgs()
	args2.Port = "50052"
	args3.Port = "50053"

	tests := []struct {
		name            string
		wantServerOne   int64
		wantServerTwo   int64
		wantServerThree int64
	}{
		{
			name:            "Add 1 peer to each",
			wantServerOne:   2,
			wantServerTwo:   2,
			wantServerThree: 2,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hubServer := server.MakeHubServer(ctx, args)
			hubServer2 := server.MakeHubServer(ctx, args2)
			hubServer3 := server.MakeHubServer(ctx, args3)
			metrics.PeersKnown.Set(0)
			go hubServer.Run()
			go hubServer2.Run()
			go hubServer3.Run()
			conn, err := grpc.Dial("localhost:"+args.Port,
				grpc.WithInsecure(),
				grpc.WithBlock(),
			)
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}
			conn2, err := grpc.Dial("localhost:50052",
				grpc.WithInsecure(),
				grpc.WithBlock(),
			)
			if err != nil {
				log.Fatalf("did not connect: %v", err)
			}

			c := pb.NewHubClient(conn)
			c2 := pb.NewHubClient(conn2)

			msg := &pb.ServerMessage{
				Address: "0.0.0.0",
				Port:    "50052",
			}

			msg2 := &pb.ServerMessage{
				Address: "0.0.0.0",
				Port:    "50053",
			}

			_, err = c.AddPeer(context.Background(), msg)
			if err != nil {
				log.Println(err)
			}
			_, err = c2.AddPeer(context.Background(), msg2)
			if err != nil {
				log.Println(err)
			}

			hubServer.GrpcServer.GracefulStop()
			hubServer2.GrpcServer.GracefulStop()
			hubServer3.GrpcServer.GracefulStop()
			got1 := hubServer.GetNumPeersExported()()
			got2 := hubServer2.GetNumPeersExported()()
			got3 := hubServer3.GetNumPeersExported()()
			if got1 != tt.wantServerOne {
				t.Errorf("len(hubServer.PeerServers) = %d, want %d\n", got1, tt.wantServerOne)
			}
			if got2 != tt.wantServerTwo {
				t.Errorf("len(hubServer2.PeerServers) = %d, want %d\n", got2, tt.wantServerTwo)
			}
			if got3 != tt.wantServerThree {
				t.Errorf("len(hubServer3.PeerServers) = %d, want %d\n", got3, tt.wantServerThree)
			}
		})
	}

}

// TestAddPeer tests the ability to add peers
func TestUDPServer(t *testing.T) {
	ctx := context.Background()
	args := makeDefaultArgs()
	args.DisableStartUDP = false
	args2 := makeDefaultArgs()
	args2.Port = "50052"
	args2.DisableStartUDP = false

	tests := []struct {
		name string
		want string
	}{
		{
			name: "hubs hubServer external ip",
			want: "127.0.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			hubServer := server.MakeHubServer(ctx, args)
			hubServer2 := server.MakeHubServer(ctx, args2)
			go hubServer.Run()
			go hubServer2.Run()
			metrics.PeersKnown.Set(0)

			peer := &server.Peer{
				Address: "0.0.0.0",
				Port:    "50052",
			}

			err := hubServer.AddPeerExported()(peer, true, true)
			if err != nil {
				log.Println(err)
			}

			hubServer.GrpcServer.GracefulStop()
			hubServer2.GrpcServer.GracefulStop()

			got1 := hubServer.ExternalIP.String()
			if got1 != tt.want {
				t.Errorf("hubServer.ExternalIP = %s, want %s\n", got1, tt.want)
				t.Errorf("hubServer.Args.Port = %s\n", hubServer.Args.Port)
			}
			got2 := hubServer2.ExternalIP.String()
			if got2 != tt.want {
				t.Errorf("hubServer2.ExternalIP = %s, want %s\n", got2, tt.want)
				t.Errorf("hubServer2.Args.Port = %s\n", hubServer2.Args.Port)
			}
		})
	}

}
