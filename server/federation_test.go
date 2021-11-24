package server

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"
	"testing"

	"github.com/lbryio/hub/internal/metrics"
	pb "github.com/lbryio/hub/protobuf/go"
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

func makeDefaultArgs() *Args {
	args := &Args{
		CmdType:                ServeCmd,
		Host:                   DefaultHost,
		Port:                   DefaultPort,
		EsHost:                 DefaultEsHost,
		EsPort:                 DefaultEsPort,
		PrometheusPort:         DefaultPrometheusPort,
		EsIndex:                DefaultEsIndex,
		RefreshDelta:           DefaultRefreshDelta,
		CacheTTL:               DefaultCacheTTL,
		PeerFile:               DefaultPeerFile,
		Country:                DefaultCountry,
		DisableEs:              true,
		Debug:                  true,
		DisableLoadPeers:       true,
		DisableStartPrometheus: true,
		DisableStartUDP:        true,
		DisableWritePeers:      true,
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
			server := MakeHubServer(ctx, args)
			server.ExternalIP = net.IPv4(0, 0, 0, 0)
			metrics.PeersKnown.Set(0)

			for i := 0; i < 10; i++ {
				var msg *pb.ServerMessage
				if strings.Contains(tt.name, "1 unique") {
					msg = &pb.ServerMessage{
						Address: "1.1.1.1",
						Port:    "50051",
					}
				} else {
					x := i + 1
					msg = &pb.ServerMessage{
						Address: fmt.Sprintf("%d.%d.%d.%d", x, x, x, x),
						Port:    "50051",
					}
				}
				//log.Printf("Adding peer %+v\n", msg)
				err := server.addPeer(msg, false, false)
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
			server := MakeHubServer(ctx, args)
			server.ExternalIP = net.IPv4(0, 0, 0, 0)

			for i := 0; i < 10; i++ {
				var msg *pb.ServerMessage
				if strings.Contains(tt.name, "1 unique") {
					msg = &pb.ServerMessage{
						Address: "1.1.1.1",
						Port:    "50051",
					}
				} else {
					x := i + 1
					msg = &pb.ServerMessage{
						Address: fmt.Sprintf("%d.%d.%d.%d", x, x, x, x),
						Port:    "50051",
					}
				}
				//log.Printf("Adding peer %+v\n", msg)
				err := server.addPeer(msg, false, false)
				if err != nil {
					log.Println(err)
				}
			}
			//log.Println("Counting lines...")
			got := lineCountFile(server.Args.PeerFile)
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
			server := MakeHubServer(ctx, args)
			server2 := MakeHubServer(ctx, args2)
			metrics.PeersKnown.Set(0)
			go server.Run()
			go server2.Run()
			//go server.Run()
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

			server.GrpcServer.GracefulStop()
			server2.GrpcServer.GracefulStop()
			got1 := server.getNumPeers()
			got2 := server2.getNumPeers()
			if got1 != tt.wantServerOne {
				t.Errorf("len(server.PeerServers) = %d, want %d\n", got1, tt.wantServerOne)
			}
			if got2 != tt.wantServerTwo {
				t.Errorf("len(server2.PeerServers) = %d, want %d\n", got2, tt.wantServerTwo)
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
			server := MakeHubServer(ctx, args)
			server2 := MakeHubServer(ctx, args2)
			server3 := MakeHubServer(ctx, args3)
			metrics.PeersKnown.Set(0)
			go server.Run()
			go server2.Run()
			go server3.Run()
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

			server.GrpcServer.GracefulStop()
			server2.GrpcServer.GracefulStop()
			server3.GrpcServer.GracefulStop()
			got1 := server.getNumPeers()
			got2 := server2.getNumPeers()
			got3 := server3.getNumPeers()
			if got1 != tt.wantServerOne {
				t.Errorf("len(server.PeerServers) = %d, want %d\n", got1, tt.wantServerOne)
			}
			if got2 != tt.wantServerTwo {
				t.Errorf("len(server2.PeerServers) = %d, want %d\n", got2, tt.wantServerTwo)
			}
			if got3 != tt.wantServerThree {
				t.Errorf("len(server3.PeerServers) = %d, want %d\n", got3, tt.wantServerThree)
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
			server := MakeHubServer(ctx, args)
			server2 := MakeHubServer(ctx, args2)
			server3 := MakeHubServer(ctx, args3)
			metrics.PeersKnown.Set(0)
			go server.Run()
			go server2.Run()
			go server3.Run()
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

			server.GrpcServer.GracefulStop()
			server2.GrpcServer.GracefulStop()
			server3.GrpcServer.GracefulStop()
			got1 := server.getNumPeers()
			got2 := server2.getNumPeers()
			got3 := server3.getNumPeers()
			if got1 != tt.wantServerOne {
				t.Errorf("len(server.PeerServers) = %d, want %d\n", got1, tt.wantServerOne)
			}
			if got2 != tt.wantServerTwo {
				t.Errorf("len(server2.PeerServers) = %d, want %d\n", got2, tt.wantServerTwo)
			}
			if got3 != tt.wantServerThree {
				t.Errorf("len(server3.PeerServers) = %d, want %d\n", got3, tt.wantServerThree)
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
			name: "hubs server external ip",
			want: "127.0.0.1",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			server := MakeHubServer(ctx, args)
			server2 := MakeHubServer(ctx, args2)
			go server.Run()
			go server2.Run()
			metrics.PeersKnown.Set(0)

			msg := &pb.ServerMessage{
				Address: "0.0.0.0",
				Port:    "50052",
			}

			err := server.addPeer(msg, true, true)
			if err != nil {
				log.Println(err)
			}

			server.GrpcServer.GracefulStop()
			server2.GrpcServer.GracefulStop()

			got1 := server.ExternalIP.String()
			if got1 != tt.want {
				t.Errorf("server.ExternalIP = %s, want %s\n", got1, tt.want)
				t.Errorf("server.Args.Port = %s\n", server.Args.Port)
			}
			got2 := server2.ExternalIP.String()
			if got2 != tt.want {
				t.Errorf("server2.ExternalIP = %s, want %s\n", got2, tt.want)
				t.Errorf("server2.Args.Port = %s\n", server2.Args.Port)
			}
		})
	}

}
