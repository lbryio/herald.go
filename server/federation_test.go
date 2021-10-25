package server

import (
	"bufio"
	"context"
	"fmt"
	"log"
	"os"
	"strings"
	"testing"
	"time"

	pb "github.com/lbryio/hub/protobuf/go"
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

// TestPeerAdder tests the peer adder goroutine.
func TestPeerAdder(t *testing.T) {
	ctx := context.Background()
	args := &Args{
		CmdType:         ServeCmd,
		Host:            DefaultHost,
		Port:            DefaultPort,
		EsHost:          DefaultEsHost,
		EsPort:          DefaultEsPort,
		UDPPort:         DefaultUdpPort,
		DisableEs:       true,
		PrometheusPort:  DefaultPrometheusPort,
		EsIndex:         DefaultEsIndex,
		Debug:           true,
		RefreshDelta:    DefaultRefreshDelta,
		CacheTTL:        DefaultCacheTTL,
		PeerFile:        DefaultPeerFile,
		Country:         DefaultCountry,
		StartPeerAdder:  false,
		StartPrometheus: false,
		StartUDP:        false,
		WritePeers:      false,
	}

	tests := []struct {
		name string
		want int
	} {
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
		t.Run(tt.name, func(t *testing.T){
			server := MakeHubServer(ctx, args)
			ctxWCancel, cancel := context.WithCancel(ctx)

			go server.peerAdder(ctxWCancel)
			for i := 0; i < 10; i++ {
				var msg *peerAddMsg
				if strings.Contains(tt.name, "1 unique") {
					msg = &peerAddMsg{
						msg: &pb.ServerMessage{
							Address: "1.1.1.1",
							Port:    "50051",
						},
						ping: false,
					}
				} else {
					msg = &peerAddMsg{
						msg: &pb.ServerMessage{
							Address: fmt.Sprintf("%d.%d.%d.%d", i, i, i, i),
							Port:    "50051",
						},
						ping: false,
					}
				}
				server.peerChannel <- msg
			}
			// Have to give it a second to update peers since it's in
			// another thread.
			time.Sleep(time.Second)
			got := len(server.Servers)
			if got != tt.want {
				t.Errorf("len(server.Servers) = %d, want %d", got, tt.want)
			}
			cancel()
		})
	}

}

// TestPeerWriter tests that the peerAdder goroutine writes the peer file
// properly when set to do so.
func TestPeerWriter(t *testing.T) {
	ctx := context.Background()
	args := &Args{
		CmdType:         ServeCmd,
		Host:            DefaultHost,
		Port:            DefaultPort,
		EsHost:          DefaultEsHost,
		EsPort:          DefaultEsPort,
		UDPPort:         DefaultUdpPort,
		DisableEs:       true,
		PrometheusPort:  DefaultPrometheusPort,
		EsIndex:         DefaultEsIndex,
		Debug:           true,
		RefreshDelta:    DefaultRefreshDelta,
		CacheTTL:        DefaultCacheTTL,
		PeerFile:        DefaultPeerFile,
		Country:         DefaultCountry,
		StartPeerAdder:  false,
		StartPrometheus: false,
		StartUDP:        false,
		WritePeers:      true,
	}

	tests := []struct {
		name string
		want int
	} {
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
		t.Run(tt.name, func(t *testing.T){
			server := MakeHubServer(ctx, args)
			ctxWCancel, cancel := context.WithCancel(ctx)

			go server.peerAdder(ctxWCancel)
			for i := 0; i < 10; i++ {
				var msg *peerAddMsg
				if strings.Contains(tt.name, "1 unique") {
					msg = &peerAddMsg{
						msg: &pb.ServerMessage{
							Address: "1.1.1.1",
							Port:    "50051",
						},
						ping: false,
					}
				} else {
					msg = &peerAddMsg{
						msg: &pb.ServerMessage{
							Address: fmt.Sprintf("%d.%d.%d.%d", i, i, i, i),
							Port:    "50051",
						},
						ping: false,
					}
				}
				server.peerChannel <- msg
			}
			// Have to give it a second to update peers since it's in
			// another thread.
			time.Sleep(time.Second * 1)
			got := lineCountFile(server.Args.PeerFile)
			if got != tt.want {
				t.Errorf("len(server.Servers) = %d, want %d", got, tt.want)
			}
			cancel()
		})
	}

	removeFile(args.PeerFile)
}
