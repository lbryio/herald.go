package main

import (
	"context"
	"log"
	"net"
	"os"
	"time"

	pb "github.com/lbryio/hub/protobuf/go"
	"google.golang.org/grpc"
)

const (
	port = ":50051"
)

func main() {
	if len(os.Args) > 1 && os.Args[1] == "serve" {
		l, err := net.Listen("tcp", port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		s := grpc.NewServer()
		pb.RegisterHubServer(s, &server{})

		log.Printf("listening on %s\n", l.Addr().String())
		if err := s.Serve(l); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		return
	}

	conn, err := grpc.Dial("localhost"+port, grpc.WithInsecure(), grpc.WithBlock())
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewHubClient(conn)

	// Contact the server and print out its response.
	query := "what is lbry"
	if len(os.Args) > 1 {
		query = os.Args[1]
	}

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	r, err := c.Search(ctx, &pb.SearchRequest{Query: query})
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("found %d results\n", r.GetTotal())
}
