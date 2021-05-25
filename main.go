package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/lbryio/hub/server"

	"google.golang.org/grpc"
)

const (
	defaultPort = "50051"
	defaultRPCUser = "rpcuser"
	defaultRPCPassword = "rpcpassword"
)

type loginCreds struct {
	Username, Password string
}

func (c *loginCreds) GetRequestMetadata(context.Context, ...string) (map[string]string, error) {
	return map[string]string{
		"username": c.Username,
		"password": c.Password,
	}, nil
}

func (c *loginCreds) RequireTransportSecurity() bool {
	return false
}


func parseArgs(searchRequest *pb.SearchRequest) server.Args {
	query := flag.String("query", "", "query string")
	claimType := flag.String("claimType", "", "claim type")
	id := flag.String("id", "", "_id")
	author := flag.String("author", "", "author")
	title := flag.String("title", "", "title")
	channelName := flag.String("channelName", "", "channel name")
	description := flag.String("description", "", "description")

	port := flag.String("rpcport", defaultPort, "port")
	user := flag.String("rpcuser", defaultRPCUser, "username")
	pass := flag.String("rpcpassword", defaultRPCPassword, "password")

	flag.Parse()

	if *query != "" {
		searchRequest.Query = *query
	}
	if *claimType != "" {
		searchRequest.ClaimType = []string{*claimType}
	}
	if *id != "" {
		searchRequest.XId = [][]byte{[]byte(*id)}
	}
	if *author != "" {
		searchRequest.Author = []string{*author}
	}
	if *title != "" {
		searchRequest.Title = []string{*title}
	}
	if *channelName != "" {
		searchRequest.ChannelId = &pb.InvertibleField{Invert: false, Value: []string{*channelName}}
	}
	if *description != "" {
		searchRequest.Description = []string{*description}
	}


	return server.Args{Port: ":" + *port, User: *user, Pass: *pass}
}

func parseServerArgs() server.Args {
	port := flag.String("rpcport", defaultPort, "port")
	user := flag.String("rpcuser", defaultRPCUser, "username")
	pass := flag.String("rpcpassword", defaultRPCPassword, "password")

	flag.Parse()

	return server.Args{Port: ":" + *port, User: *user, Pass: *pass}
}

func main() {
	if len(os.Args) == 2 && os.Args[1] == "serve" {
		args := parseServerArgs()

		l, err := net.Listen("tcp", args.Port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		s := server.MakeHubServer(args)
		pb.RegisterHubServer(s, &server.Server{})

		log.Printf("listening on %s\n", l.Addr().String())
		if err := s.Serve(l); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		return
	}

	searchRequest := &pb.SearchRequest{}

	args := parseArgs(searchRequest)

	conn, err := grpc.Dial("localhost"+args.Port,
		grpc.WithInsecure(),
		//grpc.WithBlock(),
		grpc.WithPerRPCCredentials(&loginCreds{
			Username: args.User,
			Password: args.Pass,
		}),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewHubClient(conn)

	/*
	var query string
	if len(os.Args) > 1 {
		query = strings.Join(os.Args[1:], " ")
	} else {
		log.Printf("error: no search query provided\n")
		os.Exit(1)
	}
	 */

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()


	r, err := c.Search(ctx, searchRequest)
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("found %d results\n", r.GetTotal())

	for _, t := range r.Txos {
		fmt.Printf("%s:%d\n", server.FromHash(t.TxHash), t.Nout)
	}
}
