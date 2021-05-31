package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"time"

	"github.com/akamensky/argparse"
	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/lbryio/hub/server"

	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
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
	parser := argparse.NewParser("hub", "hub server and client")

	serveCmd := parser.NewCommand("serve", "start the hub server")

	port := parser.String("", "rpcport", &argparse.Options{Required: false, Help: "port", Default: defaultPort})
	user := parser.String("", "rpcuser", &argparse.Options{Required: false, Help: "user", Default: defaultRPCUser})
	pass := parser.String("", "rpcpassword", &argparse.Options{Required: false, Help: "password", Default: defaultRPCPassword})

	query := parser.String("", "query", &argparse.Options{Required: false, Help: "text query"})
	name := parser.String("", "name", &argparse.Options{Required: false, Help: "name"})
	claimType := parser.String("", "claim_type", &argparse.Options{Required: false, Help: "claim_type"})
	id := parser.String("", "id", &argparse.Options{Required: false, Help: "id"})
	author := parser.String("", "author", &argparse.Options{Required: false, Help: "author"})
	title := parser.String("", "title", &argparse.Options{Required: false, Help: "title"})
	description := parser.String("", "description", &argparse.Options{Required: false, Help: "description"})
	channelId := parser.String("", "channel_id", &argparse.Options{Required: false, Help: "channel id"})
	channelIds := parser.StringList("", "channel_ids", &argparse.Options{Required: false, Help: "channel ids"})

	// Now parse the arguments
	err := parser.Parse(os.Args)
	if err != nil {
		log.Fatalln(parser.Usage(err))
	}

	args := server.Args{Serve: false, Port: ":" + *port, User: *user, Pass: *pass}

	/*
	Verify no invalid argument combinations
	 */
	if len(*channelIds) > 0 && *channelId != "" {
		log.Fatal("Cannot specify both channel_id and channel_ids")
	}

	if serveCmd.Happened() {
		args.Serve = true
	}

	if *query != "" {
		searchRequest.Query = *query
	}
	if *name!= "" {
		searchRequest.Name = []string{*name}
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
	if *description != "" {
		searchRequest.Description = []string{*description}
	}
	if *channelId != "" {
		searchRequest.ChannelId = &pb.InvertibleField{Invert: false, Value: []string{*channelId}}
	}
	if len(*channelIds) > 0 {
		searchRequest.ChannelId = &pb.InvertibleField{Invert: false, Value: *channelIds}
	}

	return args
}

func main() {
	searchRequest := &pb.SearchRequest{}

	args := parseArgs(searchRequest)

	if args.Serve {

		l, err := net.Listen("tcp", args.Port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		s := server.MakeHubServer(args)
		pb.RegisterHubServer(s, &server.Server{})
		reflection.Register(s)

		log.Printf("listening on %s\n", l.Addr().String())
		if err := s.Serve(l); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		return
	}

	conn, err := grpc.Dial("localhost"+args.Port,
		grpc.WithInsecure(),
		grpc.WithBlock(),
		//grpc.WithPerRPCCredentials(&loginCreds{
		//	Username: args.User,
		//	Password: args.Pass,
		//}),
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
