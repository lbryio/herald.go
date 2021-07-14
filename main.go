package main

import (
	"context"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"strings"
	"time"

	"github.com/akamensky/argparse"
	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/lbryio/hub/server"
	"github.com/lbryio/lbry.go/v2/extras/util"
	"google.golang.org/grpc"
	"google.golang.org/grpc/reflection"
)

const (
	defaultHost = "0.0.0.0"
	defaultPort = "50051"
	defaultEsHost = "http://localhost"
	defaultEsPort = "9200"
)


func GetEnvironment(data []string, getkeyval func(item string) (key, val string)) map[string]string {
	items := make(map[string]string)
	for _, item := range data {
		key, val := getkeyval(item)
		items[key] = val
	}
	return items
}

func GetEnvironmentStandard() map[string]string {
	return GetEnvironment(os.Environ(), func(item string) (key, val string) {
		splits := strings.Split(item, "=")
		key = splits[0]
		val = splits[1]
		return
	})
}

/*
func makeServeCmd(parser *argparse.Parser) *argparse.Command {
	serveCmd := parser.NewCommand("serve", "start the hub server")

	host := serveCmd.String("", "rpchost", &argparse.Options{Required: false, Help: "host", Default: defaultHost})
	port := serveCmd.String("", "rpcport", &argparse.Options{Required: false, Help: "port", Default: defaultPort})
	esHost := serveCmd.String("", "eshost", &argparse.Options{Required: false, Help: "host", Default: defaultEsHost})
	esPort := serveCmd.String("", "esport", &argparse.Options{Required: false, Help: "port", Default: defaultEsPort})
	dev := serveCmd.Flag("", "dev", &argparse.Options{Required: false, Help: "port", Default: false})

	return serveCmd
}
 */

func parseArgs(searchRequest *pb.SearchRequest, blockReq *pb.BlockRequest) *server.Args {

	environment := GetEnvironmentStandard()
	parser := argparse.NewParser("hub", "hub server and client")

	serveCmd := parser.NewCommand("serve", "start the hub server")
	searchCmd := parser.NewCommand("search", "claim search")
	getblockCmd := parser.NewCommand("getblock", "get block")
	getblockHeaderCmd := parser.NewCommand("getblockheader", "get block header")
	subscribeHeaderCmd := parser.NewCommand("subscribeheader", "get block header")

	host := parser.String("", "rpchost", &argparse.Options{Required: false, Help: "host", Default: defaultHost})
	port := parser.String("", "rpcport", &argparse.Options{Required: false, Help: "port", Default: defaultPort})
	esHost := parser.String("", "eshost", &argparse.Options{Required: false, Help: "host", Default: defaultEsHost})
	esPort := parser.String("", "esport", &argparse.Options{Required: false, Help: "port", Default: defaultEsPort})
	dev := parser.Flag("", "dev", &argparse.Options{Required: false, Help: "port", Default: false})

	text := parser.String("", "text", &argparse.Options{Required: false, Help: "text query"})
	name := parser.String("", "name", &argparse.Options{Required: false, Help: "name"})
	claimType := parser.String("", "claim_type", &argparse.Options{Required: false, Help: "claim_type"})
	id := parser.String("", "id", &argparse.Options{Required: false, Help: "id"})
	author := parser.String("", "author", &argparse.Options{Required: false, Help: "author"})
	title := parser.String("", "title", &argparse.Options{Required: false, Help: "title"})
	description := parser.String("", "description", &argparse.Options{Required: false, Help: "description"})
	channelId := parser.String("", "channel_id", &argparse.Options{Required: false, Help: "channel id"})
	channelIds := parser.StringList("", "channel_ids", &argparse.Options{Required: false, Help: "channel ids"})

	hash := parser.String("", "hash", &argparse.Options{Required: false, Help: "block hash"})

	// Now parse the arguments
	err := parser.Parse(os.Args)
	if err != nil {
		log.Fatalln(parser.Usage(err))
	}


	args := &server.Args{
		CmdType: server.SearchCmd,
		Host: *host,
		Port: ":" + *port,
		EsHost: *esHost,
		EsPort: *esPort,
		Dev: *dev,
	}

	if esHost, ok := environment["ELASTIC_HOST"]; ok {
		args.EsHost = esHost
	}

	if !strings.HasPrefix(args.EsHost, "http") {
		args.EsHost = "http://" + args.EsHost
	}

	if esPort, ok := environment["ELASTIC_PORT"]; ok {
		args.EsPort = esPort
	}

	/*
	Verify no invalid argument combinations
	 */
	if len(*channelIds) > 0 && *channelId != "" {
		log.Fatal("Cannot specify both channel_id and channel_ids")
	}

	if serveCmd.Happened() {
		args.CmdType = server.ServeCmd
	} else if searchCmd.Happened() {
		args.CmdType = server.SearchCmd
	} else if getblockCmd.Happened() {
		args.CmdType = server.GetblockCmd
		blockReq.Verbose = true
	} else if getblockHeaderCmd.Happened() {
		args.CmdType = server.GetblockHeaderCmd
		blockReq.Verbose = true
	} else if subscribeHeaderCmd.Happened() {
		args.CmdType = server.SubscribeHeaderCmd
		blockReq.Verbose = true
	}

	if *text != "" {
		searchRequest.Text = *text
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

	if *hash != "" {
		blockReq.Blockhash = *hash
	}

	return args
}

func main() {

	searchRequest := &pb.SearchRequest{}
	blockReq := &pb.BlockRequest{}

	args := parseArgs(searchRequest, blockReq)

	if args.CmdType == server.ServeCmd {

		l, err := net.Listen("tcp", args.Port)
		if err != nil {
			log.Fatalf("failed to listen: %v", err)
		}

		s := server.MakeHubServer(args)
		pb.RegisterHubServer(s.GrpcServer, s)
		reflection.Register(s.GrpcServer)

		log.Printf("listening on %s\n", l.Addr().String())
		log.Println(s.Args)
		if err := s.GrpcServer.Serve(l); err != nil {
			log.Fatalf("failed to serve: %v", err)
		}
		return
	}

	conn, err := grpc.Dial("localhost"+args.Port,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewHubClient(conn)

	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()

	log.Println(args)
	if args.CmdType == server.SearchCmd {
		r, err := c.Search(ctx, searchRequest)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("found %d results\n", r.GetTotal())

		for _, t := range r.Txos {
			fmt.Printf("%s:%d\n", util.TxHashToTxId(t.TxHash), t.Nout)
		}
	} else if args.CmdType == server.GetblockCmd {
		r, err := c.GetBlock(ctx, blockReq)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(r)
	} else if args.CmdType == server.GetblockHeaderCmd {
		r, err := c.GetBlockHeader(ctx, blockReq)
		if err != nil {
			log.Fatal(err)
		}
		log.Println(r)
	} else if args.CmdType == server.SubscribeHeaderCmd {
		ctx2, cancel2 := context.WithTimeout(context.Background(), time.Hour)
		defer cancel2()
		header, err := c.SubscribeHeaders(ctx2, blockReq)
		if err != nil {
			log.Fatal(err)
		}
		for {
			x, err := header.Recv()
			log.Println(x)
			if err == io.EOF {
				break
			}
			if err != nil {
				log.Fatalln(err)
			}
		}
	}
}
