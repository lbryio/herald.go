package main

import (
	"context"
	"fmt"
	"log"
	"time"

	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/lbryio/hub/server"
	"github.com/lbryio/lbry.go/v2/extras/util"
	"google.golang.org/grpc"
)

func main() {

	ctx := context.Background()
	searchRequest := &pb.SearchRequest{}

	args := server.ParseArgs(searchRequest)

	if args.CmdType == server.ServeCmd {
		// This will cancel goroutines with the server finishes.
		ctxWCancel, cancel := context.WithCancel(ctx)
		defer cancel()

		s := server.MakeHubServer(ctxWCancel, args)
		s.Run()
		//l, err := net.Listen("tcp", ":"+args.Port)
		//if err != nil {
		//	log.Fatalf("failed to listen: %v", err)
		//}
		//
		//pb.RegisterHubServer(s.GrpcServer, s)
		//reflection.Register(s.GrpcServer)
		//
		//log.Printf("listening on %s\n", l.Addr().String())
		//log.Println(s.Args)
		//if err := s.GrpcServer.Serve(l); err != nil {
		//	log.Fatalf("failed to serve: %v", err)
		//}
		return
	}

	conn, err := grpc.Dial("localhost:"+args.Port,
		grpc.WithInsecure(),
		grpc.WithBlock(),
	)
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	c := pb.NewHubClient(conn)

	ctxWTimeout, cancelQuery := context.WithTimeout(ctx, time.Second)
	defer cancelQuery()

	log.Println(args)
	switch args.CmdType {
	case server.SearchCmd:
		r, err := c.Search(ctxWTimeout, searchRequest)
		if err != nil {
			log.Fatal(err)
		}

		log.Printf("found %d results\n", r.GetTotal())

		for _, t := range r.Txos {
			fmt.Printf("%s:%d\n", util.TxHashToTxId(t.TxHash), t.Nout)
		}
	default:
		log.Fatalln("Unknown Command Type!")
	}
}
