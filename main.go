package main

import (
	"context"
	"fmt"
	"time"

	_ "net/http/pprof"

	"github.com/lbryio/herald.go/internal"
	pb "github.com/lbryio/herald.go/protobuf/go"
	"github.com/lbryio/herald.go/server"
	"github.com/lbryio/lbry.go/v3/extras/stop"
	log "github.com/sirupsen/logrus"
	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

func main() {

	log.SetFormatter(&log.TextFormatter{
		FullTimestamp: true,
	})

	ctx := context.Background()
	searchRequest := &pb.SearchRequest{}

	args := server.ParseArgs(searchRequest)

	if args.CmdType == server.ServeCmd {
		// This will cancel goroutines with the server finishes.
		stopGroup := stop.New()

		initsignals()
		interrupt := interruptListener()

		s := server.MakeHubServer(stopGroup, args)
		go s.Run()

		defer s.Stop()

		<-interrupt
		return
	}

	conn, err := grpc.Dial("localhost:"+string(args.Port),
		grpc.WithTransportCredentials(insecure.NewCredentials()),
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
			fmt.Printf("%s:%d\n", internal.TxHashToTxId(t.TxHash), t.Nout)
		}
	default:
		log.Fatalln("Unknown Command Type!")
	}
}
