package main

import (
	"bytes"
	"context"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/lbryio/hub/db"
	"github.com/lbryio/hub/db/prefixes"
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

		interrupt := make(chan os.Signal, 1)
		signal.Notify(interrupt, os.Interrupt, syscall.SIGTERM)
		defer signal.Stop(interrupt)

		s := server.MakeHubServer(ctxWCancel, args)
		s.Run()

		select {
		case <-interrupt:
			break
		case <-ctx.Done():
			break
		}

		log.Println("Shutting down server...")

		s.EsClient.Stop()
		s.GrpcServer.GracefulStop()

		log.Println("Returning from main...")

		return
	} else if args.CmdType == server.DBCmd {
		options := &db.IterOptions{
			FillCache:    false,
			Prefix:       []byte{prefixes.SupportAmount},
			Start:        nil,
			Stop:         nil,
			IncludeStart: true,
			IncludeStop:  false,
			IncludeKey:   true,
			IncludeValue: true,
			RawKey:       true,
			RawValue:     true,
		}

		dbVal, err := db.GetDB("/mnt/d/data/wallet/lbry-rocksdb/")
		if err != nil {
			log.Fatalln(err)
		}

		db.ReadWriteRawN(dbVal, options, "./testdata/support_amount.csv", 10)

		return
	} else if args.CmdType == server.DBCmd2 {
		pxs := prefixes.GetPrefixes()
		for _, prefix := range pxs {
			//var rawPrefix byte = prefixes.ClaimExpiration

			//prefix := []byte{rawPrefix}
			columnFamily := string(prefix)
			options := &db.IterOptions{
				FillCache:    false,
				Prefix:       prefix,
				Start:        nil,
				Stop:         nil,
				IncludeStart: true,
				IncludeStop:  false,
				IncludeKey:   true,
				IncludeValue: true,
				RawKey:       true,
				RawValue:     true,
			}

			dbVal, handles, err := db.GetDBCF("/mnt/d/data/snapshot_1072108/lbry-rocksdb/", columnFamily)
			if err != nil {
				log.Fatalln(err)
			}

			options.CfHandle = handles[1]
			var n = 10
			if bytes.Equal(prefix, []byte{prefixes.Undo}) || bytes.Equal(prefix, []byte{prefixes.DBState}) {
				n = 1
			}

			db.ReadWriteRawNCF(dbVal, options, fmt.Sprintf("./testdata/%s.csv", columnFamily), n)
		}

		return
	} else if args.CmdType == server.DBCmd3 {
		channelHash, _ := hex.DecodeString("2556ed1cab9d17f2a9392030a9ad7f5d138f11bd")
		// name := util.NormalizeName("@Styxhexenhammer666")
		// txNum := uint32(0x6284e3)
		// position := uint16(0x0)
		// typ := uint8(prefixes.ACTIVATED_CLAIM_TXO_TYPE)
		var rawPrefix byte = prefixes.SupportAmount
		var startRaw []byte = nil
		prefix := []byte{rawPrefix}
		columnFamily := string(prefix)
		// start := prefixes.NewClaimTakeoverKey(name)
		start := prefixes.NewSupportAmountKey(channelHash)
		startRaw = start.PackKey()
		// start := &prefixes.ChannelCountKey{
		// 	Prefix:      prefix,
		// 	ChannelHash: channelHash,
		// }
		// startRaw := prefixes.ChannelCountKeyPackPartial(start, 1)
		// startRaw := start.PackKey()
		options := &db.IterOptions{
			FillCache:    false,
			Prefix:       prefix,
			Start:        startRaw,
			Stop:         nil,
			IncludeStart: true,
			IncludeStop:  false,
			IncludeKey:   true,
			IncludeValue: true,
			RawKey:       true,
			RawValue:     true,
		}

		dbVal, handles, err := db.GetDBCF("/mnt/d/data/snapshot_1072108/lbry-rocksdb/", columnFamily)
		if err != nil {
			log.Fatalln(err)
		}

		options.CfHandle = handles[1]

		db.ReadWriteRawNColumnFamilies(dbVal, options, fmt.Sprintf("./testdata/%s_resolve.csv", columnFamily), 1)
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
