package server

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	pb "github.com/lbryio/hub/protobuf/go"
	"log"
	"time"

	zmq "github.com/go-zeromq/zmq4"
	//"net/rpc/jsonrpc"
	"github.com/ybbus/jsonrpc/v2"
)

func (s *Server) SubscribeHeaders(request *pb.BlockRequest, stream pb.Hub_SubscribeHeadersServer) error {
	//log.SetPrefix("psenvsub: ")

	//  Prepare our subscriber
	log.Println("asdf")
	sub := zmq.NewSub(context.Background(), zmq.WithDialerRetry(time.Second * 60), zmq.WithDialerTimeout(time.Second * 30))
	defer sub.Close()

	err := sub.Dial("tcp://localhost:28333")
	if err != nil {
		log.Fatalf("could not dial: %v", err)
	}

	err = sub.SetOption(zmq.OptionSubscribe, "hashblockheader")
	//err = sub.SetOption(zmq.OptionSubscribe, "hashblock")
	if err != nil {
		//log.Fatalf("could not subscribe: %v", err)
		return err
	}

	for {
		// Read envelope
		msg, err := sub.Recv()
		if err != nil {
			//log.Fatalf("could not receive message: %v", err)
			return err
		}
		hash := hex.EncodeToString(msg.Frames[1][0:32])
		height := binary.LittleEndian.Uint32(msg.Frames[1][32:])
		log.Printf("[%s] %s\n", msg.Frames[0], hash)
		stream.Send(&pb.BlockHeaderOutput{Hash: hash, Height: int64(height)})
	}
}
func (s *Server) GetBlock(ctx context.Context, blockReq *pb.BlockRequest) (*pb.BlockOutput, error) {

	log.Println("In GetBlock")
	rpcClient := jsonrpc.NewClientWithOpts("http://localhost"+":29245", &jsonrpc.RPCClientOpts{
		CustomHeaders: map[string]string{
			"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte("lbry"+":"+"lbry")),
		},
	})

	log.Println("Making call ...")
	var r pb.BlockOutput
	res, err := rpcClient.Call("getblock", blockReq.Blockhash)
	if err != nil {
		log.Println(err)
		return &pb.BlockOutput{Hash: "", Confirmations: 0}, err
	}
	log.Println(res)
	err = res.GetObject(&r)
	if err != nil {
		log.Println(err)
		return &pb.BlockOutput{Hash: "", Confirmations: 0}, err
	}

	return &r, nil
}

func (s *Server) GetBlockHeader(ctx context.Context, blockReq *pb.BlockRequest) (*pb.BlockHeaderOutput, error) {

	log.Println("In GetBlock")
	rpcClient := jsonrpc.NewClientWithOpts("http://localhost"+":29245", &jsonrpc.RPCClientOpts{
		CustomHeaders: map[string]string{
			"Authorization": "Basic " + base64.StdEncoding.EncodeToString([]byte("lbry"+":"+"lbry")),
		},
	})

	log.Println("Making call ...")
	var r pb.BlockHeaderOutput
	res, err := rpcClient.Call("getblockheader", blockReq.Blockhash)
	if err != nil {
		log.Println(err)
		return &pb.BlockHeaderOutput{Hash: "", Confirmations: 0}, err
	}
	log.Println(res)
	err = res.GetObject(&r)
	if err != nil {
		log.Println(err)
		return &pb.BlockHeaderOutput{Hash: "", Confirmations: 0}, err
	}

	return &r, nil
}
/*
func (s *Server) GetBlock(ctx context.Context, blockReq *pb.BlockRequest) (*pb.BlockOutput, error) {

	log.Println("In GetBlock")
	conn, err := jsonrpc.Dial("tcp", "localhost"+":19245")
	if err != nil {
		log.Fatalf("did not connect: %v", err)
	}
	defer conn.Close()

	log.Println("Making call ...")
	var r pb.BlockOutput
	//var rr json.RawMessage
	err = conn.Call("getblock", blockReq.Blockhash, nil)
	if err != nil {
		log.Println(err)
		return &pb.BlockOutput{Hash: "", Confirmations: 0}, err
	}

	return &r, nil
}

 */