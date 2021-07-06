package server

import (
	"context"
	"encoding/base64"
	pb "github.com/lbryio/hub/protobuf/go"
	"log"

	//"net/rpc/jsonrpc"
	"github.com/ybbus/jsonrpc/v2"
)

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
	res, err := rpcClient.Call("getblock", blockReq.Blockhash)
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