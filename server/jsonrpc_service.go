package server

import (
	"log"
	"net/http"

	"github.com/gorilla/mux"
	"github.com/gorilla/rpc"
	"github.com/gorilla/rpc/json"
	"github.com/lbryio/herald.go/db"
	pb "github.com/lbryio/herald.go/protobuf/go"
)

type ClaimtrieService struct {
	DB *db.ReadOnlyDBColumnFamily
}

type ResolveData struct {
	Data []string `json:"data"`
}

type Result struct {
	Data string `json:"data"`
}

// Resolve is the json rpc endpoint for resolve
func (t *ClaimtrieService) Resolve(r *http.Request, args *ResolveData, result **pb.Outputs) error {
	log.Println("Resolve")
	res, err := InternalResolve(args.Data, t.DB)
	*result = res
	return err
}

// StartJsonRPC starts the json rpc server and registers the endpoints.
func (s *Server) StartJsonRPC() error {
	port := ":" + s.Args.JSONRPCPort

	s1 := rpc.NewServer() // Create a new RPC server
	// Register the type of data requested as JSON, with custom codec.
	s1.RegisterCodec(&BlockchainCodec{json.NewCodec()}, "application/json")

	// Register "blockchain.claimtrie.*"" handlers.
	claimtrieSvc := &ClaimtrieService{s.DB}
	s1.RegisterService(claimtrieSvc, "blockchain_claimtrie")

	// Register other "blockchain.{block,address,scripthash}.*" handlers.
	blockchainSvc := &BlockchainService{s.DB, s.Chain}
	s1.RegisterService(&blockchainSvc, "blockchain_block")
	s1.RegisterService(&BlockchainAddressService{*blockchainSvc}, "blockchain_address")
	s1.RegisterService(&BlockchainScripthashService{*blockchainSvc}, "blockchain_scripthash")

	r := mux.NewRouter()
	r.Handle("/rpc", s1)
	log.Fatal(http.ListenAndServe(port, r))

	return nil
}
