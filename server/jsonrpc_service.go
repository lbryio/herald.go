package server

import (
	"net/http"

	"github.com/gorilla/mux"
	"github.com/gorilla/rpc"
	"github.com/gorilla/rpc/json"
	"github.com/lbryio/herald.go/db"
	pb "github.com/lbryio/herald.go/protobuf/go"
	log "github.com/sirupsen/logrus"
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
	err := s1.RegisterService(claimtrieSvc, "blockchain_claimtrie")
	if err != nil {
		log.Errorf("RegisterService: %v\n", err)
	}

	// Register other "blockchain.{block,address,scripthash}.*" handlers.
	blockchainSvc := &BlockchainService{s.DB, s.Chain}
	err = s1.RegisterService(blockchainSvc, "blockchain_block")
	if err != nil {
		log.Errorf("RegisterService: %v\n", err)
	}
	err = s1.RegisterService(&BlockchainAddressService{*blockchainSvc}, "blockchain_address")
	if err != nil {
		log.Errorf("RegisterService: %v\n", err)
	}
	err = s1.RegisterService(&BlockchainScripthashService{*blockchainSvc}, "blockchain_scripthash")
	if err != nil {
		log.Errorf("RegisterService: %v\n", err)
	}

	r := mux.NewRouter()
	r.Handle("/rpc", s1)
	log.Fatal(http.ListenAndServe(port, r))

	return nil
}
