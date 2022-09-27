package server

import (
	"fmt"
	"net/http"
	"strings"

	gorilla_mux "github.com/gorilla/mux"
	gorilla_rpc "github.com/gorilla/rpc"
	gorilla_json "github.com/gorilla/rpc/json"
	log "github.com/sirupsen/logrus"
)

type gorillaRpcCodec struct {
	gorilla_rpc.Codec
}

func (c *gorillaRpcCodec) NewRequest(r *http.Request) gorilla_rpc.CodecRequest {
	return &gorillaRpcCodecRequest{c.Codec.NewRequest(r)}
}

// gorillaRpcCodecRequest provides ability to rewrite the incoming
// request "method" field. For example:
//     blockchain.block.get_header -> blockchain_block.Get_header
//     blockchain.address.listunspent -> blockchain_address.Listunspent
// This makes the "method" string compatible with Gorilla/RPC
// requirements.
type gorillaRpcCodecRequest struct {
	gorilla_rpc.CodecRequest
}

func (cr *gorillaRpcCodecRequest) Method() (string, error) {
	rawMethod, err := cr.CodecRequest.Method()
	if err != nil {
		return rawMethod, err
	}
	parts := strings.Split(rawMethod, ".")
	if len(parts) < 2 {
		return rawMethod, fmt.Errorf("blockchain rpc: service/method ill-formed: %q", rawMethod)
	}
	service := strings.Join(parts[0:len(parts)-1], "_")
	method := parts[len(parts)-1]
	if len(method) < 1 {
		return rawMethod, fmt.Errorf("blockchain rpc: method ill-formed: %q", method)
	}
	method = strings.ToUpper(string(method[0])) + string(method[1:])
	return service + "." + method, err
}

// StartJsonRPC starts the json rpc server and registers the endpoints.
func (s *Server) StartJsonRPC() error {
	port := ":" + s.Args.JSONRPCPort

	s1 := gorilla_rpc.NewServer() // Create a new RPC server
	// Register the type of data requested as JSON, with custom codec.
	s1.RegisterCodec(&gorillaRpcCodec{gorilla_json.NewCodec()}, "application/json")

	// Register "blockchain.claimtrie.*"" handlers.
	claimtrieSvc := &ClaimtrieService{s.DB}
	err := s1.RegisterTCPService(claimtrieSvc, "blockchain_claimtrie")
	if err != nil {
		log.Errorf("RegisterService: %v\n", err)
	}

	// Register other "blockchain.{block,address,scripthash}.*" handlers.
	blockchainSvc := &BlockchainBlockService{s.DB, s.Chain}
	err = s1.RegisterTCPService(blockchainSvc, "blockchain_block")
	if err != nil {
		log.Errorf("RegisterService: %v\n", err)
	}
	err = s1.RegisterTCPService(&BlockchainAddressService{*blockchainSvc}, "blockchain_address")
	if err != nil {
		log.Errorf("RegisterService: %v\n", err)
	}
	err = s1.RegisterTCPService(&BlockchainScripthashService{*blockchainSvc}, "blockchain_scripthash")
	if err != nil {
		log.Errorf("RegisterService: %v\n", err)
	}

	r := gorilla_mux.NewRouter()
	r.Handle("/rpc", s1)
	log.Fatal(http.ListenAndServe(port, r))

	return nil
}
