package server

import (
	"fmt"
	"net"
	"net/http"
	"strconv"
	"strings"

	gorilla_mux "github.com/gorilla/mux"
	gorilla_rpc "github.com/gorilla/rpc"
	gorilla_json "github.com/gorilla/rpc/json"
	log "github.com/sirupsen/logrus"
	"golang.org/x/net/netutil"
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
	s.sessionManager.start()
	defer s.sessionManager.stop()

	// Set up the pure JSONRPC server with persistent connections/sessions.
	if s.Args.JSONRPCPort != 0 {
		port := ":" + strconv.FormatUint(uint64(s.Args.JSONRPCPort), 10)
		laddr, err := net.ResolveTCPAddr("tcp", port)
		if err != nil {
			log.Errorf("ResoveIPAddr: %v\n", err)
			goto fail1
		}
		listener, err := net.ListenTCP("tcp", laddr)
		if err != nil {
			log.Errorf("ListenTCP: %v\n", err)
			goto fail1
		}
		log.Infof("JSONRPC server listening on %s", listener.Addr().String())
		acceptConnections := func(listener net.Listener) {
			for {
				conn, err := listener.Accept()
				if err != nil {
					log.Errorf("Accept: %v\n", err)
					break
				}
				log.Infof("Accepted: %v", conn.RemoteAddr())
				s.sessionManager.addSession(conn)
			}
		}
		go acceptConnections(netutil.LimitListener(listener, s.sessionManager.sessionsMax))
	}

fail1:
	// Set up the JSONRPC over HTTP server.
	if s.Args.JSONRPCHTTPPort != 0 {
		s1 := gorilla_rpc.NewServer() // Create a new RPC server
		// Register the type of data requested as JSON, with custom codec.
		s1.RegisterCodec(&gorillaRpcCodec{gorilla_json.NewCodec()}, "application/json")

		// Register "blockchain.claimtrie.*"" handlers.
		claimtrieSvc := &ClaimtrieService{s.DB}
		err := s1.RegisterTCPService(claimtrieSvc, "blockchain_claimtrie")
		if err != nil {
			log.Errorf("RegisterTCPService: %v\n", err)
			goto fail2
		}

		// Register "blockchain.{block,address,scripthash,transaction}.*" handlers.
		blockchainSvc := &BlockchainBlockService{s.DB, s.Chain}
		err = s1.RegisterTCPService(blockchainSvc, "blockchain_block")
		if err != nil {
			log.Errorf("RegisterTCPService: %v\n", err)
			goto fail2
		}
		err = s1.RegisterTCPService(&BlockchainHeadersService{s.DB, s.Chain, s.sessionManager, nil}, "blockchain_headers")
		if err != nil {
			log.Errorf("RegisterTCPService: %v\n", err)
			goto fail2
		}
		err = s1.RegisterTCPService(&BlockchainAddressService{s.DB, s.Chain, s.sessionManager, nil}, "blockchain_address")
		if err != nil {
			log.Errorf("RegisterTCPService: %v\n", err)
			goto fail2
		}
		err = s1.RegisterTCPService(&BlockchainScripthashService{s.DB, s.Chain, s.sessionManager, nil}, "blockchain_scripthash")
		if err != nil {
			log.Errorf("RegisterTCPService: %v\n", err)
			goto fail2
		}
		err = s1.RegisterTCPService(&BlockchainTransactionService{s.DB, s.Chain, s.sessionManager}, "blockchain_transaction")
		if err != nil {
			log.Errorf("RegisterTCPService: %v\n", err)
			goto fail2
		}

		// Register "server.{features,banner,version}" handlers.
		serverSvc := &ServerService{s.Args}
		err = s1.RegisterTCPService(serverSvc, "server")
		if err != nil {
			log.Errorf("RegisterTCPService: %v\n", err)
			goto fail2
		}

		r := gorilla_mux.NewRouter()
		r.Handle("/rpc", s1)
		port := ":" + strconv.FormatUint(uint64(s.Args.JSONRPCHTTPPort), 10)
		log.Infof("HTTP JSONRPC server listening on %s", port)
		log.Fatal(http.ListenAndServe(port, r))
	}

fail2:
	return nil
}
