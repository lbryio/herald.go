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

type JSONServer struct {
	DB *db.ReadOnlyDBColumnFamily
}

type ResolveData struct {
	Data []string `json:"data"`
}

type Result struct {
	Data string `json:"data"`
}

// Resolve is the json rpc endpoint for resolve
func (t *JSONServer) Resolve(r *http.Request, args *ResolveData, result **pb.Outputs) error {
	log.Println("Resolve")
	res, err := InternalResolve(args.Data, t.DB)
	*result = res
	return err
}

// StartJsonRPC starts the json rpc server and registers the endpoints.
func (s *Server) StartJsonRPC() error {
	server := new(JSONServer)
	server.DB = s.DB

	port := ":" + s.Args.JSONRPCPort

	s1 := rpc.NewServer()                                 // Create a new RPC server
	s1.RegisterCodec(json.NewCodec(), "application/json") // Register the type of data requested as JSON
	s1.RegisterService(server, "")                        // Register the service by creating a new JSON server

	r := mux.NewRouter()
	r.Handle("/rpc", s1)
	log.Fatal(http.ListenAndServe(port, r))

	return nil
}
