package server

import (
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

// Resolve is the json rpc endpoint for 'blockchain.claimtrie.resolve'.
func (t *ClaimtrieService) Resolve(args *ResolveData, result **pb.Outputs) error {
	log.Println("Resolve")
	res, err := InternalResolve(args.Data, t.DB)
	*result = res
	return err
}
