package server

import (
	"context"
	"fmt"

	"github.com/lbryio/herald.go/db"
	"github.com/lbryio/herald.go/internal/metrics"
	pb "github.com/lbryio/herald.go/protobuf/go"
	"github.com/prometheus/client_golang/prometheus"
	log "github.com/sirupsen/logrus"
)

type ClaimtrieService struct {
	DB     *db.ReadOnlyDBColumnFamily
	Server *Server
}

type ResolveData struct {
	Data []string `json:"data"`
}

type Result struct {
	Data string `json:"data"`
}

type GetClaimByIDData struct {
	ClaimID string `json:"claim_id"`
}

// Resolve is the json rpc endpoint for 'blockchain.claimtrie.resolve'.
func (t *ClaimtrieService) Resolve(args *ResolveData, result **pb.Outputs) error {
	log.Println("Resolve")
	res, err := InternalResolve(args.Data, t.DB)
	*result = res
	return err
}

// Search is the json rpc endpoint for 'blockchain.claimtrie.search'.
func (t *ClaimtrieService) Search(args *pb.SearchRequest, result **pb.Outputs) error {
	log.Println("Search")
	if t.Server == nil {
		log.Warnln("Server is nil in Search")
		*result = nil
		return nil
	}
	ctx := context.Background()
	res, err := t.Server.Search(ctx, args)
	*result = res
	return err
}

// GetClaimByID is the json rpc endpoint for 'blockchain.claimtrie.getclaimbyid'.
func (t *ClaimtrieService) GetClaimByID(args *GetClaimByIDData, result **pb.Outputs) error {
	log.Println("GetClaimByID")
	if len(args.ClaimID) != 40 {
		*result = nil
		return fmt.Errorf("len(claim_id) != 40")
	}

	rows, extras, err := t.DB.GetClaimByID(args.ClaimID)
	if err != nil {
		*result = nil
		return err
	}

	metrics.RequestsCount.With(prometheus.Labels{"method": "blockchain.claimtrie.getclaimbyid"}).Inc()

	// FIXME: this has txos and extras and so does GetClaimById?
	allTxos := make([]*pb.Output, 0)
	allExtraTxos := make([]*pb.Output, 0)

	for _, row := range rows {
		txos, extraTxos, err := row.ToOutputs()
		if err != nil {
			*result = nil
			return err
		}
		// TODO: there may be a more efficient way to do this.
		allTxos = append(allTxos, txos...)
		allExtraTxos = append(allExtraTxos, extraTxos...)
	}

	for _, extra := range extras {
		txos, extraTxos, err := extra.ToOutputs()
		if err != nil {
			*result = nil
			return err
		}
		// TODO: there may be a more efficient way to do this.
		allTxos = append(allTxos, txos...)
		allExtraTxos = append(allExtraTxos, extraTxos...)
	}

	res := &pb.Outputs{
		Txos:         allTxos,
		ExtraTxos:    allExtraTxos,
		Total:        uint32(len(allTxos) + len(allExtraTxos)),
		Offset:       0,   //TODO
		Blocked:      nil, //TODO
		BlockedTotal: 0,   //TODO
	}

	log.Warn(res)

	*result = res
	return nil
}
