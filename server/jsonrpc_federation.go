package server

import (
	"context"

	pb "github.com/lbryio/herald.go/protobuf/go"
	log "github.com/sirupsen/logrus"
)

type PeersService struct {
	Server *Server
}

type PeersSubscribeReq struct {
	Ip      string   `json:"ip"`
	Host    string   `json:"host"`
	Details []string `json:"details"`
}

type PeersSubscribeResp string

// Features is the json rpc endpoint for 'server.peers.subcribe'.
func (t *PeersService) Subscribe(req *PeersSubscribeReq, res **PeersSubscribeResp) error {
	log.Println("PeersSubscribe")
	ctx := context.Background()
	var port = "50001"

	// FIXME: Get the actual port from the request details

	msg := &pb.ServerMessage{
		Address: req.Ip,
		Port:    port,
	}

	peers, err := t.Server.PeerSubscribe(ctx, msg)
	if err != nil {
		log.Println(err)
		*res = nil
		return err
	}

	*res = (*PeersSubscribeResp)(&peers.Value)
	return nil
}
