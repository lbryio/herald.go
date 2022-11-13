package server

import (
	log "github.com/sirupsen/logrus"
)

type PeersService struct{}

type PeersSubscribeReq struct {
	Ip      string   `json:"ip"`
	Host    string   `json:"host"`
	Details []string `json:"details"`
}

type PeersSubscribeResp struct{}

// Features is the json rpc endpoint for 'server.peers.subcribe'.
func (t *PeersService) Subscribe(req *PeersSubscribeReq, res **PeersSubscribeResp) error {
	log.Println("PeersSubscribe")

	*res = nil

	return nil
}
