package server

import (
	"errors"

	log "github.com/sirupsen/logrus"
)

type PeersService struct {
	Server *Server
	// needed for subscribe/unsubscribe
	sessionMgr *sessionManager
	session    *session
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
	// var port = "50001"

	// FIXME: Get the actual port from the request details

	if t.sessionMgr == nil || t.session == nil {
		*res = nil
		return errors.New("no session, rpc not supported")
	}
	t.sessionMgr.peersSubscribe(t.session, true /*subscribe*/)

	*res = nil
	return nil
}
