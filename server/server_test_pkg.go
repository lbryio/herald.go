package server

import (
	"github.com/lbryio/herald.go/db"
	"github.com/lbryio/lbcd/chaincfg"
	"github.com/lbryio/lbry.go/v3/extras/stop"
)

func (s *Server) AddPeerExported() func(*Peer, bool, bool) error {
	return s.addPeer
}

func (s *Server) GetNumPeersExported() func() int64 {
	return s.getNumPeers
}

func NewSessionManagerExported(db *db.ReadOnlyDBColumnFamily, args *Args, grp *stop.Group, chain *chaincfg.Params) *sessionManager {
	return newSessionManager(db, args, grp, chain)
}
