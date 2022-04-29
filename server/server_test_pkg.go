package server

func (s *Server) AddPeerExported() func(*Peer, bool, bool) error {
	return s.addPeer
}

func (s *Server) GetNumPeersExported() func() int64 {
	return s.getNumPeers
}
