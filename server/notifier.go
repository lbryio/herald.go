package server

import (
	"encoding/binary"
	"net"

	"github.com/lbryio/herald.go/internal"
	"github.com/sirupsen/logrus"
)

const NotifierResponseLength = 40

// AddHeightSub adds a new height subscriber
func (s *Server) AddHeightSub(addr net.Addr, conn net.Conn) {
	s.HeightSubsMut.Lock()
	defer s.HeightSubsMut.Unlock()
	s.HeightSubs[addr] = conn
}

// DoNotify sends a notification to all height subscribers
func (s *Server) DoNotify(heightHash *internal.HeightHash) error {
	buff := make([]byte, NotifierResponseLength)
	toDelete := make([]net.Addr, 0)

	s.HeightSubsMut.RLock()
	for addr, conn := range s.HeightSubs {
		// struct.pack(b'>Q32s', height, block_hash)
		binary.BigEndian.PutUint64(buff, heightHash.Height)
		copy(buff[8:], heightHash.BlockHash[:32])
		logrus.Tracef("notifying %s", addr)
		n, err := conn.Write(buff)
		if err != nil {
			logrus.Warn(err)
			toDelete = append(toDelete, addr)
		}
		if n != NotifierResponseLength {
			logrus.Warn("not all bytes written")
		}
	}
	s.HeightSubsMut.RUnlock()

	if len(toDelete) > 0 {
		s.HeightSubsMut.Lock()
		for _, v := range toDelete {
			delete(s.HeightSubs, v)
		}
		s.HeightSubsMut.Unlock()
	}

	return nil
}

// RunNotifier Runs the notfying action forever
func (s *Server) RunNotifier() error {
	for notification := range s.NotifierChan {
		switch notification.(type) {
		case internal.HeightHash:
			heightHash, _ := notification.(internal.HeightHash)
			s.DoNotify(&heightHash)
		// Do we need this?
		// case peerNotification:
		// 	peer, _ := notification.(peerNotification)
		// 	s.notifyPeerSubs(&Peer{Address: peer.address, Port: peer.port})
		default:
			logrus.Warn("unknown notification type")
		}
		s.sessionManager.doNotify(notification)
	}
	return nil
}

// NotifierServer implements the TCP protocol for height/blockheader notifications
func (s *Server) NotifierServer() error {
	s.Grp.Add(1)
	address := ":" + s.Args.NotifierPort
	addr, err := net.ResolveTCPAddr("tcp", address)
	if err != nil {
		return err
	}

	listen, err := net.ListenTCP("tcp", addr)
	if err != nil {
		return err
	}

	defer listen.Close()
	rdyCh := make(chan bool)

	for {
		var conn net.Conn
		var err error

		logrus.Info("Waiting for connection")

		go func() {
			conn, err = listen.Accept()
			rdyCh <- true
		}()

		select {
		case <-s.Grp.Ch():
			s.Grp.Done()
			return nil
		case <-rdyCh:
			logrus.Info("Connection accepted")
		}

		if err != nil {
			logrus.Warn(err)
			continue
		}

		addr := conn.RemoteAddr()

		logrus.Println(addr)

		// _, err = conn.Write([]byte(addr.String()))
		// if err != nil {
		// 	logrus.Warn(err)
		// 	continue
		// }

		go s.AddHeightSub(addr, conn)
	}
}
