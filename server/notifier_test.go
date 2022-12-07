package server_test

import (
	"encoding/hex"
	"fmt"
	"net"
	"testing"
	"time"

	"github.com/lbryio/herald.go/internal"
	"github.com/lbryio/herald.go/server"
	"github.com/lbryio/lbry.go/v3/extras/stop"
	"github.com/sirupsen/logrus"
)

const defaultBufferSize = 1024

func subReady(s *server.Server) error {
	sleepTime := time.Millisecond * 100
	for {
		if sleepTime > time.Second {
			return fmt.Errorf("timeout")
		}
		s.HeightSubsMut.RLock()
		if len(s.HeightSubs) > 0 {
			s.HeightSubsMut.RUnlock()
			return nil
		}
		s.HeightSubsMut.RUnlock()

		logrus.Warn("waiting for subscriber")
		time.Sleep(sleepTime)
		sleepTime = sleepTime * 2

	}
}

func tcpConnReady(addr string) (net.Conn, error) {
	sleepTime := time.Millisecond * 100
	for {
		if sleepTime > time.Second {
			return nil, fmt.Errorf("timeout")
		}

		conn, err := net.Dial("tcp", addr)
		if err != nil {
			logrus.Warn(err)
			time.Sleep(sleepTime)
			sleepTime = sleepTime * 2
			continue
		}
		return conn, nil
	}
}

func tcpRead(conn net.Conn) ([]byte, error) {
	buf := make([]byte, defaultBufferSize)
	n, err := conn.Read(buf)
	if err != nil {
		return nil, err
	}
	if n != server.NotifierResponseLength {
		return nil, fmt.Errorf("not all bytes read")
	}

	return buf[:n], nil
}

func TestNotifierServer(t *testing.T) {
	args := server.MakeDefaultTestArgs()
	ctx := stop.NewDebug()
	hub := server.MakeHubServer(ctx, args)

	go hub.NotifierServer()
	go hub.RunNotifier()

	addr := fmt.Sprintf(":%d", args.NotifierPort)
	logrus.Info(addr)
	conn, err := tcpConnReady(addr)
	if err != nil {
		t.Fatal(err)
	}

	resCh := make(chan []byte)

	go func() {
		logrus.Warn("waiting for response")
		res, err := tcpRead(conn)
		logrus.Warn("got response")
		if err != nil {
			logrus.Warn(err)
			return
		}
		resCh <- res
	}()

	// Hacky but needed because if the reader isn't ready
	// before the writer sends it won't get the data
	err = subReady(hub)
	if err != nil {
		t.Fatal(err)
	}

	hash, _ := hex.DecodeString("AAAAAAAAAAAAAAAAAAAAAAAAAAAAAAAA")
	logrus.Warn("sending hash")
	hub.NotifierChan <- internal.HeightHash{Height: 1, BlockHash: hash}

	res := <-resCh
	logrus.Info(string(res))
}
