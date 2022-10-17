package server

import (
	"encoding/hex"
	"fmt"
	"net"
	"net/rpc"
	"net/rpc/jsonrpc"
	"strings"
	"sync"
	"time"
	"unsafe"

	"github.com/lbryio/herald.go/db"
	"github.com/lbryio/herald.go/internal"
	"github.com/lbryio/lbcd/chaincfg"
	"github.com/lbryio/lbry.go/v3/extras/stop"
	log "github.com/sirupsen/logrus"
)

type headerNotification struct {
	internal.HeightHash
	blockHeader         [HEADER_SIZE]byte
	blockHeaderElectrum *BlockHeaderElectrum
	blockHeaderStr      string
}

type hashXNotification struct {
	hashX     [HASHX_LEN]byte
	status    []byte
	statusStr string
}

type session struct {
	id   uintptr
	addr net.Addr
	conn net.Conn
	// hashXSubs maps hashX to the original subscription key (address or scripthash)
	hashXSubs map[[HASHX_LEN]byte]string
	// headersSub indicates header subscription
	headersSub bool
	// headersSubRaw indicates the header subscription mode
	headersSubRaw bool
	// client provides the ability to send notifications
	client    rpc.ClientCodec
	clientSeq uint64
	// lastRecv records time of last incoming data
	lastRecv time.Time
	// lastSend records time of last outgoing data
	lastSend time.Time
}

func (s *session) doNotify(notification interface{}) {
	var method string
	var params interface{}
	switch notification.(type) {
	case headerNotification:
		if !s.headersSub {
			return
		}
		note, _ := notification.(headerNotification)
		heightHash := note.HeightHash
		method = "blockchain.headers.subscribe"
		if s.headersSubRaw {
			header := note.blockHeaderStr
			if len(header) == 0 {
				header = hex.EncodeToString(note.blockHeader[:])
			}
			params = &HeadersSubscribeRawResp{
				Hex:    header,
				Height: uint32(heightHash.Height),
			}
		} else {
			header := note.blockHeaderElectrum
			if header == nil { // not initialized
				header = newBlockHeaderElectrum(&note.blockHeader, uint32(heightHash.Height))
			}
			params = header
		}
	case hashXNotification:
		note, _ := notification.(hashXNotification)
		orig, ok := s.hashXSubs[note.hashX]
		if !ok {
			return
		}
		if len(orig) == 64 {
			method = "blockchain.scripthash.subscribe"
		} else {
			method = "blockchain.address.subscribe"
		}
		status := note.statusStr
		if len(status) == 0 {
			status = hex.EncodeToString(note.status)
		}
		params = []string{orig, status}
	default:
		log.Warnf("unknown notification type: %v", notification)
		return
	}
	// Send the notification.
	s.clientSeq += 1
	req := &rpc.Request{
		ServiceMethod: method,
		Seq:           s.clientSeq,
	}
	err := s.client.WriteRequest(req, params)
	if err != nil {
		log.Warnf("error: %v", err)
	}
	// Bump last send time.
	s.lastSend = time.Now()
}

type sessionMap map[uintptr]*session

type sessionManager struct {
	// sessionsMut protects sessions, headerSubs, hashXSubs state
	sessionsMut sync.RWMutex
	sessions    sessionMap
	// sessionsWait   sync.WaitGroup
	grp            *stop.Group
	sessionsMax    int
	sessionTimeout time.Duration
	manageTicker   *time.Ticker
	db             *db.ReadOnlyDBColumnFamily
	args           *Args
	chain          *chaincfg.Params
	// headerSubs are sessions subscribed via 'blockchain.headers.subscribe'
	headerSubs sessionMap
	// hashXSubs are sessions subscribed via 'blockchain.{address,scripthash}.subscribe'
	hashXSubs map[[HASHX_LEN]byte]sessionMap
}

func newSessionManager(db *db.ReadOnlyDBColumnFamily, args *Args, grp *stop.Group, chain *chaincfg.Params) *sessionManager {
	return &sessionManager{
		sessions:       make(sessionMap),
		grp:            grp,
		sessionsMax:    args.MaxSessions,
		sessionTimeout: time.Duration(args.SessionTimeout) * time.Second,
		manageTicker:   time.NewTicker(time.Duration(max(5, args.SessionTimeout/20)) * time.Second),
		db:             db,
		args:           args,
		chain:          chain,
		headerSubs:     make(sessionMap),
		hashXSubs:      make(map[[HASHX_LEN]byte]sessionMap),
	}
}

func (sm *sessionManager) start() {
	go sm.manage()
}

func (sm *sessionManager) stop() {
	sm.sessionsMut.Lock()
	defer sm.sessionsMut.Unlock()
	sm.headerSubs = make(sessionMap)
	sm.hashXSubs = make(map[[HASHX_LEN]byte]sessionMap)
	for _, sess := range sm.sessions {
		sess.client.Close()
		sess.conn.Close()
	}
	sm.sessions = make(sessionMap)
}

func (sm *sessionManager) manage() {
	for {
		sm.sessionsMut.Lock()
		for _, sess := range sm.sessions {
			if time.Since(sess.lastRecv) > sm.sessionTimeout {
				sm.removeSessionLocked(sess)
				log.Infof("session %v timed out", sess.addr.String())
			}
		}
		sm.sessionsMut.Unlock()
		// Wait for next management clock tick.
		select {
		case <-sm.grp.Ch():
			return
		case <-sm.manageTicker.C:
			continue
		}
	}
}

func (sm *sessionManager) addSession(conn net.Conn) *session {
	sm.sessionsMut.Lock()
	sess := &session{
		addr:      conn.RemoteAddr(),
		conn:      conn,
		hashXSubs: make(map[[11]byte]string),
		client:    jsonrpc.NewClientCodec(conn),
		lastRecv:  time.Now(),
	}
	sess.id = uintptr(unsafe.Pointer(sess))
	sm.sessions[sess.id] = sess
	sm.sessionsMut.Unlock()

	// Create a new RPC server. These services are linked to the
	// session, which allows RPC handlers to know the session for
	// each request and update subscriptions.
	s1 := rpc.NewServer()

	// Register "server.{features,banner,version}" handlers.
	serverSvc := &ServerService{sm.args}
	err := s1.RegisterName("server", serverSvc)
	if err != nil {
		log.Errorf("RegisterTCPService: %v\n", err)
	}

	// Register "blockchain.claimtrie.*"" handlers.
	claimtrieSvc := &ClaimtrieService{sm.db}
	err = s1.RegisterName("blockchain.claimtrie", claimtrieSvc)
	if err != nil {
		log.Errorf("RegisterService: %v\n", err)
	}

	// Register other "blockchain.{block,address,scripthash}.*" handlers.
	blockchainSvc := &BlockchainBlockService{sm.db, sm.chain}
	err = s1.RegisterName("blockchain.block", blockchainSvc)
	if err != nil {
		log.Errorf("RegisterName: %v\n", err)
		goto fail
	}
	err = s1.RegisterName("blockchain.headers", &BlockchainHeadersService{sm.db, sm.chain, sm, sess})
	if err != nil {
		log.Errorf("RegisterName: %v\n", err)
		goto fail
	}
	err = s1.RegisterName("blockchain.address", &BlockchainAddressService{sm.db, sm.chain, sm, sess})
	if err != nil {
		log.Errorf("RegisterName: %v\n", err)
		goto fail
	}
	err = s1.RegisterName("blockchain.scripthash", &BlockchainScripthashService{sm.db, sm.chain, sm, sess})
	if err != nil {
		log.Errorf("RegisterName: %v\n", err)
		goto fail
	}

	sm.grp.Add(1)
	go func() {
		s1.ServeCodec(&SessionServerCodec{jsonrpc.NewServerCodec(conn), sess})
		log.Infof("session %v goroutine exit", sess.addr.String())
		sm.grp.Done()
	}()
	return sess

fail:
	sm.removeSession(sess)
	return nil
}

func (sm *sessionManager) removeSession(sess *session) {
	sm.sessionsMut.Lock()
	defer sm.sessionsMut.Unlock()
	sm.removeSessionLocked(sess)
}

func (sm *sessionManager) removeSessionLocked(sess *session) {
	if sess.headersSub {
		delete(sm.headerSubs, sess.id)
	}
	for hashX := range sess.hashXSubs {
		subs, ok := sm.hashXSubs[hashX]
		if !ok {
			continue
		}
		delete(subs, sess.id)
	}
	delete(sm.sessions, sess.id)
	sess.client.Close()
	sess.conn.Close()
}

func (sm *sessionManager) headersSubscribe(sess *session, raw bool, subscribe bool) {
	sm.sessionsMut.Lock()
	defer sm.sessionsMut.Unlock()
	if subscribe {
		sm.headerSubs[sess.id] = sess
		sess.headersSub = true
		sess.headersSubRaw = raw
		return
	}
	delete(sm.headerSubs, sess.id)
	sess.headersSub = false
	sess.headersSubRaw = false
}

func (sm *sessionManager) hashXSubscribe(sess *session, hashX []byte, original string, subscribe bool) {
	sm.sessionsMut.Lock()
	defer sm.sessionsMut.Unlock()
	var key [HASHX_LEN]byte
	copy(key[:], hashX)
	subs, ok := sm.hashXSubs[key]
	if subscribe {
		if !ok {
			subs = make(sessionMap)
			sm.hashXSubs[key] = subs
		}
		subs[sess.id] = sess
		sess.hashXSubs[key] = original
		return
	}
	if ok {
		delete(subs, sess.id)
		if len(subs) == 0 {
			delete(sm.hashXSubs, key)
		}
	}
	delete(sess.hashXSubs, key)
}

func (sm *sessionManager) doNotify(notification interface{}) {
	sm.sessionsMut.RLock()
	var subsCopy sessionMap
	switch notification.(type) {
	case headerNotification:
		note, _ := notification.(headerNotification)
		subsCopy = sm.headerSubs
		if len(subsCopy) > 0 {
			note.blockHeaderElectrum = newBlockHeaderElectrum(&note.blockHeader, uint32(note.Height))
			note.blockHeaderStr = hex.EncodeToString(note.blockHeader[:])
		}
	case hashXNotification:
		note, _ := notification.(hashXNotification)
		hashXSubs, ok := sm.hashXSubs[note.hashX]
		if ok {
			subsCopy = hashXSubs
		}
		if len(subsCopy) > 0 {
			note.statusStr = hex.EncodeToString(note.status)
		}
	default:
		log.Warnf("unknown notification type: %v", notification)
	}
	sm.sessionsMut.RUnlock()

	// Deliver notification to relevant sessions.
	for _, sess := range subsCopy {
		sess.doNotify(notification)
	}
}

type SessionServerCodec struct {
	rpc.ServerCodec
	sess *session
}

// ReadRequestHeader provides ability to rewrite the incoming
// request "method" field. For example:
//     blockchain.block.get_header -> blockchain.block.Get_header
//     blockchain.address.listunspent -> blockchain.address.Listunspent
// This makes the "method" string compatible with rpc.Server
// requirements.
func (c *SessionServerCodec) ReadRequestHeader(req *rpc.Request) error {
	log.Infof("receive header from %v", c.sess.addr.String())
	err := c.ServerCodec.ReadRequestHeader(req)
	if err != nil {
		log.Warnf("error: %v", err)
		return err
	}
	rawMethod := req.ServiceMethod
	parts := strings.Split(rawMethod, ".")
	if len(parts) < 2 {
		return fmt.Errorf("blockchain rpc: service/method ill-formed: %q", rawMethod)
	}
	service := strings.Join(parts[0:len(parts)-1], ".")
	method := parts[len(parts)-1]
	if len(method) < 1 {
		return fmt.Errorf("blockchain rpc: method ill-formed: %q", method)
	}
	method = strings.ToUpper(string(method[0])) + string(method[1:])
	req.ServiceMethod = service + "." + method
	return err
}

// ReadRequestBody wraps the regular implementation, but updates session stats too.
func (c *SessionServerCodec) ReadRequestBody(params any) error {
	err := c.ServerCodec.ReadRequestBody(params)
	if err != nil {
		log.Warnf("error: %v", err)
		return err
	}
	log.Infof("receive body from %v", c.sess.addr.String())
	// Bump last receive time.
	c.sess.lastRecv = time.Now()
	return err
}

// WriteResponse wraps the regular implementation, but updates session stats too.
func (c *SessionServerCodec) WriteResponse(resp *rpc.Response, reply any) error {
	log.Infof("respond to %v", c.sess.addr.String())
	err := c.ServerCodec.WriteResponse(resp, reply)
	if err != nil {
		return err
	}
	// Bump last send time.
	c.sess.lastSend = time.Now()
	return err
}
