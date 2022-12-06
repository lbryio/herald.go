package server

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
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
	"github.com/lbryio/lbcd/chaincfg/chainhash"
	"github.com/lbryio/lbry.go/v3/extras/stop"
	log "github.com/sirupsen/logrus"
)

type headerNotification struct {
	internal.HeightHash
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
				header = hex.EncodeToString(note.BlockHeader[:])
			}
			params = &HeadersSubscribeRawResp{
				Hex:    header,
				Height: uint32(heightHash.Height),
			}
		} else {
			header := note.blockHeaderElectrum
			if header == nil { // not initialized
				header = newBlockHeaderElectrum((*[HEADER_SIZE]byte)(note.BlockHeader), uint32(heightHash.Height))
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
	sm.grp.Add(1)
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
			sm.grp.Done()
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
		log.Errorf("RegisterName: %v\n", err)
	}

	// Register "blockchain.claimtrie.*"" handlers.
	claimtrieSvc := &ClaimtrieService{sm.db}
	err = s1.RegisterName("blockchain.claimtrie", claimtrieSvc)
	if err != nil {
		log.Errorf("RegisterName: %v\n", err)
	}

	// Register "blockchain.{block,address,scripthash,transaction}.*" handlers.
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
	err = s1.RegisterName("blockchain.transaction", &BlockchainTransactionService{sm.db, sm.chain, sm})
	if err != nil {
		log.Errorf("RegisterName: %v\n", err)
		goto fail
	}

	sm.grp.Add(1)
	go func() {
		s1.ServeCodec(&sessionServerCodec{jsonrpc.NewServerCodec(newJsonPatchingCodec(conn)), sess})
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

func (sm *sessionManager) broadcastTx(rawTx []byte) (*chainhash.Hash, error) {
	// TODO
	return nil, nil
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
	switch notification.(type) {
	case internal.HeightHash:
		// The HeightHash notification translates to headerNotification.
		notification = &headerNotification{HeightHash: notification.(internal.HeightHash)}
	}
	sm.sessionsMut.RLock()
	var subsCopy sessionMap
	switch notification.(type) {
	case headerNotification:
		note, _ := notification.(headerNotification)
		subsCopy = sm.headerSubs
		if len(subsCopy) > 0 {
			hdr := [HEADER_SIZE]byte{}
			copy(hdr[:], note.BlockHeader)
			note.blockHeaderElectrum = newBlockHeaderElectrum(&hdr, uint32(note.Height))
			note.blockHeaderStr = hex.EncodeToString(note.BlockHeader[:])
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

type sessionServerCodec struct {
	rpc.ServerCodec
	sess *session
}

// ReadRequestHeader provides ability to rewrite the incoming
// request "method" field. For example:
//     blockchain.block.get_header -> blockchain.block.Get_header
//     blockchain.address.listunspent -> blockchain.address.Listunspent
// This makes the "method" string compatible with rpc.Server
// requirements.
func (c *sessionServerCodec) ReadRequestHeader(req *rpc.Request) error {
	log.Infof("from %v receive header", c.sess.addr.String())
	err := c.ServerCodec.ReadRequestHeader(req)
	if err != nil {
		log.Warnf("error: %v", err)
		return err
	}
	log.Infof("from %v receive header: %#v", c.sess.addr.String(), *req)
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
func (c *sessionServerCodec) ReadRequestBody(params any) error {
	log.Infof("from %v receive body", c.sess.addr.String())
	err := c.ServerCodec.ReadRequestBody(params)
	if err != nil {
		log.Warnf("error: %v", err)
		return err
	}
	log.Infof("from %v receive body: %#v", c.sess.addr.String(), params)
	// Bump last receive time.
	c.sess.lastRecv = time.Now()
	return err
}

// WriteResponse wraps the regular implementation, but updates session stats too.
func (c *sessionServerCodec) WriteResponse(resp *rpc.Response, reply any) error {
	log.Infof("respond to %v", c.sess.addr.String())
	err := c.ServerCodec.WriteResponse(resp, reply)
	if err != nil {
		return err
	}
	// Bump last send time.
	c.sess.lastSend = time.Now()
	return err
}

// serverRequest is a duplicate of serverRequest from
// net/rpc/jsonrpc/server.go  with an added Version which
// we can check.
type serverRequest struct {
	Version string           `json:"jsonrpc"`
	Method  string           `json:"method"`
	Params  *json.RawMessage `json:"params"`
	Id      *json.RawMessage `json:"id"`
}

// serverResponse is a duplicate of serverResponse from
// net/rpc/jsonrpc/server.go with an added Version which
// we can set at will.
type serverResponse struct {
	Version string           `json:"jsonrpc"`
	Id      *json.RawMessage `json:"id"`
	Result  any              `json:"result,omitempty"`
	Error   any              `json:"error,omitempty"`
}

// jsonPatchingCodec is able to intercept the JSON requests/responses
// and tweak them. Currently, it appears we need to make several changes:
// 1) add "jsonrpc": "2.0" (or "jsonrpc": "1.0") in response
// 2) add newline to frame response
// 3) add "params": [] when "params" is missing
// 4) replace params ["arg1", "arg2", ...] with [["arg1", "arg2", ...]]
type jsonPatchingCodec struct {
	conn      net.Conn
	inBuffer  *bytes.Buffer
	dec       *json.Decoder
	enc       *json.Encoder
	outBuffer *bytes.Buffer
}

func newJsonPatchingCodec(conn net.Conn) *jsonPatchingCodec {
	buf1, buf2 := bytes.NewBuffer(nil), bytes.NewBuffer(nil)
	return &jsonPatchingCodec{
		conn:      conn,
		inBuffer:  buf1,
		dec:       json.NewDecoder(buf1),
		enc:       json.NewEncoder(buf2),
		outBuffer: buf2,
	}
}

func (c *jsonPatchingCodec) Read(p []byte) (n int, err error) {
	if c.outBuffer.Len() > 0 {
		// Return remaining decoded bytes.
		return c.outBuffer.Read(p)
	}
	// Buffer contents consumed. Try to decode more JSON.

	// Read until framing newline. This allows us to print the raw request.
	for !bytes.ContainsAny(c.inBuffer.Bytes(), "\n") {
		var buf [1024]byte
		n, err = c.conn.Read(buf[:])
		if err != nil {
			return 0, err
		}
		c.inBuffer.Write(buf[:n])
	}
	log.Infof("raw request: %v", c.inBuffer.String())

	var req serverRequest
	err = c.dec.Decode(&req)
	if err != nil {
		return 0, err
	}

	if req.Params != nil {
		n := len(*req.Params)
		if n < 2 || (*req.Params)[0] != '[' && (*req.Params)[n-1] != ']' {
			// This is an error, but we're not going to try to correct it.
			goto encode
		}
		// FIXME: The heuristics here don't cover all possibilities.
		// For example: [{obj1}, {obj2}] or ["foo,bar"] would not
		// be handled correctly.
		bracketed := (*req.Params)[1 : n-1]
		n = len(bracketed)
		if n > 1 && (bracketed[0] == '{' || bracketed[0] == '[') {
			// Probable single object or list argument.
			goto encode
		}
		// The params look like ["arg1", "arg2", "arg3", ...].
		// We're in trouble because our jsonrpc library does not
		// handle this. So pack these args in an inner list.
		// The handler method will receive ONE list argument.
		params := json.RawMessage(fmt.Sprintf("[[%s]]", bracketed))
		req.Params = &params
	} else {
		// Add empty argument list if params omitted.
		params := json.RawMessage("[]")
		req.Params = &params
	}

encode:
	// Encode the request. This allows us to print the patched request.
	buf, err := json.Marshal(req)
	if err != nil {
		return 0, err
	}
	log.Infof("patched request: %v", string(buf))

	err = c.enc.Encode(req)
	if err != nil {
		return 0, err
	}
	return c.outBuffer.Read(p)
}

func (c *jsonPatchingCodec) Write(p []byte) (n int, err error) {
	log.Infof("raw response: %v", string(p))
	var resp serverResponse
	err = json.Unmarshal(p, &resp)
	if err != nil {
		return 0, err
	}

	// Add "jsonrpc": "2.0" if missing.
	if len(resp.Version) == 0 {
		resp.Version = "2.0"
	}

	buf, err := json.Marshal(resp)
	if err != nil {
		return 0, err
	}
	log.Infof("patched response: %v", string(buf))

	// Add newline for framing.
	return c.conn.Write(append(buf, '\n'))
}

func (c *jsonPatchingCodec) Close() error {
	return c.conn.Close()
}
