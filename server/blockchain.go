package server

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"

	"github.com/lbryio/herald.go/internal"
	"github.com/lbryio/lbcd/chaincfg"
	"github.com/lbryio/lbcd/chaincfg/chainhash"
	"github.com/lbryio/lbcd/txscript"
	"github.com/lbryio/lbcd/wire"
	"github.com/lbryio/lbcutil"
	"golang.org/x/exp/constraints"
)

type RpcReq interface {
}
type RpcResp interface {
}
type RpcHandler interface {
	Handle() (RpcResp, error)
}

const CHUNK_SIZE = 96
const MAX_CHUNK_SIZE = 40960
const HEADER_SIZE = wire.MaxBlockHeaderPayload
const HASHX_LEN = 11

func min[Ord constraints.Ordered](x, y Ord) Ord {
	if x < y {
		return x
	}
	return y
}

type blockGetChunkReq uint32
type blockGetChunkResp string

// 'blockchain.block.get_chunk'
func (req *blockGetChunkReq) Handle(s *Server) (*blockGetChunkResp, error) {
	index := uint32(*req)
	db_headers, err := s.DB.GetHeaders(index*CHUNK_SIZE, CHUNK_SIZE)
	if err != nil {
		return nil, err
	}
	raw := make([]byte, 0, HEADER_SIZE*len(db_headers))
	for _, h := range db_headers {
		raw = append(raw, h[:]...)
	}
	headers := blockGetChunkResp(hex.EncodeToString(raw))
	return &headers, err
}

type blockGetHeaderReq uint32
type blockGetHeaderResp struct {
	Version       uint32 `json:"version"`
	PrevBlockHash string `json:"prev_block_hash"`
	MerkleRoot    string `json:"merkle_root"`
	ClaimTrieRoot string `json:"claim_trie_root"`
	Timestamp     uint32 `json:"timestamp"`
	Bits          uint32 `json:"bits"`
	Nonce         uint32 `json:"nonce"`
	BlockHeight   uint32 `json:"block_height"`
}

// 'blockchain.block.get_header'
func (req *blockGetHeaderReq) Handle(s *Server) (*blockGetHeaderResp, error) {
	height := uint32(*req)
	headers, err := s.DB.GetHeaders(height, 1)
	if err != nil {
		return nil, err
	}
	if len(headers) < 1 {
		return nil, errors.New("not found")
	}
	decode := func(header *[HEADER_SIZE]byte, height uint32) *blockGetHeaderResp {
		var h1, h2, h3 chainhash.Hash
		h1.SetBytes(header[4:36])
		h2.SetBytes(header[36:68])
		h3.SetBytes(header[68:100])
		return &blockGetHeaderResp{
			Version:       binary.LittleEndian.Uint32(header[0:]),
			PrevBlockHash: h1.String(),
			MerkleRoot:    h2.String(),
			ClaimTrieRoot: h3.String(),
			Timestamp:     binary.LittleEndian.Uint32(header[100:]),
			Bits:          binary.LittleEndian.Uint32(header[104:]),
			Nonce:         binary.LittleEndian.Uint32(header[108:]),
			BlockHeight:   height,
		}
	}
	return decode(&headers[0], height), nil
}

type blockHeadersReq struct {
	StartHeight uint32 `json:"start_height"`
	Count       uint32 `json:"count"`
	CpHeight    uint32 `json:"cp_height"`
	B64         bool   `json:"b64"`
}

type blockHeadersResp struct {
	Base64 string `json:"base64,omitempty"`
	Hex    string `json:"hex,omitempty"`
	Count  uint32 `json:"count"`
	Max    uint32 `json:"max"`
	Branch string `json:"branch,omitempty"`
	Root   string `json:"root,omitempty"`
}

// 'blockchain.block.headers'
func (req *blockHeadersReq) Handle(s *Server) (*blockHeadersResp, error) {
	count := min(req.Count, MAX_CHUNK_SIZE)
	db_headers, err := s.DB.GetHeaders(req.StartHeight, count)
	if err != nil {
		return nil, err
	}
	count = uint32(len(db_headers))
	raw := make([]byte, 0, HEADER_SIZE*count)
	for _, h := range db_headers {
		raw = append(raw, h[:]...)
	}
	result := &blockHeadersResp{
		Count: count,
		Max:   MAX_CHUNK_SIZE,
	}
	if req.B64 {
		zipped := bytes.Buffer{}
		w := zlib.NewWriter(&zipped)
		w.Write(raw)
		w.Close()
		result.Base64 = base64.StdEncoding.EncodeToString(zipped.Bytes())
	} else {
		result.Hex = hex.EncodeToString(raw)
	}
	if count > 0 && req.CpHeight > 0 {
		// TODO
		//last_height := height + count - 1
	}
	return result, err
}

func hashX(scripthash string) []byte {
	sh, _ := hex.DecodeString(scripthash)
	internal.ReverseBytesInPlace(sh)
	return sh[:HASHX_LEN]
}

func hashXScript(script []byte, coin *chaincfg.Params) []byte {
	if _, err := txscript.ExtractClaimScript(script); err == nil {
		baseScript := txscript.StripClaimScriptPrefix(script)
		if class, addrs, _, err := txscript.ExtractPkScriptAddrs(baseScript, coin); err == nil {
			switch class {
			case txscript.PubKeyHashTy, txscript.ScriptHashTy, txscript.PubKeyTy:
				script, _ := txscript.PayToAddrScript(addrs[0])
				return hashXScript(script, coin)
			}
		}
	}
	sum := sha256.Sum256(script)
	return sum[:HASHX_LEN]
}

type addressGetBalanceReq struct {
	Address string `json:"address"`
}
type addressGetBalanceResp struct {
	Confirmed   uint64 `json:"confirmed"`
	Unconfirmed uint64 `json:"unconfirmed"`
}

// 'blockchain.address.get_balance'
func (req *addressGetBalanceReq) Handle(s *Server) (*addressGetBalanceResp, error) {
	address, err := lbcutil.DecodeAddress(req.Address, s.Chain)
	if err != nil {
		return nil, err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, err
	}
	hashX := hashXScript(script, s.Chain)
	confirmed, unconfirmed, err := s.DB.GetBalance(hashX)
	if err != nil {
		return nil, err
	}
	return &addressGetBalanceResp{confirmed, unconfirmed}, err
}

type addressGetHistoryReq struct {
	Address string `json:"address"`
}
type TxInfo struct {
	TxHash string `json:"tx_hash"`
	Height uint32 `json:"height"`
}
type TxInfoFee struct {
	TxInfo
	Fee uint64 `json:"fee"`
}
type addressGetHistoryResp struct {
	Confirmed   []TxInfo    `json:"confirmed"`
	Unconfirmed []TxInfoFee `json:"unconfirmed"`
}

// 'blockchain.address.get_history'
func (req *addressGetHistoryReq) Handle(s *Server) (*addressGetHistoryResp, error) {
	address, err := lbcutil.DecodeAddress(req.Address, s.Chain)
	if err != nil {
		return nil, err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, err
	}
	hashX := hashXScript(script, s.Chain)
	dbTXs, err := s.DB.GetHistory(hashX)
	confirmed := make([]TxInfo, 0, len(dbTXs))
	for _, tx := range dbTXs {
		confirmed = append(confirmed,
			TxInfo{
				TxHash: tx.TxHash.String(),
				Height: tx.Height,
			})
	}
	result := &addressGetHistoryResp{
		Confirmed:   confirmed,
		Unconfirmed: []TxInfoFee{}, // TODO
	}
	return result, nil
}

type addressGetMempoolReq struct {
	Address string `json:"address"`
}
type addressGetMempoolResp []TxInfoFee

// 'blockchain.address.get_mempool'
func (req *addressGetMempoolReq) Handle(s *Server) (*addressGetMempoolResp, error) {
	address, err := lbcutil.DecodeAddress(req.Address, s.Chain)
	if err != nil {
		return nil, err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, err
	}
	hashX := hashXScript(script, s.Chain)
	// TODO...
	internal.ReverseBytesInPlace(hashX)
	unconfirmed := make([]TxInfoFee, 0, 100)
	result := addressGetMempoolResp(unconfirmed)
	return &result, nil
}

type addressListUnspentReq struct {
	Address string `json:"address"`
}
type TXOInfo struct {
	TxHash string `json:"tx_hash"`
	TxPos  uint16 `json:"tx_pos"`
	Height uint32 `json:"height"`
	Value  uint64 `json:"value"`
}
type addressListUnspentResp []TXOInfo

// 'blockchain.address.listunspent'
func (req *addressListUnspentReq) Handle(s *Server) (*addressListUnspentResp, error) {
	address, err := lbcutil.DecodeAddress(req.Address, s.Chain)
	if err != nil {
		return nil, err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, err
	}
	hashX := hashXScript(script, s.Chain)
	dbTXOs, err := s.DB.GetUnspent(hashX)
	unspent := make([]TXOInfo, 0, len(dbTXOs))
	for _, txo := range dbTXOs {
		unspent = append(unspent,
			TXOInfo{
				TxHash: txo.TxHash.String(),
				TxPos:  txo.TxPos,
				Height: txo.Height,
				Value:  txo.Value,
			})
	}
	result := addressListUnspentResp(unspent)
	return &result, nil
}
