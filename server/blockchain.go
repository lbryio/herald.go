package server

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"

	"github.com/lbryio/lbcd/chaincfg"
	"github.com/lbryio/lbcd/txscript"
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
const HEADER_SIZE = 112
const HASHX_SIZE = 11

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
		return &blockGetHeaderResp{
			Version:       binary.LittleEndian.Uint32(header[0:]),
			PrevBlockHash: hex.EncodeToString(header[4:46]),
			MerkleRoot:    hex.EncodeToString(header[36:68]),
			ClaimTrieRoot: hex.EncodeToString(header[68:100]),
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
	return sum[:HASHX_SIZE]
}

type addressGetBalanceReq struct {
	Address string
}
type addressGetBalanceResp struct {
	Confirmed   uint64
	Unconfirmed uint64
}

// 'blockchain.address.get_balance'
func (req *addressGetBalanceReq) Handle(s *Server) (*addressGetBalanceResp, error) {
	address, err := lbcutil.DecodeAddress(req.Address, s.Coin)
	if err != nil {
		return nil, err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, err
	}
	hashX := hashXScript(script, s.Coin)
	confirmed, unconfirmed, err := s.DB.GetBalance(hashX)
	if err != nil {
		return nil, err
	}
	return &addressGetBalanceResp{confirmed, unconfirmed}, err
}

type addressGetHistoryReq struct {
	Address string
}
type TxInfo struct {
	TxHash string
	Height uint32
}
type TxInfoFee struct {
	TxInfo
	Fee uint64
}
type addressGetHistoryResp struct {
	Confirmed   []TxInfo
	Unconfirmed []TxInfoFee
}

// 'blockchain.address.get_history'
func (req *addressGetHistoryReq) Handle(s *Server) (*addressGetHistoryResp, error) {
	address, err := lbcutil.DecodeAddress(req.Address, s.Coin)
	if err != nil {
		return nil, err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, err
	}
	hashX := hashXScript(script, s.Coin)
	dbTXs, err := s.DB.GetHistory(hashX)
	confirmed := make([]TxInfo, 0, len(dbTXs))
	for _, tx := range dbTXs {
		confirmed = append(confirmed,
			TxInfo{
				TxHash: hex.EncodeToString(tx.TxHash[:]),
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
	Address string
}
type addressGetMempoolResp []TxInfoFee

// 'blockchain.address.get_mempool'
func (req *addressGetMempoolReq) Handle(s *Server) (*addressGetMempoolResp, error) {
	address, err := lbcutil.DecodeAddress(req.Address, s.Coin)
	if err != nil {
		return nil, err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, err
	}
	// TODO...
	hashX := hashXScript(script, s.Coin)
	dbTXs, err := s.DB.GetHistory(hashX)
	confirmed := make([]TxInfo, 0, len(dbTXs))
	for _, tx := range dbTXs {
		confirmed = append(confirmed,
			TxInfo{
				TxHash: hex.EncodeToString(tx.TxHash[:]),
				Height: tx.Height,
			})
	}
	unconfirmed := make([]TxInfoFee, 0, 100)
	result := addressGetMempoolResp(unconfirmed)
	return &result, nil
}

type addressListUnspentReq struct {
	Address string
}
type TXOInfo struct {
	TxHash string
	TxPos  uint16
	Height uint32
	Value  uint64
}
type addressListUnspentResp []TXOInfo

// 'blockchain.address.listunspent'
func (req *addressListUnspentReq) Handle(s *Server) (*addressListUnspentResp, error) {
	address, err := lbcutil.DecodeAddress(req.Address, s.Coin)
	if err != nil {
		return nil, err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return nil, err
	}
	hashX := hashXScript(script, s.Coin)
	dbTXOs, err := s.DB.GetUnspent(hashX)
	unspent := make([]TXOInfo, 0, len(dbTXOs))
	for _, txo := range dbTXOs {
		unspent = append(unspent,
			TXOInfo{
				TxHash: hex.EncodeToString(txo.TxHash[:]),
				TxPos:  txo.TxPos,
				Height: txo.Height,
				Value:  txo.Value,
			})
	}
	result := addressListUnspentResp(unspent)
	return &result, nil
}
