package server

import (
	"bytes"
	"compress/zlib"
	"crypto/sha256"
	"encoding/base64"
	"encoding/binary"
	"encoding/hex"
	"errors"
	"fmt"
	"net/http"

	"github.com/lbryio/herald.go/db"
	"github.com/lbryio/herald.go/internal"
	"github.com/lbryio/lbcd/chaincfg"
	"github.com/lbryio/lbcd/chaincfg/chainhash"
	"github.com/lbryio/lbcd/txscript"
	"github.com/lbryio/lbcd/wire"
	"github.com/lbryio/lbcutil"
	"golang.org/x/exp/constraints"
)

// BlockchainBlockService methods handle "blockchain.block.*" RPCs
type BlockchainBlockService struct {
	DB    *db.ReadOnlyDBColumnFamily
	Chain *chaincfg.Params
}

// BlockchainAddressService methods handle "blockchain.address.*" RPCs
type BlockchainAddressService struct {
	BlockchainBlockService
}

// BlockchainScripthashService methods handle "blockchain.scripthash.*" RPCs
type BlockchainScripthashService struct {
	BlockchainBlockService
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

type BlockHeaderElectrum struct {
	Version       uint32 `json:"version"`
	PrevBlockHash string `json:"prev_block_hash"`
	MerkleRoot    string `json:"merkle_root"`
	ClaimTrieRoot string `json:"claim_trie_root"`
	Timestamp     uint32 `json:"timestamp"`
	Bits          uint32 `json:"bits"`
	Nonce         uint32 `json:"nonce"`
	BlockHeight   uint32 `json:"block_height"`
}

func newBlockHeaderElectrum(header *[HEADER_SIZE]byte, height uint32) *BlockHeaderElectrum {
	var h1, h2, h3 chainhash.Hash
	h1.SetBytes(header[4:36])
	h2.SetBytes(header[36:68])
	h3.SetBytes(header[68:100])
	return &BlockHeaderElectrum{
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

type BlockGetServerHeightReq struct{}
type BlockGetServerHeightResp uint32

func (s *BlockchainBlockService) Get_server_height(r *http.Request, req *BlockGetServerHeightReq, resp **BlockGetServerHeightResp) error {
	if s.DB == nil || s.DB.LastState == nil {
		return fmt.Errorf("unknown height")
	}
	result := BlockGetServerHeightResp(s.DB.LastState.Height)
	*resp = &result
	return nil
}

type BlockGetChunkReq uint32
type BlockGetChunkResp string

// 'blockchain.block.get_chunk'
func (s *BlockchainBlockService) Get_chunk(r *http.Request, req *BlockGetChunkReq, resp **BlockGetChunkResp) error {
	index := uint32(*req)
	db_headers, err := s.DB.GetHeaders(index*CHUNK_SIZE, CHUNK_SIZE)
	if err != nil {
		return err
	}
	raw := make([]byte, 0, HEADER_SIZE*len(db_headers))
	for _, h := range db_headers {
		raw = append(raw, h[:]...)
	}
	headers := BlockGetChunkResp(hex.EncodeToString(raw))
	*resp = &headers
	return err
}

type BlockGetHeaderReq uint32
type BlockGetHeaderResp struct {
	BlockHeaderElectrum
}

// 'blockchain.block.get_header'
func (s *BlockchainBlockService) Get_header(r *http.Request, req *BlockGetHeaderReq, resp **BlockGetHeaderResp) error {
	height := uint32(*req)
	headers, err := s.DB.GetHeaders(height, 1)
	if err != nil {
		return err
	}
	if len(headers) < 1 {
		return errors.New("not found")
	}
	*resp = &BlockGetHeaderResp{*newBlockHeaderElectrum(&headers[0], height)}
	return err
}

type BlockHeadersReq struct {
	StartHeight uint32 `json:"start_height"`
	Count       uint32 `json:"count"`
	CpHeight    uint32 `json:"cp_height"`
	B64         bool   `json:"b64"`
}

type BlockHeadersResp struct {
	Base64 string `json:"base64,omitempty"`
	Hex    string `json:"hex,omitempty"`
	Count  uint32 `json:"count"`
	Max    uint32 `json:"max"`
	Branch string `json:"branch,omitempty"`
	Root   string `json:"root,omitempty"`
}

// 'blockchain.block.headers'
func (s *BlockchainBlockService) Headers(r *http.Request, req *BlockHeadersReq, resp **BlockHeadersResp) error {
	count := min(req.Count, MAX_CHUNK_SIZE)
	db_headers, err := s.DB.GetHeaders(req.StartHeight, count)
	if err != nil {
		return err
	}
	count = uint32(len(db_headers))
	raw := make([]byte, 0, HEADER_SIZE*count)
	for _, h := range db_headers {
		raw = append(raw, h[:]...)
	}
	result := &BlockHeadersResp{
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
	*resp = result
	return err
}

func decodeScriptHash(scripthash string) ([]byte, error) {
	sh, err := hex.DecodeString(scripthash)
	if err != nil {
		return nil, err
	}
	if len(sh) != chainhash.HashSize {
		return nil, fmt.Errorf("invalid scripthash: %v (length %v)", scripthash, len(sh))
	}
	internal.ReverseBytesInPlace(sh)
	return sh, nil
}

func hashX(scripthash []byte) []byte {
	return scripthash[:HASHX_LEN]
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

type AddressGetBalanceReq struct {
	Address string `json:"address"`
}
type AddressGetBalanceResp struct {
	Confirmed   uint64 `json:"confirmed"`
	Unconfirmed uint64 `json:"unconfirmed"`
}

// 'blockchain.address.get_balance'
func (s *BlockchainAddressService) Get_balance(r *http.Request, req *AddressGetBalanceReq, resp **AddressGetBalanceResp) error {
	address, err := lbcutil.DecodeAddress(req.Address, s.Chain)
	if err != nil {
		return err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return err
	}
	hashX := hashXScript(script, s.Chain)
	confirmed, unconfirmed, err := s.DB.GetBalance(hashX)
	if err != nil {
		return err
	}
	*resp = &AddressGetBalanceResp{confirmed, unconfirmed}
	return err
}

type scripthashGetBalanceReq struct {
	ScriptHash string `json:"scripthash"`
}
type ScripthashGetBalanceResp struct {
	Confirmed   uint64 `json:"confirmed"`
	Unconfirmed uint64 `json:"unconfirmed"`
}

// 'blockchain.scripthash.get_balance'
func (s *BlockchainScripthashService) Get_balance(r *http.Request, req *scripthashGetBalanceReq, resp **ScripthashGetBalanceResp) error {
	scripthash, err := decodeScriptHash(req.ScriptHash)
	if err != nil {
		return err
	}
	hashX := hashX(scripthash)
	confirmed, unconfirmed, err := s.DB.GetBalance(hashX)
	if err != nil {
		return err
	}
	*resp = &ScripthashGetBalanceResp{confirmed, unconfirmed}
	return err
}

type AddressGetHistoryReq struct {
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
type AddressGetHistoryResp struct {
	Confirmed   []TxInfo    `json:"confirmed"`
	Unconfirmed []TxInfoFee `json:"unconfirmed"`
}

// 'blockchain.address.get_history'
func (s *BlockchainAddressService) Get_history(r *http.Request, req *AddressGetHistoryReq, resp **AddressGetHistoryResp) error {
	address, err := lbcutil.DecodeAddress(req.Address, s.Chain)
	if err != nil {
		return err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return err
	}
	hashX := hashXScript(script, s.Chain)
	dbTXs, err := s.DB.GetHistory(hashX)
	if err != nil {
		return err
	}
	confirmed := make([]TxInfo, 0, len(dbTXs))
	for _, tx := range dbTXs {
		confirmed = append(confirmed,
			TxInfo{
				TxHash: tx.TxHash.String(),
				Height: tx.Height,
			})
	}
	result := &AddressGetHistoryResp{
		Confirmed:   confirmed,
		Unconfirmed: []TxInfoFee{}, // TODO
	}
	*resp = result
	return err
}

type ScripthashGetHistoryReq struct {
	ScriptHash string `json:"scripthash"`
}
type ScripthashGetHistoryResp struct {
	Confirmed   []TxInfo    `json:"confirmed"`
	Unconfirmed []TxInfoFee `json:"unconfirmed"`
}

// 'blockchain.scripthash.get_history'
func (s *BlockchainScripthashService) Get_history(r *http.Request, req *ScripthashGetHistoryReq, resp **ScripthashGetHistoryResp) error {
	scripthash, err := decodeScriptHash(req.ScriptHash)
	if err != nil {
		return err
	}
	hashX := hashX(scripthash)
	dbTXs, err := s.DB.GetHistory(hashX)
	if err != nil {
		return err
	}
	confirmed := make([]TxInfo, 0, len(dbTXs))
	for _, tx := range dbTXs {
		confirmed = append(confirmed,
			TxInfo{
				TxHash: tx.TxHash.String(),
				Height: tx.Height,
			})
	}
	result := &ScripthashGetHistoryResp{
		Confirmed:   confirmed,
		Unconfirmed: []TxInfoFee{}, // TODO
	}
	*resp = result
	return err
}

type AddressGetMempoolReq struct {
	Address string `json:"address"`
}
type AddressGetMempoolResp []TxInfoFee

// 'blockchain.address.get_mempool'
func (s *BlockchainAddressService) Get_mempool(r *http.Request, req *AddressGetMempoolReq, resp **AddressGetMempoolResp) error {
	address, err := lbcutil.DecodeAddress(req.Address, s.Chain)
	if err != nil {
		return err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return err
	}
	hashX := hashXScript(script, s.Chain)
	// TODO...
	internal.ReverseBytesInPlace(hashX)
	unconfirmed := make([]TxInfoFee, 0, 100)
	result := AddressGetMempoolResp(unconfirmed)
	*resp = &result
	return err
}

type ScripthashGetMempoolReq struct {
	ScriptHash string `json:"scripthash"`
}
type ScripthashGetMempoolResp []TxInfoFee

// 'blockchain.scripthash.get_mempool'
func (s *BlockchainScripthashService) Get_mempool(r *http.Request, req *ScripthashGetMempoolReq, resp **ScripthashGetMempoolResp) error {
	scripthash, err := decodeScriptHash(req.ScriptHash)
	if err != nil {
		return err
	}
	hashX := hashX(scripthash)
	// TODO...
	internal.ReverseBytesInPlace(hashX)
	unconfirmed := make([]TxInfoFee, 0, 100)
	result := ScripthashGetMempoolResp(unconfirmed)
	*resp = &result
	return err
}

type AddressListUnspentReq struct {
	Address string `json:"address"`
}
type TXOInfo struct {
	TxHash string `json:"tx_hash"`
	TxPos  uint16 `json:"tx_pos"`
	Height uint32 `json:"height"`
	Value  uint64 `json:"value"`
}
type AddressListUnspentResp []TXOInfo

// 'blockchain.address.listunspent'
func (s *BlockchainAddressService) Listunspent(r *http.Request, req *AddressListUnspentReq, resp **AddressListUnspentResp) error {
	address, err := lbcutil.DecodeAddress(req.Address, s.Chain)
	if err != nil {
		return err
	}
	script, err := txscript.PayToAddrScript(address)
	if err != nil {
		return err
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
	result := AddressListUnspentResp(unspent)
	*resp = &result
	return err
}

type ScripthashListUnspentReq struct {
	ScriptHash string `json:"scripthash"`
}
type ScripthashListUnspentResp []TXOInfo

// 'blockchain.scripthash.listunspent'
func (s *BlockchainScripthashService) Listunspent(r *http.Request, req *ScripthashListUnspentReq, resp **ScripthashListUnspentResp) error {
	scripthash, err := decodeScriptHash(req.ScriptHash)
	if err != nil {
		return err
	}
	hashX := hashX(scripthash)
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
	result := ScripthashListUnspentResp(unspent)
	*resp = &result
	return err
}
