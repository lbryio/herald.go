package prefixes

// The prefixes package contains the key/value structures for the rocksdb prefix
// store and related functions.
// Each key/value prefix pair has a pack and unpack function for serializing / deserializing
// it into bytes for storage in rocksdb. They also have a pack partial function which allows
// for serialization of a set number of fields, this is used i.e. for keys with a hash, tx_num
// and height where there multiple be multiple with the same hash but different tx_num and height.

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"

	"github.com/lbryio/herald.go/internal"
	"github.com/lbryio/lbcd/chaincfg/chainhash"
)

const (
	ClaimToSupport = 'K'
	SupportToClaim = 'L'

	ClaimToTXO = 'E'
	TXOToClaim = 'G'

	ClaimToChannel = 'I'
	ChannelToClaim = 'J'

	ClaimShortIdPrefix = 'F'
	EffectiveAmount    = 'D'
	ClaimExpiration    = 'O'

	ClaimTakeover            = 'P'
	PendingActivation        = 'Q'
	ActivatedClaimAndSupport = 'R'
	ActiveAmount             = 'S'

	Repost        = 'V'
	RepostedClaim = 'W'

	Undo      = 'M'
	ClaimDiff = 'Y'

	Tx            = 'B'
	BlockHash     = 'C'
	Header        = 'H'
	TxNum         = 'N'
	TxCount       = 'T'
	TxHash        = 'X'
	UTXO          = 'u'
	HashXUTXO     = 'h'
	HashXHistory  = 'x'
	DBState       = 's'
	ChannelCount  = 'Z'
	SupportAmount = 'a'
	BlockTXs      = 'b'

	TrendingNotifications = 'c'
	MempoolTx             = 'd'
	TouchedHashX          = 'e'
	HashXStatus           = 'f'
	HashXMempoolStatus    = 'g'

	ActivateClaimTXOType    = 1
	ActivatedSupportTXOType = 2

	OnesCompTwiddle64 uint64 = 0xffffffffffffffff
	OnesCompTwiddle32 uint32 = 0xffffffff
)

// GetPrefixes returns an array of all the byte prefix constants.
func GetPrefixes() [][]byte {
	return [][]byte{
		{ClaimToSupport},
		{SupportToClaim},
		{ClaimToTXO},
		{TXOToClaim},
		{ClaimToChannel},
		{ChannelToClaim},
		{ClaimShortIdPrefix},
		{EffectiveAmount},
		{ClaimExpiration},
		{ClaimTakeover},
		{PendingActivation},
		{ActivatedClaimAndSupport},
		{ActiveAmount},
		{Repost},
		{RepostedClaim},
		{Undo},
		{ClaimDiff},
		{Tx},
		{BlockHash},
		{Header},
		{TxNum},
		{TxCount},
		{TxHash},
		{UTXO},
		{HashXUTXO},
		{HashXHistory},
		{DBState},
		{ChannelCount},
		{SupportAmount},
		{BlockTXs},
		{TrendingNotifications},
		{MempoolTx},
		{TouchedHashX},
		{HashXStatus},
		{HashXMempoolStatus},
	}
}

// PrefixRowKV is a generic key/value pair for a prefix.
type PrefixRowKV struct {
	Key      BaseKey
	Value    BaseValue
	RawKey   []byte
	RawValue []byte
}

type BaseKey interface {
	NumFields() int
	PartialPack(fields int) []byte
	PackKey() []byte
}

type BaseValue interface {
	PackValue() []byte
}

type KeyUnpacker interface {
	UnpackKey(buf []byte)
}

type ValueUnpacker interface {
	UnpackValue(buf []byte)
}

type LengthEncodedName struct {
	NameLen uint16 `struct:"sizeof=Name"`
	Name    string `json:"name"`
}

func NewLengthEncodedName(s string) LengthEncodedName {
	return LengthEncodedName{
		NameLen: uint16(len(s)),
		Name:    s,
	}
}

type LengthEncodedNormalizedName struct {
	NormalizedNameLen uint16 `struct:"sizeof=NormalizedName"`
	NormalizedName    string `json:"normalized_name"`
}

func NewLengthEncodedNormalizedName(s string) LengthEncodedNormalizedName {
	return LengthEncodedNormalizedName{
		NormalizedNameLen: uint16(len(s)),
		NormalizedName:    s,
	}
}

type LengthEncodedPartialClaimId struct {
	PartialClaimIdLen uint8  `struct:"sizeof=PartialClaimId"`
	PartialClaimId    string `json:"partial_claim_id"`
}

func NewLengthEncodedPartialClaimId(s string) LengthEncodedPartialClaimId {
	return LengthEncodedPartialClaimId{
		PartialClaimIdLen: uint8(len(s)),
		PartialClaimId:    s,
	}
}

type DBStateKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
}

type DBStateValue struct {
	Genesis        *chainhash.Hash
	Height         uint32
	TxCount        uint32
	Tip            *chainhash.Hash
	UtxoFlushCount uint32
	WallTime       uint32
	FirstSync      bool
	DDVersion      uint8
	HistFlushCount int32
	CompFlushCount int32
	CompCursor     int32
	EsSyncHeight   uint32
}

func NewDBStateValue() *DBStateValue {
	return &DBStateValue{
		Genesis:        new(chainhash.Hash),
		Height:         0,
		TxCount:        0,
		Tip:            new(chainhash.Hash),
		UtxoFlushCount: 0,
		WallTime:       0,
		FirstSync:      true,
		DDVersion:      0,
		HistFlushCount: 0,
		CompFlushCount: -1,
		CompCursor:     -1,
		EsSyncHeight:   0,
	}
}

func NewDBStateKey() *DBStateKey {
	return &DBStateKey{
		Prefix: []byte{DBState},
	}
}

func (k *DBStateKey) PackKey() []byte {
	prefixLen := 1
	n := prefixLen
	key := make([]byte, n)
	copy(key, k.Prefix)

	return key
}

func (v *DBStateValue) PackValue() []byte {
	// b'>32sLL32sLLBBlllL'
	n := 32 + 4 + 4 + 32 + 4 + 4 + 1 + 1 + 4 + 4 + 4 + 4
	value := make([]byte, n)
	copy(value, v.Genesis[:32])
	binary.BigEndian.PutUint32(value[32:], v.Height)
	binary.BigEndian.PutUint32(value[32+4:], v.TxCount)
	copy(value[32+4+4:], v.Tip[:32])
	binary.BigEndian.PutUint32(value[32+4+4+32:], v.UtxoFlushCount)
	binary.BigEndian.PutUint32(value[32+4+4+32+4:], v.WallTime)
	var bitSetVar uint8
	if v.FirstSync {
		bitSetVar = 1
	}
	value[32+4+4+32+4+4] = bitSetVar
	value[32+4+4+32+4+4+1] = v.DDVersion

	binary.BigEndian.PutUint32(value[32+4+4+32+4+4+1+1:], uint32(v.HistFlushCount))
	binary.BigEndian.PutUint32(value[32+4+4+32+4+4+1+1+4:], uint32(v.CompFlushCount))
	binary.BigEndian.PutUint32(value[32+4+4+32+4+4+1+1+4+4:], uint32(v.CompCursor))
	binary.BigEndian.PutUint32(value[32+4+4+32+4+4+1+1+4+4+4:], v.EsSyncHeight)

	return value
}

func (kv *DBStateKey) NumFields() int {
	return 0
}

func (k *DBStateKey) PartialPack(fields int) []byte {
	prefixLen := 1
	var n = prefixLen

	key := make([]byte, n)
	copy(key, k.Prefix)

	return key
}

func DBStateKeyUnpack(key []byte) *DBStateKey {
	prefixLen := 1
	return &DBStateKey{
		Prefix: key[:prefixLen],
	}
}

func DBStateValueUnpack(value []byte) *DBStateValue {
	genesis := (*chainhash.Hash)(value[:32])
	tip := (*chainhash.Hash)(value[32+4+4 : 32+4+4+32])
	x := &DBStateValue{
		Genesis:        genesis,
		Height:         binary.BigEndian.Uint32(value[32:]),
		TxCount:        binary.BigEndian.Uint32(value[32+4:]),
		Tip:            tip,
		UtxoFlushCount: binary.BigEndian.Uint32(value[32+4+4+32:]),
		WallTime:       binary.BigEndian.Uint32(value[32+4+4+32+4:]),
		FirstSync:      value[32+4+4+32+4+4] == 1,
		DDVersion:      value[32+4+4+32+4+4+1],
		HistFlushCount: int32(binary.BigEndian.Uint32(value[32+4+4+32+4+4+1+1:])),
		CompFlushCount: int32(binary.BigEndian.Uint32(value[32+4+4+32+4+4+1+1+4:])),
		CompCursor:     int32(binary.BigEndian.Uint32(value[32+4+4+32+4+4+1+1+4+4:])),
		EsSyncHeight:   binary.BigEndian.Uint32(value[32+4+4+32+4+4+1+1+4+4+4:]),
	}
	return x
}

type UndoKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	Height uint64 `json:"height"`
}

type UndoValue struct {
	Data []byte `struct-while:"!_eof" json:"data"`
}

func (k *UndoKey) PackKey() []byte {
	prefixLen := 1
	// b'>L'
	n := prefixLen + 8
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint64(key[prefixLen:], k.Height)

	return key
}

func (v *UndoValue) PackValue() []byte {
	len := len(v.Data)
	value := make([]byte, len)
	copy(value, v.Data)

	return value
}

func (kv *UndoKey) NumFields() int {
	return 1
}

func (k *UndoKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 8
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint64(key[prefixLen:], k.Height)
		}
	}

	return key
}

func UndoKeyUnpack(key []byte) *UndoKey {
	prefixLen := 1
	return &UndoKey{
		Prefix: key[:prefixLen],
		Height: binary.BigEndian.Uint64(key[prefixLen:]),
	}
}

func UndoValueUnpack(value []byte) *UndoValue {
	return &UndoValue{
		Data: value,
	}
}

type UTXOKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	HashX  []byte `struct:"[11]byte" json:"hashx"`
	TxNum  uint32 `json:"tx_num"`
	Nout   uint16 `json:"nout"`
}

type UTXOValue struct {
	Amount uint64 `json:"amount"`
}

type HashXUTXOKey struct {
	Prefix      []byte `struct:"[1]byte" json:"prefix"`
	ShortTXHash []byte `struct:"[4]byte" json:"short_tx_hash"`
	TxNum       uint32 `json:"tx_num"`
	Nout        uint16 `json:"nout"`
}

type HashXUTXOValue struct {
	HashX []byte `struct:"[11]byte" json:"hashx"`
}

//
// HashXUTXOKey / HashXUTXOValue
//

func (k *HashXUTXOKey) String() string {
	return fmt.Sprintf(
		"%s(short_tx_hash=%s, tx_num=%d, nout=%d)",
		reflect.TypeOf(k),
		hex.EncodeToString(k.ShortTXHash),
		k.TxNum,
		k.Nout,
	)
}

func (v *HashXUTXOValue) String() string {
	return fmt.Sprintf(
		"%s(hashX=%s)",
		reflect.TypeOf(v),
		hex.EncodeToString(v.HashX),
	)
}

func (k *HashXUTXOKey) PackKey() []byte {
	prefixLen := 1
	// b'>4sLH'
	n := prefixLen + 4 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.ShortTXHash)
	binary.BigEndian.PutUint32(key[prefixLen+4:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+8:], k.Nout)

	return key
}

func (v *HashXUTXOValue) PackValue() []byte {
	value := make([]byte, 11)
	copy(value, v.HashX)

	return value
}

func (kv *HashXUTXOKey) NumFields() int {
	return 3
}

// HashXUTXOKeyPackPartial packs a variable number of fields into a byte
// array
func (k *HashXUTXOKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 3 {
		fields = 3
	}
	if fields < 0 {
		fields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		case 2:
			n += 4
		case 3:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.ShortTXHash)
		case 2:
			binary.BigEndian.PutUint32(key[prefixLen+4:], k.TxNum)
		case 3:
			binary.BigEndian.PutUint16(key[prefixLen+8:], k.Nout)
		}
	}

	return key
}

func HashXUTXOKeyUnpack(key []byte) *HashXUTXOKey {
	prefixLen := 1
	return &HashXUTXOKey{
		Prefix:      key[:prefixLen],
		ShortTXHash: key[prefixLen : prefixLen+4],
		TxNum:       binary.BigEndian.Uint32(key[prefixLen+4:]),
		Nout:        binary.BigEndian.Uint16(key[prefixLen+8:]),
	}
}

func HashXUTXOValueUnpack(value []byte) *HashXUTXOValue {
	return &HashXUTXOValue{
		HashX: value[:11],
	}
}

type HashXHistoryKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	HashX  []byte `struct:"[11]byte" json:"hashx"`
	Height uint32 `json:"height"`
}

type HashXHistoryValue struct {
	HashXes []uint16 `struct-while:"!_eof" json:"hashxes"`
}

func (k *HashXHistoryKey) String() string {
	return fmt.Sprintf(
		"%s(hashx=%s, height=%d)",
		reflect.TypeOf(k),
		hex.EncodeToString(k.HashX),
		k.Height,
	)
}

func (k *HashXHistoryKey) PackKey() []byte {
	prefixLen := 1
	// b'>11sL'
	n := prefixLen + 11 + 4
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.HashX)
	binary.BigEndian.PutUint32(key[prefixLen+11:], k.Height)

	return key
}

func (v *HashXHistoryValue) PackValue() []byte {
	n := len(v.HashXes)
	value := make([]byte, n*2)
	for i, x := range v.HashXes {
		binary.BigEndian.PutUint16(value[i*2:], x)
	}

	return value
}

func (kv *HashXHistoryKey) NumFields() int {
	return 2
}

// HashXHistoryKeyPackPartial packs a variable number of fields into a byte
// array
func (k *HashXHistoryKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 2 {
		fields = 2
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 11
		case 2:
			n += 4
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.HashX[:11])
		case 2:
			binary.BigEndian.PutUint32(key[prefixLen+11:], k.Height)
		}
	}

	return key
}

func HashXHistoryKeyUnpack(key []byte) *HashXHistoryKey {
	prefixLen := 1
	return &HashXHistoryKey{
		Prefix: key[:prefixLen],
		HashX:  key[prefixLen : prefixLen+11],
		Height: binary.BigEndian.Uint32(key[prefixLen+11:]),
	}
}

func HashXHistoryValueUnpack(value []byte) *HashXHistoryValue {
	n := len(value) / 2
	hashxes := make([]uint16, n)
	for i := 0; i < n; i++ {
		hashxes[i] = binary.BigEndian.Uint16(value[i*2:])
	}
	return &HashXHistoryValue{
		HashXes: hashxes,
	}
}

type BlockHashKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	Height uint32 `json:"height"`
}

type BlockHashValue struct {
	BlockHash *chainhash.Hash `json:"block_hash"`
}

func NewBlockHashKey(height uint32) *BlockHashKey {
	return &BlockHashKey{
		Prefix: []byte{BlockHash},
		Height: height,
	}
}

func (k *BlockHashKey) PackKey() []byte {
	prefixLen := 1
	// b'>L'
	n := prefixLen + 4
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint32(key[prefixLen:], k.Height)

	return key
}

func (v *BlockHashValue) PackValue() []byte {
	value := make([]byte, 32)
	copy(value, v.BlockHash[:32])

	return value
}

func (kv *BlockHashKey) NumFields() int {
	return 1
}

func (k *BlockHashKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		case 2:
			n += 4
		case 3:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint32(key[prefixLen:], k.Height)
		}
	}

	return key
}

func BlockHashKeyUnpack(key []byte) *BlockHashKey {
	prefixLen := 1
	return &BlockHashKey{
		Prefix: key[:prefixLen],
		Height: binary.BigEndian.Uint32(key[prefixLen:]),
	}
}

func BlockHashValueUnpack(value []byte) *BlockHashValue {
	hash := (*chainhash.Hash)(value)
	return &BlockHashValue{
		BlockHash: hash,
	}
}

type BlockTxsKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	Height uint32 `json:"height"`
}

type BlockTxsValue struct {
	TxHashes []*chainhash.Hash `struct-while:"!_eof" json:"tx_hashes"`
}

func (k *BlockTxsKey) NewBlockTxsKey(height uint32) *BlockTxsKey {
	return &BlockTxsKey{
		Prefix: []byte{BlockTXs},
		Height: height,
	}
}

func (k *BlockTxsKey) PackKey() []byte {
	prefixLen := 1
	// b'>L'
	n := prefixLen + 4
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint32(key[prefixLen:], k.Height)

	return key
}

func (v *BlockTxsValue) PackValue() []byte {
	numHashes := len(v.TxHashes)
	n := numHashes * 32
	value := make([]byte, n)

	for i, tx := range v.TxHashes {
		if len(tx) != 32 {
			log.Println("Warning, txhash not 32 bytes", tx)
			return nil
		}
		copy(value[i*32:], tx[:])
	}

	return value
}

func (kv *BlockTxsKey) NumFields() int {
	return 1
}

func (k *BlockTxsKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint32(key[prefixLen:], k.Height)
		}
	}

	return key
}

func BlockTxsKeyUnpack(key []byte) *BlockTxsKey {
	prefixLen := 1
	return &BlockTxsKey{
		Prefix: key[:prefixLen],
		Height: binary.BigEndian.Uint32(key[prefixLen:]),
	}
}

func BlockTxsValueUnpack(value []byte) *BlockTxsValue {
	numHashes := len(value) / 32
	txs := make([]*chainhash.Hash, numHashes)
	for i := 0; i < numHashes; i++ {
		txs[i] = (*chainhash.Hash)(value[i*32 : (i+1)*32])
	}
	return &BlockTxsValue{
		TxHashes: txs,
	}
}

type TxCountKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	Height uint32 `json:"height"`
}

type TxCountValue struct {
	TxCount uint32 `json:"tx_count"`
}

func NewTxCountKey(height uint32) *TxCountKey {
	return &TxCountKey{
		Prefix: []byte{TxCount},
		Height: height,
	}
}

func (k *TxCountKey) PackKey() []byte {
	prefixLen := 1
	// b'>L'
	n := prefixLen + 4
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint32(key[prefixLen:], k.Height)

	return key
}

func (v *TxCountValue) PackValue() []byte {
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, v.TxCount)

	return value
}

func (kv *TxCountKey) NumFields() int {
	return 1
}

func (k *TxCountKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint32(key[prefixLen:], k.Height)
		}
	}

	return key
}

func TxCountKeyUnpack(key []byte) *TxCountKey {
	prefixLen := 1
	return &TxCountKey{
		Prefix: key[:prefixLen],
		Height: binary.BigEndian.Uint32(key[prefixLen:]),
	}
}

func TxCountValueUnpack(value []byte) *TxCountValue {
	return &TxCountValue{
		TxCount: binary.BigEndian.Uint32(value),
	}
}

type TxHashKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	TxNum  uint32 `json:"tx_num"`
}

type TxHashValue struct {
	TxHash *chainhash.Hash `json:"tx_hash"`
}

func NewTxHashKey(txNum uint32) *TxHashKey {
	return &TxHashKey{
		Prefix: []byte{TxHash},
		TxNum:  txNum,
	}
}

func (k *TxHashKey) PackKey() []byte {
	prefixLen := 1
	// b'>L'
	n := prefixLen + 4
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint32(key[prefixLen:], k.TxNum)

	return key
}

func (v *TxHashValue) PackValue() []byte {
	n := len(v.TxHash)
	value := make([]byte, n)
	copy(value, v.TxHash[:n])

	return value
}

func (kv *TxHashKey) NumFields() int {
	return 1
}

func (k *TxHashKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint32(key[prefixLen:], k.TxNum)
		}
	}

	return key
}

func TxHashKeyUnpack(key []byte) *TxHashKey {
	prefixLen := 1
	return &TxHashKey{
		Prefix: key[:prefixLen],
		TxNum:  binary.BigEndian.Uint32(key[prefixLen:]),
	}
}

func TxHashValueUnpack(value []byte) *TxHashValue {
	return &TxHashValue{
		TxHash: (*chainhash.Hash)(value),
	}
}

type TxNumKey struct {
	Prefix []byte          `struct:"[1]byte" json:"prefix"`
	TxHash *chainhash.Hash `json:"tx_hash"`
}

type TxNumValue struct {
	TxNum uint32 `json:"tx_num"`
}

func (k *TxNumKey) PackKey() []byte {
	prefixLen := 1
	// b'>L'
	n := prefixLen + 32
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.TxHash[:32])

	return key
}

func (v *TxNumValue) PackValue() []byte {
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, v.TxNum)

	return value
}

func (kv *TxNumKey) NumFields() int {
	return 1
}

func (k *TxNumKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 32
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.TxHash[:32])
		}
	}

	return key
}

func TxNumKeyUnpack(key []byte) *TxNumKey {
	prefixLen := 1
	return &TxNumKey{
		Prefix: key[:prefixLen],
		TxHash: (*chainhash.Hash)(key[prefixLen : prefixLen+32]),
	}
}

func TxNumValueUnpack(value []byte) *TxNumValue {
	return &TxNumValue{
		TxNum: binary.BigEndian.Uint32(value),
	}
}

type TxKey struct {
	Prefix []byte          `struct:"[1]byte" json:"prefix"`
	TxHash *chainhash.Hash `struct:"*[32]byte" json:"tx_hash"`
}

type TxValue struct {
	RawTx []byte `struct-while:"!_eof" json:"raw_tx"`
}

func (k *TxKey) PackKey() []byte {
	prefixLen := 1
	// b'>L'
	n := prefixLen + 32
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.TxHash[:32])

	return key
}

func (v *TxValue) PackValue() []byte {
	value := make([]byte, len(v.RawTx))
	copy(value, v.RawTx)

	return value
}

func (kv *TxKey) NumFields() int {
	return 1
}

func (k *TxKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 32
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.TxHash[:32])
		}
	}

	return key
}

func TxKeyUnpack(key []byte) *TxKey {
	prefixLen := 1
	return &TxKey{
		Prefix: key[:prefixLen],
		TxHash: (*chainhash.Hash)(key[prefixLen : prefixLen+32]),
	}
}

func TxValueUnpack(value []byte) *TxValue {
	return &TxValue{
		RawTx: value,
	}
}

type BlockHeaderKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	Height uint32 `json:"height"`
}

type BlockHeaderValue struct {
	Header []byte `struct:"[112]byte" json:"header"`
}

func (k *BlockHeaderValue) Equals(v *BlockHeaderValue) bool {
	return bytes.Equal(k.Header, v.Header)
}

func NewHeaderKey(height uint32) *BlockHeaderKey {
	return &BlockHeaderKey{
		Prefix: []byte{Header},
		Height: height,
	}
}

func (k *BlockHeaderKey) PackKey() []byte {
	prefixLen := 1
	// b'>L'
	n := prefixLen + 4
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint32(key[prefixLen:], k.Height)

	return key
}

func (v *BlockHeaderValue) PackValue() []byte {
	value := make([]byte, 112)
	copy(value, v.Header)

	return value
}

func (kv *BlockHeaderKey) NumFields() int {
	return 1
}

func (k *BlockHeaderKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint32(key[prefixLen:], k.Height)
		}
	}

	return key
}

func BlockHeaderKeyUnpack(key []byte) *BlockHeaderKey {
	prefixLen := 1
	return &BlockHeaderKey{
		Prefix: key[:prefixLen],
		Height: binary.BigEndian.Uint32(key[prefixLen:]),
	}
}

func BlockHeaderValueUnpack(value []byte) *BlockHeaderValue {
	return &BlockHeaderValue{
		Header: value[:112],
	}
}

type ClaimToTXOKey struct {
	Prefix    []byte `struct:"[1]byte" json:"prefix"`
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
}

type ClaimToTXOValue struct {
	TxNum                   uint32 `json:"tx_num"`
	Position                uint16 `json:"position"`
	RootTxNum               uint32 `json:"root_tx_num"`
	RootPosition            uint16 `json:"root_position"`
	Amount                  uint64 `json:"amount"`
	ChannelSignatureIsValid bool   `json:"channel_signature_is_valid"`
	LengthEncodedName
}

func NewClaimToTXOKey(claimHash []byte) *ClaimToTXOKey {
	return &ClaimToTXOKey{
		Prefix:    []byte{ClaimToTXO},
		ClaimHash: claimHash,
	}
}

func (v *ClaimToTXOValue) NormalizedName() string {
	//TODO implement? Might not need to do anything.
	return internal.NormalizeName(v.Name)
}

func (k *ClaimToTXOKey) PackKey() []byte {
	prefixLen := 1
	// b'>20s'
	n := prefixLen + 20
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.ClaimHash[:20])

	return key
}

func (v *ClaimToTXOValue) PackValue() []byte {
	nameLen := len(v.Name)
	n := 4 + 2 + 4 + 2 + 8 + 1 + 2 + nameLen
	value := make([]byte, n)
	binary.BigEndian.PutUint32(value, v.TxNum)
	binary.BigEndian.PutUint16(value[4:], v.Position)
	binary.BigEndian.PutUint32(value[6:], v.RootTxNum)
	binary.BigEndian.PutUint16(value[10:], v.RootPosition)
	binary.BigEndian.PutUint64(value[12:], v.Amount)
	var bitSetVar uint8
	if v.ChannelSignatureIsValid {
		bitSetVar = 1
	}
	value[20] = bitSetVar
	binary.BigEndian.PutUint16(value[21:], uint16(nameLen))
	copy(value[23:], []byte(v.Name[:nameLen]))

	return value
}

func (kv *ClaimToTXOKey) NumFields() int {
	return 1
}

func (k *ClaimToTXOKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 20
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.ClaimHash[:20])
		}
	}

	return key
}

func ClaimToTXOKeyUnpack(key []byte) *ClaimToTXOKey {
	prefixLen := 1
	return &ClaimToTXOKey{
		Prefix:    key[:prefixLen],
		ClaimHash: key[prefixLen : prefixLen+20],
	}
}

func ClaimToTXOValueUnpack(value []byte) *ClaimToTXOValue {
	nameLen := binary.BigEndian.Uint16(value[21:])
	return &ClaimToTXOValue{
		TxNum:                   binary.BigEndian.Uint32(value),
		Position:                binary.BigEndian.Uint16(value[4:]),
		RootTxNum:               binary.BigEndian.Uint32(value[6:]),
		RootPosition:            binary.BigEndian.Uint16(value[10:]),
		Amount:                  binary.BigEndian.Uint64(value[12:]),
		ChannelSignatureIsValid: value[20] == 1,
		LengthEncodedName:       NewLengthEncodedName(string(value[23 : 23+nameLen])),
	}
}

type TXOToClaimKey struct {
	Prefix   []byte `struct:"[1]byte" json:"prefix"`
	TxNum    uint32 `json:"tx_num"`
	Position uint16 `json:"position"`
}

type TXOToClaimValue struct {
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
	LengthEncodedName
}

func NewTXOToClaimKey(txNum uint32, position uint16) *TXOToClaimKey {
	return &TXOToClaimKey{
		Prefix:   []byte{TXOToClaim},
		TxNum:    txNum,
		Position: position,
	}
}

func (k *TXOToClaimKey) PackKey() []byte {
	prefixLen := 1
	// b'>LH'
	n := prefixLen + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint32(key[prefixLen:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+4:], k.Position)

	return key
}

func (v *TXOToClaimValue) PackValue() []byte {
	nameLen := len(v.Name)
	n := 20 + 2 + nameLen
	value := make([]byte, n)
	copy(value, v.ClaimHash[:20])
	binary.BigEndian.PutUint16(value[20:], uint16(nameLen))
	copy(value[22:], []byte(v.Name))

	return value
}

func (kv *TXOToClaimKey) NumFields() int {
	return 2
}

func (k *TXOToClaimKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 2 {
		fields = 2
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		case 2:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint32(key[prefixLen:], k.TxNum)
		case 2:
			binary.BigEndian.PutUint16(key[prefixLen+4:], k.Position)
		}
	}

	return key
}

func TXOToClaimKeyUnpack(key []byte) *TXOToClaimKey {
	prefixLen := 1
	return &TXOToClaimKey{
		Prefix:   key[:prefixLen],
		TxNum:    binary.BigEndian.Uint32(key[prefixLen:]),
		Position: binary.BigEndian.Uint16(key[prefixLen+4:]),
	}
}

func TXOToClaimValueUnpack(value []byte) *TXOToClaimValue {
	nameLen := binary.BigEndian.Uint16(value[20:])
	return &TXOToClaimValue{
		ClaimHash:         value[:20],
		LengthEncodedName: NewLengthEncodedName(string(value[22 : 22+nameLen])),
	}
}

type ClaimShortIDKey struct {
	Prefix                      []byte `struct:"[1]byte" json:"prefix"`
	LengthEncodedNormalizedName        // fields NormalizedNameLen, NormalizedName
	LengthEncodedPartialClaimId        // fields PartialClaimIdLen, PartialClaimId
	RootTxNum                   uint32 `json:"root_tx_num"`
	RootPosition                uint16 `json:"root_position"`
}

type ClaimShortIDValue struct {
	TxNum    uint32 `json:"tx_num"`
	Position uint16 `json:"position"`
}

func NewClaimShortIDKey(normalizedName, partialClaimId string) *ClaimShortIDKey {
	return &ClaimShortIDKey{
		Prefix:                      []byte{ClaimShortIdPrefix},
		LengthEncodedNormalizedName: NewLengthEncodedNormalizedName(normalizedName),
		LengthEncodedPartialClaimId: NewLengthEncodedPartialClaimId(partialClaimId),
	}
}

func (k *ClaimShortIDKey) PackKey() []byte {
	prefixLen := 1
	nameLen := len(k.NormalizedName)
	partialClaimLen := len(k.PartialClaimId)
	log.Printf("nameLen: %d, partialClaimLen: %d\n", nameLen, partialClaimLen)
	n := prefixLen + 2 + nameLen + 1 + partialClaimLen + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint16(key[prefixLen:], uint16(nameLen))
	copy(key[prefixLen+2:], []byte(k.NormalizedName[:nameLen]))
	key[prefixLen+2+nameLen] = uint8(partialClaimLen)
	copy(key[prefixLen+2+nameLen+1:], []byte(k.PartialClaimId[:partialClaimLen]))
	binary.BigEndian.PutUint32(key[prefixLen+2+nameLen+1+partialClaimLen:], k.RootTxNum)
	binary.BigEndian.PutUint16(key[prefixLen+2+nameLen+1+partialClaimLen+4:], k.RootPosition)

	return key
}

func (v *ClaimShortIDValue) PackValue() []byte {
	value := make([]byte, 6)
	binary.BigEndian.PutUint32(value, v.TxNum)
	binary.BigEndian.PutUint16(value[4:], v.Position)

	return value
}

func (kv *ClaimShortIDKey) NumFields() int {
	return 4
}

func (k *ClaimShortIDKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 4 {
		fields = 4
	}
	if fields < 0 {
		fields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	nameLen := len(k.NormalizedName)
	partialClaimLen := len(k.PartialClaimId)

	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 2 + nameLen
		case 2:
			n += 1 + partialClaimLen
		case 3:
			n += 4
		case 4:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint16(key[prefixLen:], uint16(nameLen))
			copy(key[prefixLen+2:], []byte(k.NormalizedName))
		case 2:
			key[prefixLen+2+nameLen] = uint8(partialClaimLen)
			copy(key[prefixLen+2+nameLen+1:], []byte(k.PartialClaimId))
		case 3:
			binary.BigEndian.PutUint32(key[prefixLen+2+nameLen+1+partialClaimLen:], k.RootTxNum)
		case 4:
			binary.BigEndian.PutUint16(key[prefixLen+2+nameLen+1+partialClaimLen+4:], k.RootPosition)
		}
	}

	return key
}

func ClaimShortIDKeyUnpack(key []byte) *ClaimShortIDKey {
	prefixLen := 1
	nameLen := int(binary.BigEndian.Uint16(key[prefixLen:]))
	partialClaimLen := int(uint8(key[prefixLen+2+nameLen]))
	return &ClaimShortIDKey{
		Prefix:                      key[:prefixLen],
		LengthEncodedNormalizedName: NewLengthEncodedNormalizedName(string(key[prefixLen+2 : prefixLen+2+nameLen])),
		LengthEncodedPartialClaimId: NewLengthEncodedPartialClaimId(string(key[prefixLen+2+nameLen+1 : prefixLen+2+nameLen+1+partialClaimLen])),
		RootTxNum:                   binary.BigEndian.Uint32(key[prefixLen+2+nameLen+1+partialClaimLen:]),
		RootPosition:                binary.BigEndian.Uint16(key[prefixLen+2+nameLen+1+partialClaimLen+4:]),
	}
}

func ClaimShortIDValueUnpack(value []byte) *ClaimShortIDValue {
	return &ClaimShortIDValue{
		TxNum:    binary.BigEndian.Uint32(value),
		Position: binary.BigEndian.Uint16(value[4:]),
	}
}

type ClaimToChannelKey struct {
	Prefix    []byte `struct:"[1]byte" json:"prefix"`
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
	TxNum     uint32 `json:"tx_num"`
	Position  uint16 `json:"position"`
}

type ClaimToChannelValue struct {
	SigningHash []byte `struct:"[20]byte" json:"signing_hash"`
}

func NewClaimToChannelKey(claimHash []byte, txNum uint32, position uint16) *ClaimToChannelKey {
	return &ClaimToChannelKey{
		Prefix:    []byte{ClaimToChannel},
		ClaimHash: claimHash,
		TxNum:     txNum,
		Position:  position,
	}
}

func (k *ClaimToChannelKey) PackKey() []byte {
	prefixLen := 1
	// b'>20sLH'
	n := prefixLen + 20 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.ClaimHash[:20])
	binary.BigEndian.PutUint32(key[prefixLen+20:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+24:], k.Position)

	return key
}

func (v *ClaimToChannelValue) PackValue() []byte {
	value := make([]byte, 20)
	copy(value, v.SigningHash[:20])

	return value
}

func (kv *ClaimToChannelKey) NumFields() int {
	return 3
}

func (k *ClaimToChannelKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 3 {
		fields = 3
	}
	if fields < 0 {
		fields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 20
		case 2:
			n += 4
		case 3:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.ClaimHash[:20])
		case 2:
			binary.BigEndian.PutUint32(key[prefixLen+20:], k.TxNum)
		case 3:
			binary.BigEndian.PutUint16(key[prefixLen+24:], k.Position)
		}
	}

	return key
}

func ClaimToChannelKeyUnpack(key []byte) *ClaimToChannelKey {
	prefixLen := 1
	return &ClaimToChannelKey{
		Prefix:    key[:prefixLen],
		ClaimHash: key[prefixLen : prefixLen+20],
		TxNum:     binary.BigEndian.Uint32(key[prefixLen+20:]),
		Position:  binary.BigEndian.Uint16(key[prefixLen+24:]),
	}
}

func ClaimToChannelValueUnpack(value []byte) *ClaimToChannelValue {
	return &ClaimToChannelValue{
		SigningHash: value[:20],
	}
}

type ChannelToClaimKey struct {
	Prefix            []byte `struct:"[1]byte" json:"prefix"`
	SigningHash       []byte `struct:"[20]byte" json:"signing_hash"`
	LengthEncodedName        // fields NameLen, Name
	TxNum             uint32 `json:"tx_num"`
	Position          uint16 `json:"position"`
}

type ChannelToClaimValue struct {
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
}

func NewChannelToClaimKey(channelHash []byte, normalizedName string) *ChannelToClaimKey {
	return &ChannelToClaimKey{
		Prefix:            []byte{ChannelToClaim},
		SigningHash:       channelHash,
		LengthEncodedName: NewLengthEncodedName(normalizedName),
	}
}

func NewChannelToClaimKeyWHash(channelHash []byte) *ChannelToClaimKey {
	return &ChannelToClaimKey{
		Prefix:      []byte{ChannelToClaim},
		SigningHash: channelHash,
	}
}

func (k *ChannelToClaimKey) PackKey() []byte {
	prefixLen := 1
	nameLen := len(k.Name)
	n := prefixLen + 20 + 2 + nameLen + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.SigningHash[:20])
	binary.BigEndian.PutUint16(key[prefixLen+20:], uint16(nameLen))
	copy(key[prefixLen+22:], []byte(k.Name[:nameLen]))
	binary.BigEndian.PutUint32(key[prefixLen+22+nameLen:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+22+nameLen+4:], k.Position)

	return key
}

func (v *ChannelToClaimValue) PackValue() []byte {
	value := make([]byte, 20)
	copy(value, v.ClaimHash[:20])

	return value
}

func (kv *ChannelToClaimKey) NumFields() int {
	return 4
}

func (k *ChannelToClaimKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 4 {
		fields = 4
	}
	if fields < 0 {
		fields = 0
	}

	nameLen := len(k.Name)
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 20
		case 2:
			n += 2 + nameLen
		case 3:
			n += 4
		case 4:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.SigningHash[:20])
		case 2:
			binary.BigEndian.PutUint16(key[prefixLen+20:], uint16(nameLen))
			copy(key[prefixLen+22:], []byte(k.Name))
		case 3:
			binary.BigEndian.PutUint32(key[prefixLen+22+nameLen:], k.TxNum)
		case 4:
			binary.BigEndian.PutUint16(key[prefixLen+22+nameLen+4:], k.Position)
		}
	}

	return key
}

func ChannelToClaimKeyUnpack(key []byte) *ChannelToClaimKey {
	prefixLen := 1
	nameLen := int(binary.BigEndian.Uint16(key[prefixLen+20:]))
	return &ChannelToClaimKey{
		Prefix:            key[:prefixLen],
		SigningHash:       key[prefixLen : prefixLen+20],
		LengthEncodedName: NewLengthEncodedName(string(key[prefixLen+22 : prefixLen+22+nameLen])),
		TxNum:             binary.BigEndian.Uint32(key[prefixLen+22+nameLen:]),
		Position:          binary.BigEndian.Uint16(key[prefixLen+22+nameLen+4:]),
	}
}

func ChannelToClaimValueUnpack(value []byte) *ChannelToClaimValue {
	return &ChannelToClaimValue{
		ClaimHash: value[:20],
	}
}

type ChannelCountKey struct {
	Prefix      []byte `struct:"[1]byte" json:"prefix"`
	ChannelHash []byte `struct:"[20]byte" json:"channel_hash"`
}

type ChannelCountValue struct {
	Count uint32 `json:"count"`
}

func NewChannelCountKey(channelHash []byte) *ChannelCountKey {
	return &ChannelCountKey{
		Prefix:      []byte{ChannelCount},
		ChannelHash: channelHash,
	}
}

func (k *ChannelCountKey) PackKey() []byte {
	prefixLen := 1
	// b'>20sLH'
	n := prefixLen + 20
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.ChannelHash[:20])

	return key
}

func (v *ChannelCountValue) PackValue() []byte {
	value := make([]byte, 4)
	binary.BigEndian.PutUint32(value, v.Count)

	return value
}

func (kv *ChannelCountKey) NumFields() int {
	return 1
}

func (k *ChannelCountKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 20
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.ChannelHash)
		}
	}

	return key
}

func ChannelCountKeyUnpack(key []byte) *ChannelCountKey {
	prefixLen := 1
	return &ChannelCountKey{
		Prefix:      key[:prefixLen],
		ChannelHash: key[prefixLen : prefixLen+20],
	}
}

func ChannelCountValueUnpack(value []byte) *ChannelCountValue {
	return &ChannelCountValue{
		Count: binary.BigEndian.Uint32(value),
	}
}

type SupportAmountKey struct {
	Prefix    []byte `struct:"[1]byte" json:"prefix"`
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
}

type SupportAmountValue struct {
	Amount uint64 `json:"amount"`
}

func NewSupportAmountKey(claimHash []byte) *SupportAmountKey {
	return &SupportAmountKey{
		Prefix:    []byte{SupportAmount},
		ClaimHash: claimHash,
	}
}

func (k *SupportAmountKey) PackKey() []byte {
	prefixLen := 1
	// b'>20sLH'
	n := prefixLen + 20
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.ClaimHash[:20])

	return key
}

func (v *SupportAmountValue) PackValue() []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, v.Amount)

	return value
}

func (kv *SupportAmountKey) NumFields() int {
	return 1
}

func (k *SupportAmountKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 20
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.ClaimHash)
		}
	}

	return key
}

func SupportAmountKeyUnpack(key []byte) *SupportAmountKey {
	prefixLen := 1
	return &SupportAmountKey{
		Prefix:    key[:prefixLen],
		ClaimHash: key[prefixLen : prefixLen+20],
	}
}

func SupportAmountValueUnpack(value []byte) *SupportAmountValue {
	return &SupportAmountValue{
		Amount: binary.BigEndian.Uint64(value),
	}
}

type ClaimToSupportKey struct {
	Prefix    []byte `struct:"[1]byte" json:"prefix"`
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
	TxNum     uint32 `json:"tx_num"`
	Position  uint16 `json:"position"`
}

type ClaimToSupportValue struct {
	Amount uint64 `json:"amount"`
}

func (k *ClaimToSupportKey) PackKey() []byte {
	prefixLen := 1
	// b'>20sLH'
	n := prefixLen + 20 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.ClaimHash[:20])
	binary.BigEndian.PutUint32(key[prefixLen+20:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+24:], k.Position)

	return key
}

func (v *ClaimToSupportValue) PackValue() []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, v.Amount)

	return value
}

func (kv *ClaimToSupportKey) NumFields() int {
	return 3
}

func (k *ClaimToSupportKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 3 {
		fields = 3
	}
	if fields < 0 {
		fields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 20
		case 2:
			n += 4
		case 3:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.ClaimHash)
		case 2:
			binary.BigEndian.PutUint32(key[prefixLen+20:], k.TxNum)
		case 3:
			binary.BigEndian.PutUint16(key[prefixLen+24:], k.Position)
		}
	}

	return key
}

func ClaimToSupportKeyUnpack(key []byte) *ClaimToSupportKey {
	prefixLen := 1
	return &ClaimToSupportKey{
		Prefix:    key[:prefixLen],
		ClaimHash: key[prefixLen : prefixLen+20],
		TxNum:     binary.BigEndian.Uint32(key[prefixLen+20:]),
		Position:  binary.BigEndian.Uint16(key[prefixLen+24:]),
	}
}

func ClaimToSupportValueUnpack(value []byte) *ClaimToSupportValue {
	return &ClaimToSupportValue{
		Amount: binary.BigEndian.Uint64(value),
	}
}

type SupportToClaimKey struct {
	Prefix   []byte `struct:"[1]byte" json:"prefix"`
	TxNum    uint32 `json:"tx_num"`
	Position uint16 `json:"position"`
}

type SupportToClaimValue struct {
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
}

func (k *SupportToClaimKey) PackKey() []byte {
	prefixLen := 1
	// b'>LH'
	n := prefixLen + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint32(key[prefixLen:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+4:], k.Position)

	return key
}

func (v *SupportToClaimValue) PackValue() []byte {
	value := make([]byte, 20)
	copy(value, v.ClaimHash)

	return value
}

func (kv *SupportToClaimKey) NumFields() int {
	return 2
}

func (k *SupportToClaimKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 2 {
		fields = 2
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		case 2:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint32(key[prefixLen:], k.TxNum)
		case 2:
			binary.BigEndian.PutUint16(key[prefixLen+4:], k.Position)
		}
	}

	return key
}

func SupportToClaimKeyUnpack(key []byte) *SupportToClaimKey {
	prefixLen := 1
	return &SupportToClaimKey{
		Prefix:   key[:prefixLen],
		TxNum:    binary.BigEndian.Uint32(key[prefixLen:]),
		Position: binary.BigEndian.Uint16(key[prefixLen+4:]),
	}
}

func SupportToClaimValueUnpack(value []byte) *SupportToClaimValue {
	return &SupportToClaimValue{
		ClaimHash: value[:20],
	}
}

type ClaimExpirationKey struct {
	Prefix     []byte `struct:"[1]byte" json:"prefix"`
	Expiration uint32 `json:"expiration"`
	TxNum      uint32 `json:"tx_num"`
	Position   uint16 `json:"position"`
}

type ClaimExpirationValue struct {
	ClaimHash                   []byte `struct:"[20]byte" json:"claim_hash"`
	LengthEncodedNormalizedName        // fields NormalizedNameLen, NormalizedName
}

func (k *ClaimExpirationKey) PackKey() []byte {
	prefixLen := 1
	// b'>LLH'
	n := prefixLen + 4 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint32(key[prefixLen:], k.Expiration)
	binary.BigEndian.PutUint32(key[prefixLen+4:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+8:], k.Position)

	return key
}

func (v *ClaimExpirationValue) PackValue() []byte {
	nameLen := len(v.NormalizedName)
	n := 20 + 2 + nameLen
	value := make([]byte, n)
	copy(value, v.ClaimHash)
	binary.BigEndian.PutUint16(value[20:], uint16(nameLen))
	copy(value[22:], []byte(v.NormalizedName))

	return value
}

func (kv *ClaimExpirationKey) NumFields() int {
	return 3
}

func (k *ClaimExpirationKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 3 {
		fields = 3
	}
	if fields < 0 {
		fields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		case 2:
			n += 4
		case 3:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint32(key[prefixLen:], k.Expiration)
		case 2:
			binary.BigEndian.PutUint32(key[prefixLen+4:], k.TxNum)
		case 3:
			binary.BigEndian.PutUint16(key[prefixLen+8:], k.Position)
		}
	}

	return key
}

func ClaimExpirationKeyUnpack(key []byte) *ClaimExpirationKey {
	prefixLen := 1
	return &ClaimExpirationKey{
		Prefix:     key[:prefixLen],
		Expiration: binary.BigEndian.Uint32(key[prefixLen:]),
		TxNum:      binary.BigEndian.Uint32(key[prefixLen+4:]),
		Position:   binary.BigEndian.Uint16(key[prefixLen+8:]),
	}
}

func ClaimExpirationValueUnpack(value []byte) *ClaimExpirationValue {
	nameLen := binary.BigEndian.Uint16(value[20:])
	return &ClaimExpirationValue{
		ClaimHash:                   value[:20],
		LengthEncodedNormalizedName: NewLengthEncodedNormalizedName(string(value[22 : 22+nameLen])),
	}
}

type ClaimTakeoverKey struct {
	Prefix                      []byte `struct:"[1]byte" json:"prefix"`
	LengthEncodedNormalizedName        // fields NormalizedNameLen, NormalizedName
}

type ClaimTakeoverValue struct {
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
	Height    uint32 `json:"height"`
}

func NewClaimTakeoverKey(normalizedName string) *ClaimTakeoverKey {
	return &ClaimTakeoverKey{
		Prefix:                      []byte{ClaimTakeover},
		LengthEncodedNormalizedName: NewLengthEncodedNormalizedName(normalizedName),
	}
}

func (v *ClaimTakeoverValue) String() string {
	return fmt.Sprintf(
		"%s(claim_hash=%s, height=%d)",
		reflect.TypeOf(v),
		hex.EncodeToString(v.ClaimHash),
		v.Height,
	)
}

func (k *ClaimTakeoverKey) PackKey() []byte {
	prefixLen := 1
	nameLen := len(k.NormalizedName)
	n := prefixLen + 2 + nameLen
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint16(key[prefixLen:], uint16(nameLen))
	copy(key[prefixLen+2:], []byte(k.NormalizedName))

	return key
}

func (v *ClaimTakeoverValue) PackValue() []byte {
	// b'>20sL'
	value := make([]byte, 24)
	copy(value, v.ClaimHash[:20])
	binary.BigEndian.PutUint32(value[20:], uint32(v.Height))

	return value
}

func (kv *ClaimTakeoverKey) NumFields() int {
	return 1
}

func (k *ClaimTakeoverKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	nameLen := len(k.NormalizedName)
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 2 + nameLen
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint16(key[prefixLen:], uint16(nameLen))
			copy(key[prefixLen+2:], []byte(k.NormalizedName))
		}
	}

	return key
}

func ClaimTakeoverKeyUnpack(key []byte) *ClaimTakeoverKey {
	prefixLen := 1
	nameLen := binary.BigEndian.Uint16(key[prefixLen:])
	return &ClaimTakeoverKey{
		Prefix:                      key[:prefixLen],
		LengthEncodedNormalizedName: NewLengthEncodedNormalizedName(string(key[prefixLen+2 : prefixLen+2+int(nameLen)])),
	}
}

func ClaimTakeoverValueUnpack(value []byte) *ClaimTakeoverValue {
	return &ClaimTakeoverValue{
		ClaimHash: value[:20],
		Height:    binary.BigEndian.Uint32(value[20:]),
	}
}

type PendingActivationKey struct {
	Prefix   []byte `struct:"[1]byte" json:"prefix"`
	Height   uint32 `json:"height"`
	TxoType  uint8  `json:"txo_type"`
	TxNum    uint32 `json:"tx_num"`
	Position uint16 `json:"position"`
}

func (k *PendingActivationKey) IsSupport() bool {
	return k.TxoType == ActivatedSupportTXOType
}

func (k *PendingActivationKey) IsClaim() bool {
	return k.TxoType == ActivateClaimTXOType
}

type PendingActivationValue struct {
	ClaimHash                   []byte `struct:"[20]byte" json:"claim_hash"`
	LengthEncodedNormalizedName        // fields NormalizedNameLen, NormalizedName
}

func (k *PendingActivationKey) PackKey() []byte {
	prefixLen := 1
	// b'>LBLH'
	n := prefixLen + 4 + 1 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint32(key[prefixLen:], k.Height)
	key[prefixLen+4] = k.TxoType
	binary.BigEndian.PutUint32(key[prefixLen+5:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+9:], k.Position)

	return key
}

func (v *PendingActivationValue) PackValue() []byte {
	nameLen := len(v.NormalizedName)
	n := 20 + 2 + nameLen
	value := make([]byte, n)
	copy(value, v.ClaimHash[:20])
	binary.BigEndian.PutUint16(value[20:], uint16(nameLen))
	copy(value[22:], []byte(v.NormalizedName))

	return value
}

func (kv *PendingActivationKey) NumFields() int {
	return 4
}

func (k *PendingActivationKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 4 {
		fields = 4
	}
	if fields < 0 {
		fields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		case 2:
			n += 1
		case 3:
			n += 4
		case 4:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint32(key[prefixLen:], k.Height)
		case 2:
			key[prefixLen+4] = k.TxoType
		case 3:
			binary.BigEndian.PutUint32(key[prefixLen+5:], k.TxNum)
		case 4:
			binary.BigEndian.PutUint16(key[prefixLen+9:], k.Position)
		}
	}

	return key
}

func PendingActivationKeyUnpack(key []byte) *PendingActivationKey {
	prefixLen := 1
	return &PendingActivationKey{
		Prefix:   key[:prefixLen],
		Height:   binary.BigEndian.Uint32(key[prefixLen:]),
		TxoType:  key[prefixLen+4],
		TxNum:    binary.BigEndian.Uint32(key[prefixLen+5:]),
		Position: binary.BigEndian.Uint16(key[prefixLen+9:]),
	}
}

func PendingActivationValueUnpack(value []byte) *PendingActivationValue {
	nameLen := binary.BigEndian.Uint16(value[20:])
	return &PendingActivationValue{
		ClaimHash:                   value[:20],
		LengthEncodedNormalizedName: NewLengthEncodedNormalizedName(string(value[22 : 22+nameLen])),
	}
}

type ActivationKey struct {
	Prefix   []byte `struct:"[1]byte" json:"prefix"`
	TxoType  uint8  `json:"txo_type"`
	TxNum    uint32 `json:"tx_num"`
	Position uint16 `json:"position"`
}

type ActivationValue struct {
	Height                      uint32 `json:"height"`
	ClaimHash                   []byte `struct:"[20]byte" json:"claim_hash"`
	LengthEncodedNormalizedName        // fields NormalizedNameLen, NormalizedName
}

func NewActivationKey(txoType uint8, txNum uint32, position uint16) *ActivationKey {
	return &ActivationKey{
		Prefix:   []byte{ActivatedClaimAndSupport},
		TxoType:  txoType,
		TxNum:    txNum,
		Position: position,
	}
}

func (k *ActivationKey) PackKey() []byte {
	prefixLen := 1
	// b'>BLH'
	n := prefixLen + 1 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], []byte{k.TxoType})
	binary.BigEndian.PutUint32(key[prefixLen+1:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+5:], k.Position)

	return key
}

func (v *ActivationValue) PackValue() []byte {
	nameLen := len(v.NormalizedName)
	n := 4 + 20 + 2 + nameLen
	value := make([]byte, n)
	binary.BigEndian.PutUint32(value, v.Height)
	copy(value[4:], v.ClaimHash[:20])
	binary.BigEndian.PutUint16(value[24:], uint16(nameLen))
	copy(value[26:], []byte(v.NormalizedName))

	return value
}

func (kv *ActivationKey) NumFields() int {
	return 3
}

func (k *ActivationKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 3 {
		fields = 3
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 1
		case 2:
			n += 4
		case 3:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			key[prefixLen] = k.TxoType
		case 2:
			binary.BigEndian.PutUint32(key[prefixLen+1:], k.TxNum)
		case 3:
			binary.BigEndian.PutUint16(key[prefixLen+5:], k.Position)
		}
	}

	return key
}

func ActivationKeyUnpack(key []byte) *ActivationKey {
	prefixLen := 1
	return &ActivationKey{
		Prefix:   key[:prefixLen],
		TxoType:  key[prefixLen],
		TxNum:    binary.BigEndian.Uint32(key[prefixLen+1:]),
		Position: binary.BigEndian.Uint16(key[prefixLen+5:]),
	}
}

func ActivationValueUnpack(value []byte) *ActivationValue {
	nameLen := binary.BigEndian.Uint16(value[24:])
	return &ActivationValue{
		Height:                      binary.BigEndian.Uint32(value),
		ClaimHash:                   value[4 : 20+4],
		LengthEncodedNormalizedName: NewLengthEncodedNormalizedName(string(value[26 : 26+nameLen])),
	}
}

type ActiveAmountKey struct {
	Prefix           []byte `struct:"[1]byte" json:"prefix"`
	ClaimHash        []byte `struct:"[20]byte" json:"claim_hash"`
	TxoType          uint8  `json:"txo_type"`
	ActivationHeight uint32 `json:"activation_height"`
	TxNum            uint32 `json:"tx_num"`
	Position         uint16 `json:"position"`
}

type ActiveAmountValue struct {
	Amount uint64 `json:"amount"`
}

func NewActiveAmountKey(claimHash []byte, txoType uint8, activationHeight uint32) *ActiveAmountKey {
	return &ActiveAmountKey{
		Prefix:           []byte{ActiveAmount},
		ClaimHash:        claimHash,
		TxoType:          txoType,
		ActivationHeight: activationHeight,
	}
}

func (k *ActiveAmountKey) PackKey() []byte {
	prefixLen := 1
	// b'>20sBLLH'
	n := prefixLen + 20 + 1 + 4 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.ClaimHash[:20])
	copy(key[prefixLen+20:], []byte{k.TxoType})
	binary.BigEndian.PutUint32(key[prefixLen+20+1:], k.ActivationHeight)
	binary.BigEndian.PutUint32(key[prefixLen+20+1+4:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+20+1+4+4:], k.Position)

	return key
}

func (v *ActiveAmountValue) PackValue() []byte {
	// b'>Q'
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, v.Amount)

	return value
}

func (kv *ActiveAmountKey) NumFields() int {
	return 5
}

func (k *ActiveAmountKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 5 {
		fields = 5
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 20
		case 2:
			n += 1
		case 3:
			n += 4
		case 4:
			n += 4
		case 5:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.ClaimHash)
		case 2:
			copy(key[prefixLen+20:], []byte{k.TxoType})
		case 3:
			binary.BigEndian.PutUint32(key[prefixLen+20+1:], k.ActivationHeight)
		case 4:
			binary.BigEndian.PutUint32(key[prefixLen+20+1+4:], k.TxNum)
		case 5:
			binary.BigEndian.PutUint16(key[prefixLen+20+1+4+4:], k.Position)
		}
	}

	return key
}

func ActiveAmountKeyUnpack(key []byte) *ActiveAmountKey {
	prefixLen := 1
	return &ActiveAmountKey{
		Prefix:           key[:prefixLen],
		ClaimHash:        key[prefixLen : prefixLen+20],
		TxoType:          uint8(key[prefixLen+20 : prefixLen+20+1][0]),
		ActivationHeight: binary.BigEndian.Uint32(key[prefixLen+20+1:]),
		TxNum:            binary.BigEndian.Uint32(key[prefixLen+20+1+4:]),
		Position:         binary.BigEndian.Uint16(key[prefixLen+20+1+4+4:]),
	}
}

func ActiveAmountValueUnpack(value []byte) *ActiveAmountValue {
	return &ActiveAmountValue{
		Amount: binary.BigEndian.Uint64(value),
	}
}

type OnesComplementEffectiveAmount uint64

type EffectiveAmountKey struct {
	Prefix                      []byte                        `struct:"[1]byte" json:"prefix"`
	LengthEncodedNormalizedName                               // fields NormalizedNameLen, NormalizedName
	EffectiveAmount             OnesComplementEffectiveAmount `json:"effective_amount"`
	TxNum                       uint32                        `json:"tx_num"`
	Position                    uint16                        `json:"position"`
}

type EffectiveAmountValue struct {
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
}

func NewEffectiveAmountKey(normalizedName string) *EffectiveAmountKey {
	return &EffectiveAmountKey{
		Prefix:                      []byte{EffectiveAmount},
		LengthEncodedNormalizedName: NewLengthEncodedNormalizedName(normalizedName),
	}
}

func (k *EffectiveAmountKey) PackKey() []byte {
	prefixLen := 1
	// 2 byte length field, plus number of bytes in name
	nameLen := len(k.NormalizedName)
	nameLenLen := 2 + nameLen
	// b'>QLH'
	n := prefixLen + nameLenLen + 8 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)

	binary.BigEndian.PutUint16(key[prefixLen:], uint16(nameLen))
	copy(key[prefixLen+2:], []byte(k.NormalizedName))
	binary.BigEndian.PutUint64(key[prefixLen+nameLenLen:], OnesCompTwiddle64-uint64(k.EffectiveAmount))
	binary.BigEndian.PutUint32(key[prefixLen+nameLenLen+8:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+nameLenLen+8+4:], k.Position)

	return key
}

func (v *EffectiveAmountValue) PackValue() []byte {
	// b'>20s'
	value := make([]byte, 20)
	copy(value, v.ClaimHash[:20])

	return value
}

func (kv *EffectiveAmountKey) NumFields() int {
	return 4
}

func (k *EffectiveAmountKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	nameLen := len(k.NormalizedName)
	nameLenLen := 2 + nameLen
	if fields > 4 {
		fields = 4
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 2 + nameLen
		case 2:
			n += 8
		case 3:
			n += 4
		case 4:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint16(key[prefixLen:], uint16(nameLen))
			copy(key[prefixLen+2:], []byte(k.NormalizedName))
		case 2:
			binary.BigEndian.PutUint64(key[prefixLen+nameLenLen:], OnesCompTwiddle64-uint64(k.EffectiveAmount))
		case 3:
			binary.BigEndian.PutUint32(key[prefixLen+nameLenLen+8:], k.TxNum)
		case 4:
			binary.BigEndian.PutUint16(key[prefixLen+nameLenLen+8+4:], k.Position)
		}
	}

	return key
}

func EffectiveAmountKeyUnpack(key []byte) *EffectiveAmountKey {
	prefixLen := 1
	nameLen := binary.BigEndian.Uint16(key[prefixLen:])
	return &EffectiveAmountKey{
		Prefix:                      key[:prefixLen],
		LengthEncodedNormalizedName: NewLengthEncodedNormalizedName(string(key[prefixLen+2 : prefixLen+2+int(nameLen)])),
		EffectiveAmount:             OnesComplementEffectiveAmount(OnesCompTwiddle64 - binary.BigEndian.Uint64(key[prefixLen+2+int(nameLen):])),
		TxNum:                       binary.BigEndian.Uint32(key[prefixLen+2+int(nameLen)+8:]),
		Position:                    binary.BigEndian.Uint16(key[prefixLen+2+int(nameLen)+8+4:]),
	}
}

func EffectiveAmountValueUnpack(value []byte) *EffectiveAmountValue {
	return &EffectiveAmountValue{
		ClaimHash: value[:20],
	}
}

type RepostKey struct {
	Prefix    []byte `struct:"[1]byte" json:"prefix"`
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
}

type RepostValue struct {
	RepostedClaimHash []byte `struct:"[20]byte" json:"reposted_claim_hash"`
}

func NewRepostKey(claimHash []byte) *RepostKey {
	return &RepostKey{
		Prefix:    []byte{Repost},
		ClaimHash: claimHash,
	}
}

func (k *RepostKey) PackKey() []byte {
	prefixLen := 1
	// b'>20s'
	n := prefixLen + 20
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.ClaimHash)

	return key
}

func (v *RepostValue) PackValue() []byte {
	// FIXME: Is there a limit to this length?
	n := len(v.RepostedClaimHash)
	value := make([]byte, n)
	copy(value, v.RepostedClaimHash)

	return value
}

func (kv *RepostKey) NumFields() int {
	return 1
}

func (k *RepostKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 20
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.ClaimHash)
		}
	}

	return key
}

func RepostKeyUnpack(key []byte) *RepostKey {
	prefixLen := 1
	return &RepostKey{
		Prefix:    key[:prefixLen],
		ClaimHash: key[prefixLen : prefixLen+20],
	}
}

func RepostValueUnpack(value []byte) *RepostValue {
	return &RepostValue{
		RepostedClaimHash: value[:],
	}
}

type RepostedKey struct {
	Prefix            []byte `struct:"[1]byte" json:"prefix"`
	RepostedClaimHash []byte `struct:"[20]byte" json:"reposted_claim_hash"`
	TxNum             uint32 `json:"tx_num"`
	Position          uint16 `json:"position"`
}

type RepostedValue struct {
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
}

func NewRepostedKey(claimHash []byte) *RepostedKey {
	return &RepostedKey{
		Prefix:            []byte{RepostedClaim},
		RepostedClaimHash: claimHash,
	}
}

func (k *RepostedKey) PackKey() []byte {
	prefixLen := 1
	// b'>20sLH'
	n := prefixLen + 20 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.RepostedClaimHash)
	binary.BigEndian.PutUint32(key[prefixLen+20:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+24:], k.Position)

	return key
}

func (v *RepostedValue) PackValue() []byte {
	// b'>20s'
	value := make([]byte, 20)
	copy(value, v.ClaimHash)

	return value
}

func (kv *RepostedKey) NumFields() int {
	return 3
}

func (k *RepostedKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 3 {
		fields = 3
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 20
		case 2:
			n += 4
		case 3:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.RepostedClaimHash)
		case 2:
			binary.BigEndian.PutUint32(key[prefixLen+20:], k.TxNum)
		case 3:
			binary.BigEndian.PutUint16(key[prefixLen+24:], k.Position)
		}
	}

	return key
}

func RepostedKeyUnpack(key []byte) *RepostedKey {
	prefixLen := 1
	return &RepostedKey{
		Prefix:            key[:prefixLen],
		RepostedClaimHash: key[prefixLen : prefixLen+20],
		TxNum:             binary.BigEndian.Uint32(key[prefixLen+20:]),
		Position:          binary.BigEndian.Uint16(key[prefixLen+24:]),
	}
}

func RepostedValueUnpack(value []byte) *RepostedValue {
	return &RepostedValue{
		ClaimHash: value[:20],
	}
}

type TouchedOrDeletedClaimKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	Height int32  `json:"height"`
}

type TouchedOrDeletedClaimValue struct {
	TouchedClaimsLen uint32   `struct:"sizeof=TouchedClaims"`
	DeletedClaimsLen uint32   `struct:"sizeof=DeletedClaims"`
	TouchedClaims    [][]byte `struct:"[][20]byte" json:"touched_claims"`
	DeletedClaims    [][]byte `struct:"[][20]byte" json:"deleted_claims"`
}

func (v *TouchedOrDeletedClaimValue) String() string {
	touchedSB := strings.Builder{}
	touchedLen := len(v.TouchedClaims)
	for i, claim := range v.TouchedClaims {
		touchedSB.WriteString(hex.EncodeToString(claim))
		if i < touchedLen-1 {
			touchedSB.WriteString(",")
		}
	}

	deletedSB := strings.Builder{}
	deletedLen := len(v.DeletedClaims)
	for i, claim := range v.DeletedClaims {
		deletedSB.WriteString(hex.EncodeToString(claim))
		if i < deletedLen-1 {
			deletedSB.WriteString(",")
		}
	}

	return fmt.Sprintf(
		"%s(touched_claims=%s, deleted_claims=%s)",
		reflect.TypeOf(v),
		touchedSB.String(),
		deletedSB.String(),
	)
}

func (k *TouchedOrDeletedClaimKey) PackKey() []byte {
	prefixLen := 1
	// b'>L'
	n := prefixLen + 4
	key := make([]byte, n)
	copy(key, k.Prefix)
	binary.BigEndian.PutUint32(key[prefixLen:], uint32(k.Height))

	return key
}

func (v *TouchedOrDeletedClaimValue) PackValue() []byte {
	var touchedLen, deletedLen uint32 = 0, 0
	if v.TouchedClaims != nil {
		for _, claim := range v.TouchedClaims {
			if len(claim) != 20 {
				log.Println("TouchedOrDeletedClaimValue: claim not length 20?!?")
				return nil
			}
		}
		touchedLen = uint32(len(v.TouchedClaims))
	}
	if v.DeletedClaims != nil {
		for _, claim := range v.DeletedClaims {
			if len(claim) != 20 {
				log.Println("TouchedOrDeletedClaimValue: claim not length 20?!?")
				return nil
			}
		}
		deletedLen = uint32(len(v.DeletedClaims))
	}
	n := 4 + 4 + 20*touchedLen + 20*deletedLen
	value := make([]byte, n)
	binary.BigEndian.PutUint32(value, touchedLen)
	binary.BigEndian.PutUint32(value[4:], deletedLen)
	// These are sorted for consistency with the Python implementation
	sort.Slice(v.TouchedClaims, func(i, j int) bool { return bytes.Compare(v.TouchedClaims[i], v.TouchedClaims[j]) < 0 })
	sort.Slice(v.DeletedClaims, func(i, j int) bool { return bytes.Compare(v.DeletedClaims[i], v.DeletedClaims[j]) < 0 })

	var i = 8
	for j := 0; j < int(touchedLen); j++ {
		copy(value[i:], v.TouchedClaims[j])
		i += 20
	}
	for j := 0; j < int(deletedLen); j++ {
		copy(value[i:], v.DeletedClaims[j])
		i += 20
	}

	return value
}

func (kv *TouchedOrDeletedClaimKey) NumFields() int {
	return 1
}

func (k *TouchedOrDeletedClaimKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 1 {
		fields = 1
	}
	if fields < 0 {
		fields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 4
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint32(key[prefixLen:], uint32(k.Height))
		}
	}

	return key
}

func TouchedOrDeletedClaimKeyUnpack(key []byte) *TouchedOrDeletedClaimKey {
	prefixLen := 1
	return &TouchedOrDeletedClaimKey{
		Prefix: key[:prefixLen],
		Height: int32(binary.BigEndian.Uint32(key[prefixLen:])),
	}
}

func TouchedOrDeletedClaimValueUnpack(value []byte) *TouchedOrDeletedClaimValue {
	touchedLen := binary.BigEndian.Uint32(value)
	deletedLen := binary.BigEndian.Uint32(value[4:])
	touchedClaims := make([][]byte, touchedLen)
	deletedClaims := make([][]byte, deletedLen)
	var j = 8
	for i := 0; i < int(touchedLen); i++ {
		touchedClaims[i] = value[j : j+20]
		j += 20
	}
	for i := 0; i < int(deletedLen); i++ {
		deletedClaims[i] = value[j : j+20]
		j += 20
	}
	return &TouchedOrDeletedClaimValue{
		TouchedClaimsLen: touchedLen,
		DeletedClaimsLen: deletedLen,
		TouchedClaims:    touchedClaims,
		DeletedClaims:    deletedClaims,
	}
}

func (k *UTXOKey) String() string {
	return fmt.Sprintf(
		"%s(hashX=%s, tx_num=%d, nout=%d)",
		reflect.TypeOf(k),
		hex.EncodeToString(k.HashX),
		k.TxNum,
		k.Nout,
	)
}

func (k *UTXOKey) PackKey() []byte {
	prefixLen := 1
	// b'>11sLH'
	n := prefixLen + 11 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.HashX)
	binary.BigEndian.PutUint32(key[prefixLen+11:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+15:], k.Nout)

	return key
}

func (k *UTXOValue) PackValue() []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, k.Amount)

	return value
}

func (kv *UTXOKey) NumFields() int {
	return 3
}

// UTXOKeyPackPartial packs a variable number of fields for a UTXOKey into
// a byte array.
func (k *UTXOKey) PartialPack(fields int) []byte {
	// Limit fields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if fields > 3 {
		fields = 3
	}
	if fields < 0 {
		fields = 0
	}

	// b'>11sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= fields; i++ {
		switch i {
		case 1:
			n += 11
		case 2:
			n += 4
		case 3:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= fields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			copy(key[prefixLen:], k.HashX)
		case 2:
			binary.BigEndian.PutUint32(key[prefixLen+11:], k.TxNum)
		case 3:
			binary.BigEndian.PutUint16(key[prefixLen+15:], k.Nout)
		}
	}

	return key
}

func UTXOKeyUnpack(key []byte) *UTXOKey {
	prefixLen := 1
	return &UTXOKey{
		Prefix: key[:prefixLen],
		HashX:  key[prefixLen : prefixLen+11],
		TxNum:  binary.BigEndian.Uint32(key[prefixLen+11:]),
		Nout:   binary.BigEndian.Uint16(key[prefixLen+15:]),
	}
}

func UTXOValueUnpack(value []byte) *UTXOValue {
	return &UTXOValue{
		Amount: binary.BigEndian.Uint64(value),
	}
}

type TrendingNotificationKey struct {
	Prefix    []byte `struct:"[1]byte"  json:"prefix"`
	Height    uint32 `struct:"uint32"   json:"height"`
	ClaimHash []byte `struct:"[20]byte" json:"claim_hash"`
}

type TrendingNotificationValue struct {
	PreviousAmount uint64 `struct:"uint64" json:"previous_amount"`
	NewAmount      uint64 `struct:"uint64" json:"new_amount"`
}

func (kv *TrendingNotificationKey) NumFields() int {
	return 2
}

func (kv *TrendingNotificationKey) PartialPack(fields int) []byte {
	// b'>L20s'
	n := len(kv.Prefix) + 4 + 20
	buf := make([]byte, n)
	offset := 0
	offset += copy(buf, kv.Prefix[offset:])
	if fields <= 0 {
		return buf[:offset]
	}
	binary.BigEndian.PutUint32(buf[offset:], kv.Height)
	offset += 4
	if fields -= 1; fields <= 0 {
		return buf[:offset]
	}
	offset += copy(buf[offset:], kv.ClaimHash[:20])
	return buf[:offset]
}

func (kv *TrendingNotificationKey) PackKey() []byte {
	return kv.PartialPack(kv.NumFields())
}

func (kv *TrendingNotificationKey) UnpackKey(buf []byte) {
	// b'>L20s'
	offset := 0
	kv.Prefix = buf[offset:1]
	offset += 1
	kv.Height = binary.BigEndian.Uint32(buf[offset:])
	offset += 4
	kv.ClaimHash = buf[offset:20]
	offset += 20
}

func (kv *TrendingNotificationValue) PackValue() []byte {
	// b'>QQ'
	n := 8 + 8
	buf := make([]byte, n)
	offset := 0
	binary.BigEndian.PutUint64(buf[offset:], kv.PreviousAmount)
	offset += 8
	binary.BigEndian.PutUint64(buf[offset:], kv.NewAmount)
	offset += 8
	return buf
}

func (kv *TrendingNotificationValue) UnpackValue(buf []byte) {
	// b'>QQ'
	offset := 0
	kv.PreviousAmount = binary.BigEndian.Uint64(buf[offset:])
	offset += 8
	kv.NewAmount = binary.BigEndian.Uint64(buf[offset:])
	offset += 8
}

type MempoolTxKey struct {
	Prefix []byte `struct:"[1]byte"  json:"prefix"`
	TxHash []byte `struct:"[32]byte" json:"tx_hash"`
}

type MempoolTxValue struct {
	RawTx []byte `struct-while:"!_eof" json:"raw_tx"`
}

func (kv *MempoolTxKey) NumFields() int {
	return 1
}

func (kv *MempoolTxKey) Pack(fields int) []byte {
	// b'>32s'
	n := len(kv.Prefix) + 32
	buf := make([]byte, n)
	offset := 0
	offset += copy(buf[offset:], kv.Prefix[:1])
	if fields <= 0 {
		return buf[:offset]
	}
	offset += copy(buf[offset:], kv.TxHash[:32])
	return buf[:offset]
}

func (kv *MempoolTxKey) UnpackKey(buf []byte) {
	// b'>32s'
	offset := 0
	kv.Prefix = buf[offset:1]
	offset += 1
	kv.TxHash = buf[offset:32]
	offset += 32
}

func (kv *MempoolTxValue) PackValue() []byte {
	// variable length bytes
	n := len(kv.RawTx)
	buf := make([]byte, n)
	offset := 0
	offset += copy(buf, kv.RawTx)
	return buf
}

func (kv *MempoolTxValue) UnpackValue(buf []byte) {
	// variable length bytes
	offset := 0
	kv.RawTx = buf[:]
	offset += len(buf)
}

type TouchedHashXKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	Height uint32 `struct:"uint32" json:"height"`
}

type TouchedHashXValue struct {
	TouchedHashXs [][]byte `struct:"[][11]byte" struct-while:"!_eof" json:"touched_hashXs"`
}

func (kv *TouchedHashXKey) NumFields() int {
	return 1
}

func (kv *TouchedHashXKey) PartialPack(fields int) []byte {
	// b'>L'
	n := len(kv.Prefix) + 4
	buf := make([]byte, n)
	offset := 0
	offset += copy(buf[offset:], kv.Prefix[:1])
	if fields <= 0 {
		return buf[:offset]
	}
	binary.BigEndian.PutUint32(buf[offset:], kv.Height)
	offset += 4
	return buf[:offset]
}

func (kv *TouchedHashXKey) PackKey() []byte {
	return kv.PartialPack(kv.NumFields())
}

func (kv *TouchedHashXKey) UnpackKey(buf []byte) {
	// b'>L'
	offset := 0
	kv.Prefix = buf[offset:1]
	offset += 1
	kv.Height = binary.BigEndian.Uint32(buf[offset:])
	offset += 4
}

func (kv *TouchedHashXValue) PackValue() []byte {
	// variable length bytes
	n := len(kv.TouchedHashXs) * 11
	buf := make([]byte, n)
	offset := 0
	for i := range kv.TouchedHashXs {
		offset += copy(buf[offset:], kv.TouchedHashXs[i][:11])
	}
	return buf
}

func (kv *TouchedHashXValue) UnpackValue(buf []byte) {
	// variable length bytes
	n := len(buf)
	for i, offset := 0, 0; offset+11 <= n; i, offset = i+1, offset+11 {
		kv.TouchedHashXs[i] = buf[offset:11]
	}
}

type HashXStatusKey struct {
	Prefix []byte `struct:"[1]byte" json:"prefix"`
	HashX  []byte `struct:"[20]byte" json:"hashX"`
}

type HashXStatusValue struct {
	Status []byte `struct:"[32]byte" json:"status"`
}

func (kv *HashXStatusKey) NumFields() int {
	return 1
}

func (kv *HashXStatusKey) PartialPack(fields int) []byte {
	// b'>20s'
	n := len(kv.Prefix) + 20
	buf := make([]byte, n)
	offset := 0
	offset += copy(buf[offset:], kv.Prefix[:1])
	if fields <= 0 {
		return buf[:offset]
	}
	offset += copy(buf[offset:], kv.HashX[:20])
	return buf[:offset]
}

func (kv *HashXStatusKey) PackKey() []byte {
	return kv.PartialPack(kv.NumFields())
}

func (kv *HashXStatusKey) UnpackKey(buf []byte) {
	// b'>20s'
	offset := 0
	kv.Prefix = buf[offset:1]
	offset += 1
	kv.HashX = buf[offset:20]
	offset += 20
}

func (kv *HashXStatusValue) PackValue() []byte {
	// b'32s'
	n := 32
	buf := make([]byte, n)
	offset := 0
	offset += copy(buf[offset:], kv.Status[:32])
	return buf
}

func (kv *HashXStatusValue) UnpackValue(buf []byte) {
	// b'32s'
	offset := 0
	kv.Status = buf[offset:32]
	offset += 32
}

type HashXMempoolStatusKey = HashXStatusKey
type HashXMempoolStatusValue = HashXStatusValue

func UnpackGenericKey(key []byte) (BaseKey, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key length zero")
	}
	// Look up the prefix metadata, and use the registered function(s)
	// to create and unpack key of appropriate type.
	t, ok := tableRegistry[key[0]]
	if !ok {
		return nil, fmt.Errorf("unpack key function for %v not implemented", key[0])
	}
	if t.newKeyUnpack != nil {
		return t.newKeyUnpack(key).(BaseKey), nil
	}
	if t.newKey != nil {
		k := t.newKey()
		unpacker, ok := k.(KeyUnpacker)
		if ok {
			unpacker.UnpackKey(key)
			return unpacker.(BaseKey), nil
		}
	}
	return nil, fmt.Errorf("unpack key function for %v not implemented", key[0])
}

func UnpackGenericValue(key []byte, value []byte) (BaseValue, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key length zero")
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("value length zero")
	}
	// Look up the prefix metadata, and use the registered function(s)
	// to create and unpack value of appropriate type.
	t, ok := tableRegistry[key[0]]
	if !ok {
		return nil, fmt.Errorf("unpack value function for %v not implemented", key[0])
	}
	if t.newValueUnpack != nil {
		return t.newValueUnpack(value).(BaseValue), nil
	}
	if t.newValue != nil {
		k := t.newValue()
		unpacker, ok := k.(ValueUnpacker)
		if ok {
			unpacker.UnpackValue(value)
			return unpacker.(BaseValue), nil
		}
	}
	return nil, fmt.Errorf("unpack key function for %v not implemented", key[0])
}

func PackPartialGenericKey(key BaseKey, fields int) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("key length zero")
	}
	return key.PartialPack(fields), nil
}

func PackGenericKey(key BaseKey) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("key length zero")
	}
	return key.PackKey(), nil
}

func PackGenericValue(value BaseValue) ([]byte, error) {
	if value == nil {
		return nil, fmt.Errorf("value length zero")
	}
	return value.PackValue(), nil
}
