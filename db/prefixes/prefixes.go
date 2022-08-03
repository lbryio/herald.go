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
	}
}

// PrefixRowKV is a generic key/value pair for a prefix.
type PrefixRowKV struct {
	Key   interface{}
	Value interface{}
}

type DBStateKey struct {
	Prefix []byte `json:"prefix"`
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
	var histFlushCount uint32
	var compFlushCount uint32
	var compCursor uint32
	histFlushCount = (OnesCompTwiddle32 - uint32(v.HistFlushCount))
	compFlushCount = (OnesCompTwiddle32 - uint32(v.CompFlushCount))
	compCursor = (OnesCompTwiddle32 - uint32(v.CompCursor))

	binary.BigEndian.PutUint32(value[32+4+4+32+4+4+1+1:], histFlushCount)
	binary.BigEndian.PutUint32(value[32+4+4+32+4+4+1+1+4:], compFlushCount)
	binary.BigEndian.PutUint32(value[32+4+4+32+4+4+1+1+4+4:], compCursor)
	binary.BigEndian.PutUint32(value[32+4+4+32+4+4+1+1+4+4+4:], v.EsSyncHeight)

	return value
}

func DBStateKeyPackPartialKey(key *DBStateKey) func(int) []byte {
	return func(fields int) []byte {
		return DBStateKeyPackPartial(key, fields)
	}
}

func DBStateKeyPackPartialfields(fields int) func(*DBStateKey) []byte {
	return func(u *DBStateKey) []byte {
		return DBStateKeyPackPartial(u, fields)
	}
}

func DBStateKeyPackPartial(k *DBStateKey, fields int) []byte {
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
		HistFlushCount: int32(^binary.BigEndian.Uint32(value[32+4+4+32+4+4+1+1:])),
		CompFlushCount: int32(^binary.BigEndian.Uint32(value[32+4+4+32+4+4+1+1+4:])),
		CompCursor:     int32(^binary.BigEndian.Uint32(value[32+4+4+32+4+4+1+1+4+4:])),
		EsSyncHeight:   binary.BigEndian.Uint32(value[32+4+4+32+4+4+1+1+4+4+4:]),
	}
	return x
}

type UndoKey struct {
	Prefix []byte `json:"prefix"`
	Height uint64 `json:"height"`
}

type UndoValue struct {
	Data []byte `json:"data"`
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

func UndoKeyPackPartialKey(key *UndoKey) func(int) []byte {
	return func(fields int) []byte {
		return UndoKeyPackPartial(key, fields)
	}
}

func UndoKeyPackPartialfields(fields int) func(*UndoKey) []byte {
	return func(u *UndoKey) []byte {
		return UndoKeyPackPartial(u, fields)
	}
}

func UndoKeyPackPartial(k *UndoKey, fields int) []byte {
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
	Prefix []byte `json:"prefix"`
	HashX  []byte `json:"hashx"`
	TxNum  uint32 `json:"tx_num"`
	Nout   uint16 `json:"nout"`
}

type UTXOValue struct {
	Amount uint64 `json:"amount"`
}

type HashXUTXOKey struct {
	Prefix      []byte `json:"prefix"`
	ShortTXHash []byte `json:"short_tx_hash"`
	TxNum       uint32 `json:"tx_num"`
	Nout        uint16 `json:"nout"`
}

type HashXUTXOValue struct {
	HashX []byte `json:"hashx"`
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

// HashXUTXOKeyPackPartialKey creates a pack partial key function for n fields.
func HashXUTXOKeyPackPartialKey(key *HashXUTXOKey) func(int) []byte {
	return func(fields int) []byte {
		return HashXUTXOKeyPackPartial(key, fields)
	}
}

// HashXUTXOKeyPackPartialfields creates a pack partial key function for n fields.
func HashXUTXOKeyPackPartialfields(fields int) func(*HashXUTXOKey) []byte {
	return func(u *HashXUTXOKey) []byte {
		return HashXUTXOKeyPackPartial(u, fields)
	}
}

// HashXUTXOKeyPackPartial packs a variable number of fields into a byte
// array
func HashXUTXOKeyPackPartial(k *HashXUTXOKey, fields int) []byte {
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
	Prefix []byte `json:"prefix"`
	HashX  []byte `json:"hashx"`
	Height uint32 `json:"height"`
}

type HashXHistoryValue struct {
	HashXes []uint16 `json:"hashxes"`
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

// HashXHistoryKeyPackPartialKey creates a pack partial key function for n fields.
func HashXHistoryKeyPackPartialKey(key *HashXHistoryKey) func(int) []byte {
	return func(fields int) []byte {
		return HashXHistoryKeyPackPartial(key, fields)
	}
}

// HashXHistoryKeyPackPartialfields creates a pack partial key function for n fields.
func HashXHistoryKeyPackPartialfields(fields int) func(*HashXHistoryKey) []byte {
	return func(u *HashXHistoryKey) []byte {
		return HashXHistoryKeyPackPartial(u, fields)
	}
}

// HashXHistoryKeyPackPartial packs a variable number of fields into a byte
// array
func HashXHistoryKeyPackPartial(k *HashXHistoryKey, fields int) []byte {
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
	Prefix []byte `json:"prefix"`
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

func BlockHashKeyPackPartialKey(key *BlockHashKey) func(int) []byte {
	return func(fields int) []byte {
		return BlockHashKeyPackPartial(key, fields)
	}
}

func BlockHashKeyPackPartialfields(fields int) func(*BlockHashKey) []byte {
	return func(u *BlockHashKey) []byte {
		return BlockHashKeyPackPartial(u, fields)
	}
}

func BlockHashKeyPackPartial(k *BlockHashKey, fields int) []byte {
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
	Prefix []byte `json:"prefix"`
	Height uint32 `json:"height"`
}

type BlockTxsValue struct {
	TxHashes []*chainhash.Hash `json:"tx_hashes"`
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

func BlockTxsKeyPackPartialKey(key *BlockTxsKey) func(int) []byte {
	return func(fields int) []byte {
		return BlockTxsKeyPackPartial(key, fields)
	}
}

func BlockTxsKeyPackPartialfields(fields int) func(*BlockTxsKey) []byte {
	return func(u *BlockTxsKey) []byte {
		return BlockTxsKeyPackPartial(u, fields)
	}
}

func BlockTxsKeyPackPartial(k *BlockTxsKey, fields int) []byte {
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
	Prefix []byte `json:"prefix"`
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

func TxCountKeyPackPartialKey(key *TxCountKey) func(int) []byte {
	return func(fields int) []byte {
		return TxCountKeyPackPartial(key, fields)
	}
}

func TxCountKeyPackPartialfields(fields int) func(*TxCountKey) []byte {
	return func(u *TxCountKey) []byte {
		return TxCountKeyPackPartial(u, fields)
	}
}

func TxCountKeyPackPartial(k *TxCountKey, fields int) []byte {
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
	Prefix []byte `json:"prefix"`
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

func TxHashKeyPackPartialKey(key *TxHashKey) func(int) []byte {
	return func(fields int) []byte {
		return TxHashKeyPackPartial(key, fields)
	}
}

func TxHashKeyPackPartialfields(fields int) func(*TxHashKey) []byte {
	return func(u *TxHashKey) []byte {
		return TxHashKeyPackPartial(u, fields)
	}
}

func TxHashKeyPackPartial(k *TxHashKey, fields int) []byte {
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
	Prefix []byte          `json:"prefix"`
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

func TxNumKeyPackPartialKey(key *TxNumKey) func(int) []byte {
	return func(fields int) []byte {
		return TxNumKeyPackPartial(key, fields)
	}
}

func TxNumKeyPackPartialfields(fields int) func(*TxNumKey) []byte {
	return func(u *TxNumKey) []byte {
		return TxNumKeyPackPartial(u, fields)
	}
}

func TxNumKeyPackPartial(k *TxNumKey, fields int) []byte {
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
	Prefix []byte          `json:"prefix"`
	TxHash *chainhash.Hash `json:"tx_hash"`
}

type TxValue struct {
	RawTx []byte `json:"raw_tx"`
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

func TxKeyPackPartialKey(key *TxKey) func(int) []byte {
	return func(fields int) []byte {
		return TxKeyPackPartial(key, fields)
	}
}

func TxKeyPackPartialfields(fields int) func(*TxKey) []byte {
	return func(u *TxKey) []byte {
		return TxKeyPackPartial(u, fields)
	}
}

func TxKeyPackPartial(k *TxKey, fields int) []byte {
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
	Prefix []byte `json:"prefix"`
	Height uint32 `json:"height"`
}

type BlockHeaderValue struct {
	Header []byte `json:"header"`
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

func BlockHeaderKeyPackPartialKey(key *BlockHeaderKey) func(int) []byte {
	return func(fields int) []byte {
		return BlockHeaderKeyPackPartial(key, fields)
	}
}

func BlockHeaderKeyPackPartialfields(fields int) func(*BlockHeaderKey) []byte {
	return func(u *BlockHeaderKey) []byte {
		return BlockHeaderKeyPackPartial(u, fields)
	}
}

func BlockHeaderKeyPackPartial(k *BlockHeaderKey, fields int) []byte {
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
	Prefix    []byte `json:"prefix"`
	ClaimHash []byte `json:"claim_hash"`
}

type ClaimToTXOValue struct {
	TxNum                   uint32 `json:"tx_num"`
	Position                uint16 `json:"position"`
	RootTxNum               uint32 `json:"root_tx_num"`
	RootPosition            uint16 `json:"root_position"`
	Amount                  uint64 `json:"amount"`
	ChannelSignatureIsValid bool   `json:"channel_signature_is_valid"`
	Name                    string `json:"name"`
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

func ClaimToTXOKeyPackPartialKey(key *ClaimToTXOKey) func(int) []byte {
	return func(fields int) []byte {
		return ClaimToTXOKeyPackPartial(key, fields)
	}
}

func ClaimToTXOKeyPackPartialfields(fields int) func(*ClaimToTXOKey) []byte {
	return func(u *ClaimToTXOKey) []byte {
		return ClaimToTXOKeyPackPartial(u, fields)
	}
}

func ClaimToTXOKeyPackPartial(k *ClaimToTXOKey, fields int) []byte {
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
		Name:                    string(value[23 : 23+nameLen]),
	}
}

type TXOToClaimKey struct {
	Prefix   []byte `json:"prefix"`
	TxNum    uint32 `json:"tx_num"`
	Position uint16 `json:"position"`
}

type TXOToClaimValue struct {
	ClaimHash []byte `json:"claim_hash"`
	Name      string `json:"name"`
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

func TXOToClaimKeyPackPartialKey(key *TXOToClaimKey) func(int) []byte {
	return func(fields int) []byte {
		return TXOToClaimKeyPackPartial(key, fields)
	}
}

func TXOToClaimKeyPackPartialfields(fields int) func(*TXOToClaimKey) []byte {
	return func(u *TXOToClaimKey) []byte {
		return TXOToClaimKeyPackPartial(u, fields)
	}
}

func TXOToClaimKeyPackPartial(k *TXOToClaimKey, fields int) []byte {
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
		ClaimHash: value[:20],
		Name:      string(value[22 : 22+nameLen]),
	}
}

type ClaimShortIDKey struct {
	Prefix         []byte `json:"prefix"`
	NormalizedName string `json:"normalized_name"`
	PartialClaimId string `json:"partial_claim_id"`
	RootTxNum      uint32 `json:"root_tx_num"`
	RootPosition   uint16 `json:"root_position"`
}

type ClaimShortIDValue struct {
	TxNum    uint32 `json:"tx_num"`
	Position uint16 `json:"position"`
}

func NewClaimShortIDKey(normalizedName, partialClaimId string) *ClaimShortIDKey {
	return &ClaimShortIDKey{
		Prefix:         []byte{ClaimShortIdPrefix},
		NormalizedName: normalizedName,
		PartialClaimId: partialClaimId,
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

func ClaimShortIDKeyPackPartialKey(key *ClaimShortIDKey) func(int) []byte {
	return func(fields int) []byte {
		return ClaimShortIDKeyPackPartial(key, fields)
	}
}

func ClaimShortIDKeyPackPartialfields(fields int) func(*ClaimShortIDKey) []byte {
	return func(u *ClaimShortIDKey) []byte {
		return ClaimShortIDKeyPackPartial(u, fields)
	}
}

func ClaimShortIDKeyPackPartial(k *ClaimShortIDKey, fields int) []byte {
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
		Prefix:         key[:prefixLen],
		NormalizedName: string(key[prefixLen+2 : prefixLen+2+nameLen]),
		PartialClaimId: string(key[prefixLen+2+nameLen+1 : prefixLen+2+nameLen+1+partialClaimLen]),
		RootTxNum:      binary.BigEndian.Uint32(key[prefixLen+2+nameLen+1+partialClaimLen:]),
		RootPosition:   binary.BigEndian.Uint16(key[prefixLen+2+nameLen+1+partialClaimLen+4:]),
	}
}

func ClaimShortIDValueUnpack(value []byte) *ClaimShortIDValue {
	return &ClaimShortIDValue{
		TxNum:    binary.BigEndian.Uint32(value),
		Position: binary.BigEndian.Uint16(value[4:]),
	}
}

type ClaimToChannelKey struct {
	Prefix    []byte `json:"prefix"`
	ClaimHash []byte `json:"claim_hash"`
	TxNum     uint32 `json:"tx_num"`
	Position  uint16 `json:"position"`
}

type ClaimToChannelValue struct {
	SigningHash []byte `json:"signing_hash"`
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

func ClaimToChannelKeyPackPartialKey(key *ClaimToChannelKey) func(int) []byte {
	return func(fields int) []byte {
		return ClaimToChannelKeyPackPartial(key, fields)
	}
}

func ClaimToChannelKeyPackPartialfields(fields int) func(*ClaimToChannelKey) []byte {
	return func(u *ClaimToChannelKey) []byte {
		return ClaimToChannelKeyPackPartial(u, fields)
	}
}

func ClaimToChannelKeyPackPartial(k *ClaimToChannelKey, fields int) []byte {
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
	Prefix      []byte `json:"prefix"`
	SigningHash []byte `json:"signing_hash"`
	Name        string `json:"name"`
	TxNum       uint32 `json:"tx_num"`
	Position    uint16 `json:"position"`
}

type ChannelToClaimValue struct {
	ClaimHash []byte `json:"claim_hash"`
}

func NewChannelToClaimKey(channelHash []byte, normalizedName string) *ChannelToClaimKey {
	return &ChannelToClaimKey{
		Prefix:      []byte{ChannelToClaim},
		SigningHash: channelHash,
		Name:        normalizedName,
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

func ChannelToClaimKeyPackPartialKey(key *ChannelToClaimKey) func(int) []byte {
	return func(fields int) []byte {
		return ChannelToClaimKeyPackPartial(key, fields)
	}
}

func ChannelToClaimKeyPackPartialfields(fields int) func(*ChannelToClaimKey) []byte {
	return func(u *ChannelToClaimKey) []byte {
		return ChannelToClaimKeyPackPartial(u, fields)
	}
}

func ChannelToClaimKeyPackPartial(k *ChannelToClaimKey, fields int) []byte {
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
		Prefix:      key[:prefixLen],
		SigningHash: key[prefixLen : prefixLen+20],
		Name:        string(key[prefixLen+22 : prefixLen+22+nameLen]),
		TxNum:       binary.BigEndian.Uint32(key[prefixLen+22+nameLen:]),
		Position:    binary.BigEndian.Uint16(key[prefixLen+22+nameLen+4:]),
	}
}

func ChannelToClaimValueUnpack(value []byte) *ChannelToClaimValue {
	return &ChannelToClaimValue{
		ClaimHash: value[:20],
	}
}

type ChannelCountKey struct {
	Prefix      []byte `json:"prefix"`
	ChannelHash []byte `json:"channel_hash"`
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

func ChannelCountKeyPackPartialKey(key *ChannelCountKey) func(int) []byte {
	return func(fields int) []byte {
		return ChannelCountKeyPackPartial(key, fields)
	}
}

func ChannelCountKeyPackPartialfields(fields int) func(*ChannelCountKey) []byte {
	return func(u *ChannelCountKey) []byte {
		return ChannelCountKeyPackPartial(u, fields)
	}
}

func ChannelCountKeyPackPartial(k *ChannelCountKey, fields int) []byte {
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
	Prefix    []byte `json:"prefix"`
	ClaimHash []byte `json:"claim_hash"`
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

func SupportAmountKeyPackPartialKey(key *SupportAmountKey) func(int) []byte {
	return func(fields int) []byte {
		return SupportAmountKeyPackPartial(key, fields)
	}
}

func SupportAmountKeyPackPartialfields(fields int) func(*SupportAmountKey) []byte {
	return func(u *SupportAmountKey) []byte {
		return SupportAmountKeyPackPartial(u, fields)
	}
}

func SupportAmountKeyPackPartial(k *SupportAmountKey, fields int) []byte {
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
	Prefix    []byte `json:"prefix"`
	ClaimHash []byte `json:"claim_hash"`
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

func ClaimToSupportKeyPackPartialKey(key *ClaimToSupportKey) func(int) []byte {
	return func(fields int) []byte {
		return ClaimToSupportKeyPackPartial(key, fields)
	}
}

func ClaimToSupportKeyPackPartialfields(fields int) func(*ClaimToSupportKey) []byte {
	return func(u *ClaimToSupportKey) []byte {
		return ClaimToSupportKeyPackPartial(u, fields)
	}
}

func ClaimToSupportKeyPackPartial(k *ClaimToSupportKey, fields int) []byte {
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
	Prefix   []byte `json:"prefix"`
	TxNum    uint32 `json:"tx_num"`
	Position uint16 `json:"position"`
}

type SupportToClaimValue struct {
	ClaimHash []byte `json:"claim_hash"`
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

func SupportToClaimKeyPackPartialKey(key *SupportToClaimKey) func(int) []byte {
	return func(fields int) []byte {
		return SupportToClaimKeyPackPartial(key, fields)
	}
}

func SupportToClaimKeyPackPartialfields(fields int) func(*SupportToClaimKey) []byte {
	return func(u *SupportToClaimKey) []byte {
		return SupportToClaimKeyPackPartial(u, fields)
	}
}

func SupportToClaimKeyPackPartial(k *SupportToClaimKey, fields int) []byte {
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
	Prefix     []byte `json:"prefix"`
	Expiration uint32 `json:"expiration"`
	TxNum      uint32 `json:"tx_num"`
	Position   uint16 `json:"position"`
}

type ClaimExpirationValue struct {
	ClaimHash      []byte `json:"claim_hash"`
	NormalizedName string `json:"normalized_name"`
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

func ClaimExpirationKeyPackPartialKey(key *ClaimExpirationKey) func(int) []byte {
	return func(fields int) []byte {
		return ClaimExpirationKeyPackPartial(key, fields)
	}
}

func ClaimExpirationKeyPackPartialfields(fields int) func(*ClaimExpirationKey) []byte {
	return func(u *ClaimExpirationKey) []byte {
		return ClaimExpirationKeyPackPartial(u, fields)
	}
}

func ClaimExpirationKeyPackPartial(k *ClaimExpirationKey, fields int) []byte {
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
		ClaimHash:      value[:20],
		NormalizedName: string(value[22 : 22+nameLen]),
	}
}

type ClaimTakeoverKey struct {
	Prefix         []byte `json:"prefix"`
	NormalizedName string `json:"normalized_name"`
}

type ClaimTakeoverValue struct {
	ClaimHash []byte `json:"claim_hash"`
	Height    uint32 `json:"height"`
}

func NewClaimTakeoverKey(normalizedName string) *ClaimTakeoverKey {
	return &ClaimTakeoverKey{
		Prefix:         []byte{ClaimTakeover},
		NormalizedName: normalizedName,
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

func ClaimTakeoverKeyPackPartialKey(key *ClaimTakeoverKey) func(int) []byte {
	return func(fields int) []byte {
		return ClaimTakeoverKeyPackPartial(key, fields)
	}
}

func ClaimTakeoverKeyPackPartialfields(fields int) func(*ClaimTakeoverKey) []byte {
	return func(u *ClaimTakeoverKey) []byte {
		return ClaimTakeoverKeyPackPartial(u, fields)
	}
}

func ClaimTakeoverKeyPackPartial(k *ClaimTakeoverKey, fields int) []byte {
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
		Prefix:         key[:prefixLen],
		NormalizedName: string(key[prefixLen+2 : prefixLen+2+int(nameLen)]),
	}
}

func ClaimTakeoverValueUnpack(value []byte) *ClaimTakeoverValue {
	return &ClaimTakeoverValue{
		ClaimHash: value[:20],
		Height:    binary.BigEndian.Uint32(value[20:]),
	}
}

type PendingActivationKey struct {
	Prefix   []byte `json:"prefix"`
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
	ClaimHash      []byte `json:"claim_hash"`
	NormalizedName string `json:"normalized_name"`
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

func PendingActivationKeyPackPartialKey(key *PendingActivationKey) func(int) []byte {
	return func(fields int) []byte {
		return PendingActivationKeyPackPartial(key, fields)
	}
}

func PendingActivationKeyPackPartialfields(fields int) func(*PendingActivationKey) []byte {
	return func(u *PendingActivationKey) []byte {
		return PendingActivationKeyPackPartial(u, fields)
	}
}

func PendingActivationKeyPackPartial(k *PendingActivationKey, fields int) []byte {
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
		ClaimHash:      value[:20],
		NormalizedName: string(value[22 : 22+nameLen]),
	}
}

type ActivationKey struct {
	Prefix   []byte `json:"prefix"`
	TxoType  uint8  `json:"txo_type"`
	TxNum    uint32 `json:"tx_num"`
	Position uint16 `json:"position"`
}

type ActivationValue struct {
	Height         uint32 `json:"height"`
	ClaimHash      []byte `json:"claim_hash"`
	NormalizedName string `json:"normalized_name"`
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

func ActivationKeyPackPartialKey(key *ActivationKey) func(int) []byte {
	return func(fields int) []byte {
		return ActivationKeyPackPartial(key, fields)
	}
}

func ActivationKeyPackPartialfields(fields int) func(*ActivationKey) []byte {
	return func(u *ActivationKey) []byte {
		return ActivationKeyPackPartial(u, fields)
	}
}

func ActivationKeyPackPartial(k *ActivationKey, fields int) []byte {
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
		Height:         binary.BigEndian.Uint32(value),
		ClaimHash:      value[4 : 20+4],
		NormalizedName: string(value[26 : 26+nameLen]),
	}
}

type ActiveAmountKey struct {
	Prefix           []byte `json:"prefix"`
	ClaimHash        []byte `json:"claim_hash"`
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

func ActiveAmountKeyPackPartialKey(key *ActiveAmountKey) func(int) []byte {
	return func(fields int) []byte {
		return ActiveAmountKeyPackPartial(key, fields)
	}
}

func ActiveAmountKeyPackPartialfields(fields int) func(*ActiveAmountKey) []byte {
	return func(u *ActiveAmountKey) []byte {
		return ActiveAmountKeyPackPartial(u, fields)
	}
}

func ActiveAmountKeyPackPartial(k *ActiveAmountKey, fields int) []byte {
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

type EffectiveAmountKey struct {
	Prefix          []byte `json:"prefix"`
	NormalizedName  string `json:"normalized_name"`
	EffectiveAmount uint64 `json:"effective_amount"`
	TxNum           uint32 `json:"tx_num"`
	Position        uint16 `json:"position"`
}

type EffectiveAmountValue struct {
	ClaimHash []byte `json:"claim_hash"`
}

func NewEffectiveAmountKey(normalizedName string) *EffectiveAmountKey {
	return &EffectiveAmountKey{
		Prefix:         []byte{EffectiveAmount},
		NormalizedName: normalizedName,
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
	binary.BigEndian.PutUint64(key[prefixLen+nameLenLen:], OnesCompTwiddle64-k.EffectiveAmount)
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

func EffectiveAmountKeyPackPartialKey(key *EffectiveAmountKey) func(int) []byte {
	return func(fields int) []byte {
		return EffectiveAmountKeyPackPartial(key, fields)
	}
}

func EffectiveAmountKeyPackPartialfields(fields int) func(*EffectiveAmountKey) []byte {
	return func(u *EffectiveAmountKey) []byte {
		return EffectiveAmountKeyPackPartial(u, fields)
	}
}

func EffectiveAmountKeyPackPartial(k *EffectiveAmountKey, fields int) []byte {
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
			binary.BigEndian.PutUint64(key[prefixLen+nameLenLen:], OnesCompTwiddle64-k.EffectiveAmount)
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
		Prefix:          key[:prefixLen],
		NormalizedName:  string(key[prefixLen+2 : prefixLen+2+int(nameLen)]),
		EffectiveAmount: OnesCompTwiddle64 - binary.BigEndian.Uint64(key[prefixLen+2+int(nameLen):]),
		TxNum:           binary.BigEndian.Uint32(key[prefixLen+2+int(nameLen)+8:]),
		Position:        binary.BigEndian.Uint16(key[prefixLen+2+int(nameLen)+8+4:]),
	}
}

func EffectiveAmountValueUnpack(value []byte) *EffectiveAmountValue {
	return &EffectiveAmountValue{
		ClaimHash: value[:20],
	}
}

type RepostKey struct {
	Prefix    []byte `json:"prefix"`
	ClaimHash []byte `json:"claim_hash"`
}

type RepostValue struct {
	RepostedClaimHash []byte `json:"reposted_claim_hash"`
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

func RepostKeyPackPartialKey(key *RepostKey) func(int) []byte {
	return func(fields int) []byte {
		return RepostKeyPackPartial(key, fields)
	}
}

func RepostKeyPackPartialfields(fields int) func(*RepostKey) []byte {
	return func(u *RepostKey) []byte {
		return RepostKeyPackPartial(u, fields)
	}
}

func RepostKeyPackPartial(k *RepostKey, fields int) []byte {
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
	Prefix            []byte `json:"prefix"`
	RepostedClaimHash []byte `json:"reposted_claim_hash"`
	TxNum             uint32 `json:"tx_num"`
	Position          uint16 `json:"position"`
}

type RepostedValue struct {
	ClaimHash []byte `json:"claim_hash"`
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

func RepostedKeyPackPartialKey(key *RepostedKey) func(int) []byte {
	return func(fields int) []byte {
		return RepostedKeyPackPartial(key, fields)
	}
}

func RepostedKeyPackPartialfields(fields int) func(*RepostedKey) []byte {
	return func(u *RepostedKey) []byte {
		return RepostedKeyPackPartial(u, fields)
	}
}

func RepostedKeyPackPartial(k *RepostedKey, fields int) []byte {
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
	Prefix []byte `json:"prefix"`
	Height int32  `json:"height"`
}

type TouchedOrDeletedClaimValue struct {
	TouchedClaims [][]byte `json:"touched_claims"`
	DeletedClaims [][]byte `json:"deleted_claims"`
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

func TouchedOrDeletedClaimKeyPackPartialKey(key *TouchedOrDeletedClaimKey) func(int) []byte {
	return func(fields int) []byte {
		return TouchedOrDeletedClaimKeyPackPartial(key, fields)
	}
}

func TouchedOrDeletedClaimPackPartialfields(fields int) func(*TouchedOrDeletedClaimKey) []byte {
	return func(u *TouchedOrDeletedClaimKey) []byte {
		return TouchedOrDeletedClaimKeyPackPartial(u, fields)
	}
}

func TouchedOrDeletedClaimKeyPackPartial(k *TouchedOrDeletedClaimKey, fields int) []byte {
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
		TouchedClaims: touchedClaims,
		DeletedClaims: deletedClaims,
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

func UTXOKeyPackPartialKey(key *UTXOKey) func(int) []byte {
	return func(fields int) []byte {
		return UTXOKeyPackPartial(key, fields)
	}
}

func UTXOKeyPackPartialfields(fields int) func(*UTXOKey) []byte {
	return func(u *UTXOKey) []byte {
		return UTXOKeyPackPartial(u, fields)
	}
}

// UTXOKeyPackPartial packs a variable number of fields for a UTXOKey into
// a byte array.
func UTXOKeyPackPartial(k *UTXOKey, fields int) []byte {
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

// generic simulates a generic key packing / unpacking function for the prefixes
func generic(voidstar interface{}, firstByte byte, function byte, functionName string) (interface{}, error) {
	var data []byte
	if function < 2 {
		data = voidstar.([]byte)
	}
	switch uint16(firstByte) | uint16(function)<<8 {
	case ClaimToSupport:
		return ClaimToSupportKeyUnpack(data), nil
	case ClaimToSupport | 1<<8:
		return ClaimToSupportValueUnpack(data), nil
	case ClaimToSupport | 2<<8:
		return voidstar.(*ClaimToSupportKey).PackKey(), nil
	case ClaimToSupport | 3<<8:
		return voidstar.(*ClaimToSupportValue).PackValue(), nil
	case ClaimToSupport | 4<<8:
		return ClaimToSupportKeyPackPartialKey(voidstar.(*ClaimToSupportKey)), nil
	case SupportToClaim:
		return SupportToClaimKeyUnpack(data), nil
	case SupportToClaim | 1<<8:
		return SupportToClaimValueUnpack(data), nil
	case SupportToClaim | 2<<8:
		return voidstar.(*SupportToClaimKey).PackKey(), nil
	case SupportToClaim | 3<<8:
		return voidstar.(*SupportToClaimValue).PackValue(), nil
	case SupportToClaim | 4<<8:
		return SupportToClaimKeyPackPartialKey(voidstar.(*SupportToClaimKey)), nil
	case ClaimToTXO:
		return ClaimToTXOKeyUnpack(data), nil
	case ClaimToTXO | 1<<8:
		return ClaimToTXOValueUnpack(data), nil
	case ClaimToTXO | 2<<8:
		return voidstar.(*ClaimToTXOKey).PackKey(), nil
	case ClaimToTXO | 3<<8:
		return voidstar.(*ClaimToTXOValue).PackValue(), nil
	case ClaimToTXO | 4<<8:
		return ClaimToTXOKeyPackPartialKey(voidstar.(*ClaimToTXOKey)), nil
	case TXOToClaim:
		return TXOToClaimKeyUnpack(data), nil
	case TXOToClaim | 1<<8:
		return TXOToClaimValueUnpack(data), nil
	case TXOToClaim | 2<<8:
		return voidstar.(*TXOToClaimKey).PackKey(), nil
	case TXOToClaim | 3<<8:
		return voidstar.(*TXOToClaimValue).PackValue(), nil
	case TXOToClaim | 4<<8:
		return TXOToClaimKeyPackPartialKey(voidstar.(*TXOToClaimKey)), nil

	case ClaimToChannel:
		return ClaimToChannelKeyUnpack(data), nil
	case ClaimToChannel | 1<<8:
		return ClaimToChannelValueUnpack(data), nil
	case ClaimToChannel | 2<<8:
		return voidstar.(*ClaimToChannelKey).PackKey(), nil
	case ClaimToChannel | 3<<8:
		return voidstar.(*ClaimToChannelValue).PackValue(), nil
	case ClaimToChannel | 4<<8:
		return ClaimToChannelKeyPackPartialKey(voidstar.(*ClaimToChannelKey)), nil
	case ChannelToClaim:
		return ChannelToClaimKeyUnpack(data), nil
	case ChannelToClaim | 1<<8:
		return ChannelToClaimValueUnpack(data), nil
	case ChannelToClaim | 2<<8:
		return voidstar.(*ChannelToClaimKey).PackKey(), nil
	case ChannelToClaim | 3<<8:
		return voidstar.(*ChannelToClaimValue).PackValue(), nil
	case ChannelToClaim | 4<<8:
		return ChannelToClaimKeyPackPartialKey(voidstar.(*ChannelToClaimKey)), nil

	case ClaimShortIdPrefix:
		return ClaimShortIDKeyUnpack(data), nil
	case ClaimShortIdPrefix | 1<<8:
		return ClaimShortIDValueUnpack(data), nil
	case ClaimShortIdPrefix | 2<<8:
		return voidstar.(*ClaimShortIDKey).PackKey(), nil
	case ClaimShortIdPrefix | 3<<8:
		return voidstar.(*ClaimShortIDValue).PackValue(), nil
	case ClaimShortIdPrefix | 4<<8:
		return ClaimShortIDKeyPackPartialKey(voidstar.(*ClaimShortIDKey)), nil
	case EffectiveAmount:
		return EffectiveAmountKeyUnpack(data), nil
	case EffectiveAmount | 1<<8:
		return EffectiveAmountValueUnpack(data), nil
	case EffectiveAmount | 2<<8:
		return voidstar.(*EffectiveAmountKey).PackKey(), nil
	case EffectiveAmount | 3<<8:
		return voidstar.(*EffectiveAmountValue).PackValue(), nil
	case EffectiveAmount | 4<<8:
		return EffectiveAmountKeyPackPartialKey(voidstar.(*EffectiveAmountKey)), nil
	case ClaimExpiration:
		return ClaimExpirationKeyUnpack(data), nil
	case ClaimExpiration | 1<<8:
		return ClaimExpirationValueUnpack(data), nil
	case ClaimExpiration | 2<<8:
		return voidstar.(*ClaimExpirationKey).PackKey(), nil
	case ClaimExpiration | 3<<8:
		return voidstar.(*ClaimExpirationValue).PackValue(), nil
	case ClaimExpiration | 4<<8:
		return ClaimExpirationKeyPackPartialKey(voidstar.(*ClaimExpirationKey)), nil

	case ClaimTakeover:
		return ClaimTakeoverKeyUnpack(data), nil
	case ClaimTakeover | 1<<8:
		return ClaimTakeoverValueUnpack(data), nil
	case ClaimTakeover | 2<<8:
		return voidstar.(*ClaimTakeoverKey).PackKey(), nil
	case ClaimTakeover | 3<<8:
		return voidstar.(*ClaimTakeoverValue).PackValue(), nil
	case ClaimTakeover | 4<<8:
		return ClaimTakeoverKeyPackPartialKey(voidstar.(*ClaimTakeoverKey)), nil
	case PendingActivation:
		return PendingActivationKeyUnpack(data), nil
	case PendingActivation | 1<<8:
		return PendingActivationValueUnpack(data), nil
	case PendingActivation | 2<<8:
		return voidstar.(*PendingActivationKey).PackKey(), nil
	case PendingActivation | 3<<8:
		return voidstar.(*PendingActivationValue).PackValue(), nil
	case PendingActivation | 4<<8:
		return PendingActivationKeyPackPartialKey(voidstar.(*PendingActivationKey)), nil
	case ActivatedClaimAndSupport:
		return ActivationKeyUnpack(data), nil
	case ActivatedClaimAndSupport | 1<<8:
		return ActivationValueUnpack(data), nil
	case ActivatedClaimAndSupport | 2<<8:
		return voidstar.(*ActivationKey).PackKey(), nil
	case ActivatedClaimAndSupport | 3<<8:
		return voidstar.(*ActivationValue).PackValue(), nil
	case ActivatedClaimAndSupport | 4<<8:
		return ActivationKeyPackPartialKey(voidstar.(*ActivationKey)), nil
	case ActiveAmount:
		return ActiveAmountKeyUnpack(data), nil
	case ActiveAmount | 1<<8:
		return ActiveAmountValueUnpack(data), nil
	case ActiveAmount | 2<<8:
		return voidstar.(*ActiveAmountKey).PackKey(), nil
	case ActiveAmount | 3<<8:
		return voidstar.(*ActiveAmountValue).PackValue(), nil
	case ActiveAmount | 4<<8:
		return ActiveAmountKeyPackPartialKey(voidstar.(*ActiveAmountKey)), nil

	case Repost:
		return RepostKeyUnpack(data), nil
	case Repost | 1<<8:
		return RepostValueUnpack(data), nil
	case Repost | 2<<8:
		return voidstar.(*RepostKey).PackKey(), nil
	case Repost | 3<<8:
		return voidstar.(*RepostValue).PackValue(), nil
	case Repost | 4<<8:
		return RepostKeyPackPartialKey(voidstar.(*RepostKey)), nil
	case RepostedClaim:
		return RepostedKeyUnpack(data), nil
	case RepostedClaim | 1<<8:
		return RepostedValueUnpack(data), nil
	case RepostedClaim | 2<<8:
		return voidstar.(*RepostedKey).PackKey(), nil
	case RepostedClaim | 3<<8:
		return voidstar.(*RepostedValue).PackValue(), nil
	case RepostedClaim | 4<<8:
		return RepostedKeyPackPartialKey(voidstar.(*RepostedKey)), nil

	case Undo:
		return UndoKeyUnpack(data), nil
	case Undo | 1<<8:
		return UndoValueUnpack(data), nil
	case Undo | 2<<8:
		return voidstar.(*UndoKey).PackKey(), nil
	case Undo | 3<<8:
		return voidstar.(*UndoValue).PackValue(), nil
	case Undo | 4<<8:
		return UndoKeyPackPartialKey(voidstar.(*UndoKey)), nil
	case ClaimDiff:
		return TouchedOrDeletedClaimKeyUnpack(data), nil
	case ClaimDiff | 1<<8:
		return TouchedOrDeletedClaimValueUnpack(data), nil
	case ClaimDiff | 2<<8:
		return voidstar.(*TouchedOrDeletedClaimKey).PackKey(), nil
	case ClaimDiff | 3<<8:
		return voidstar.(*TouchedOrDeletedClaimValue).PackValue(), nil
	case ClaimDiff | 4<<8:
		return TouchedOrDeletedClaimKeyPackPartialKey(voidstar.(*TouchedOrDeletedClaimKey)), nil

	case Tx:
		return TxKeyUnpack(data), nil
	case Tx | 1<<8:
		return TxValueUnpack(data), nil
	case Tx | 2<<8:
		return voidstar.(*TxKey).PackKey(), nil
	case Tx | 3<<8:
		return voidstar.(*TxValue).PackValue(), nil
	case Tx | 4<<8:
		return TxKeyPackPartialKey(voidstar.(*TxKey)), nil
	case BlockHash:
		return BlockHashKeyUnpack(data), nil
	case BlockHash | 1<<8:
		return BlockHashValueUnpack(data), nil
	case BlockHash | 2<<8:
		return voidstar.(*BlockHashKey).PackKey(), nil
	case BlockHash | 3<<8:
		return voidstar.(*BlockHashValue).PackValue(), nil
	case BlockHash | 4<<8:
		return BlockHashKeyPackPartialKey(voidstar.(*BlockHashKey)), nil
	case Header:
		return BlockHeaderKeyUnpack(data), nil
	case Header | 1<<8:
		return BlockHeaderValueUnpack(data), nil
	case Header | 2<<8:
		return voidstar.(*BlockHeaderKey).PackKey(), nil
	case Header | 3<<8:
		return voidstar.(*BlockHeaderValue).PackValue(), nil
	case Header | 4<<8:
		return BlockHeaderKeyPackPartialKey(voidstar.(*BlockHeaderKey)), nil
	case TxNum:
		return TxNumKeyUnpack(data), nil
	case TxNum | 1<<8:
		return TxNumValueUnpack(data), nil
	case TxNum | 2<<8:
		return voidstar.(*TxNumKey).PackKey(), nil
	case TxNum | 3<<8:
		return voidstar.(*TxNumValue).PackValue(), nil
	case TxNum | 4<<8:
		return TxNumKeyPackPartialKey(voidstar.(*TxNumKey)), nil

	case TxCount:
		return TxCountKeyUnpack(data), nil
	case TxCount | 1<<8:
		return TxCountValueUnpack(data), nil
	case TxCount | 2<<8:
		return voidstar.(*TxCountKey).PackKey(), nil
	case TxCount | 3<<8:
		return voidstar.(*TxCountValue).PackValue(), nil
	case TxCount | 4<<8:
		return TxCountKeyPackPartialKey(voidstar.(*TxCountKey)), nil
	case TxHash:
		return TxHashKeyUnpack(data), nil
	case TxHash | 1<<8:
		return TxHashValueUnpack(data), nil
	case TxHash | 2<<8:
		return voidstar.(*TxHashKey).PackKey(), nil
	case TxHash | 3<<8:
		return voidstar.(*TxHashValue).PackValue(), nil
	case TxHash | 4<<8:
		return TxHashKeyPackPartialKey(voidstar.(*TxHashKey)), nil
	case UTXO:
		return UTXOKeyUnpack(data), nil
	case UTXO | 1<<8:
		return UTXOValueUnpack(data), nil
	case UTXO | 2<<8:
		return voidstar.(*UTXOKey).PackKey(), nil
	case UTXO | 3<<8:
		return voidstar.(*UTXOValue).PackValue(), nil
	case UTXO | 4<<8:
		return UTXOKeyPackPartialKey(voidstar.(*UTXOKey)), nil
	case HashXUTXO:
		return HashXUTXOKeyUnpack(data), nil
	case HashXUTXO | 1<<8:
		return HashXUTXOValueUnpack(data), nil
	case HashXUTXO | 2<<8:
		return voidstar.(*HashXUTXOKey).PackKey(), nil
	case HashXUTXO | 3<<8:
		return voidstar.(*HashXUTXOValue).PackValue(), nil
	case HashXUTXO | 4<<8:
		return HashXUTXOKeyPackPartialKey(voidstar.(*HashXUTXOKey)), nil
	case HashXHistory:
		return HashXHistoryKeyUnpack(data), nil
	case HashXHistory | 1<<8:
		return HashXHistoryValueUnpack(data), nil
	case HashXHistory | 2<<8:
		return voidstar.(*HashXHistoryKey).PackKey(), nil
	case HashXHistory | 3<<8:
		return voidstar.(*HashXHistoryValue).PackValue(), nil
	case HashXHistory | 4<<8:
		return HashXHistoryKeyPackPartialKey(voidstar.(*HashXHistoryKey)), nil
	case DBState:
		return DBStateKeyUnpack(data), nil
	case DBState | 1<<8:
		return DBStateValueUnpack(data), nil
	case DBState | 2<<8:
		return voidstar.(*DBStateKey).PackKey(), nil
	case DBState | 3<<8:
		return voidstar.(*DBStateValue).PackValue(), nil
	case DBState | 4<<8:
		return DBStateKeyPackPartialKey(voidstar.(*DBStateKey)), nil

	case ChannelCount:
		return ChannelCountKeyUnpack(data), nil
	case ChannelCount | 1<<8:
		return ChannelCountValueUnpack(data), nil
	case ChannelCount | 2<<8:
		return voidstar.(*ChannelCountKey).PackKey(), nil
	case ChannelCount | 3<<8:
		return voidstar.(*ChannelCountValue).PackValue(), nil
	case ChannelCount | 4<<8:
		return ChannelCountKeyPackPartialKey(voidstar.(*ChannelCountKey)), nil
	case SupportAmount:
		return SupportAmountKeyUnpack(data), nil
	case SupportAmount | 1<<8:
		return SupportAmountValueUnpack(data), nil
	case SupportAmount | 2<<8:
		return voidstar.(*SupportAmountKey).PackKey(), nil
	case SupportAmount | 3<<8:
		return voidstar.(*SupportAmountValue).PackValue(), nil
	case SupportAmount | 4<<8:
		return SupportAmountKeyPackPartialKey(voidstar.(*SupportAmountKey)), nil
	case BlockTXs:
		return BlockTxsKeyUnpack(data), nil
	case BlockTXs | 1<<8:
		return BlockTxsValueUnpack(data), nil
	case BlockTXs | 2<<8:
		return voidstar.(*BlockTxsKey).PackKey(), nil
	case BlockTXs | 3<<8:
		return voidstar.(*BlockTxsValue).PackValue(), nil
	case BlockTXs | 4<<8:
		return BlockTxsKeyPackPartialKey(voidstar.(*BlockTxsKey)), nil

	}
	return nil, fmt.Errorf("%s function for %v not implemented", functionName, firstByte)
}

func UnpackGenericKey(key []byte) (interface{}, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key length zero")
	}
	return generic(key, key[0], 0, "unpack key")
}

func UnpackGenericValue(key, value []byte) (interface{}, error) {
	if len(key) == 0 {
		return nil, fmt.Errorf("key length zero")
	}
	if len(value) == 0 {
		return nil, fmt.Errorf("value length zero")
	}
	return generic(value, key[0], 1, "unpack value")
}

func PackPartialGenericKey(prefix byte, key interface{}, fields int) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("key length zero")
	}
	genericRes, err := generic(key, prefix, 4, "pack partial key")
	res := genericRes.(func(int) []byte)(fields)
	return res, err
}

func PackGenericKey(prefix byte, key interface{}) ([]byte, error) {
	if key == nil {
		return nil, fmt.Errorf("key length zero")
	}
	genericRes, err := generic(key, prefix, 2, "pack key")
	return genericRes.([]byte), err
}

func PackGenericValue(prefix byte, value interface{}) ([]byte, error) {
	if value == nil {
		return nil, fmt.Errorf("value length zero")
	}
	genericRes, err := generic(value, prefix, 3, "pack value")
	return genericRes.([]byte), err
}
