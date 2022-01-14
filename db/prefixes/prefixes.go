package prefixes

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"reflect"
	"sort"
	"strings"

	"github.com/lbryio/lbry.go/extras/errors"
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

	ACTIVATED_CLAIM_TXO_TYPE   = 1
	ACTIVATED_SUPPORT_TXO_TYPE = 2

	OnesCompTwiddle uint64 = 0xffffffffffffffff
)

type PrefixRowKV struct {
	Key   interface{}
	Value interface{}
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

/*
class HashXHistoryKey(NamedTuple):
    hashX: bytes
    height: int

    def __str__(self):
        return f"{self.__class__.__name__}(hashX={self.hashX.hex()}, height={self.height})"


class HashXHistoryValue(NamedTuple):
    hashXes: typing.List[int]
*/

type HashXHistoryKey struct {
	Prefix []byte `json:"prefix"`
	HashX  []byte `json:"hashx"`
	Height uint32 `json:"height"`
}

type HashXHistoryValue struct {
	HashXes []uint32 `json:"hashxes"`
}

/*
class BlockHashKey(NamedTuple):
    height: int


class BlockHashValue(NamedTuple):
    block_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(block_hash={self.block_hash.hex()})"
*/

type BlockHashKey struct {
	Prefix []byte `json:"prefix"`
	Height uint32 `json:"height"`
}

type BlockHashValue struct {
	BlockHash []byte `json:"block_hash"`
}

/*
class BlockTxsKey(NamedTuple):
    height: int


class BlockTxsValue(NamedTuple):
    tx_hashes: typing.List[bytes]
*/

type BlockTxsKey struct {
	Prefix []byte `json:"prefix"`
	Height uint32 `json:"height"`
}

type BlockTxsValue struct {
	TxHashes []byte `json:"tx_hashes"`
}

/*
class TxCountKey(NamedTuple):
    height: int


class TxCountValue(NamedTuple):
    tx_count: int
*/

type TxCountKey struct {
	Prefix []byte `json:"prefix"`
	Height uint32 `json:"height"`
}

type TxCountValue struct {
	TxCount uint32 `json:"tx_count"`
}

/*
class TxHashKey(NamedTuple):
    tx_num: int


class TxHashValue(NamedTuple):
    tx_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(tx_hash={self.tx_hash.hex()})"
*/

type TxHashKey struct {
	Prefix []byte `json:"prefix"`
	TxNum  uint32 `json:"tx_num"`
}

type TxHashValue struct {
	TxHash []byte `json:"tx_hash"`
}

/*
class TxNumKey(NamedTuple):
    tx_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(tx_hash={self.tx_hash.hex()})"


class TxNumValue(NamedTuple):
    tx_num: int
*/

type TxNumKey struct {
	Prefix []byte `json:"prefix"`
	TxHash []byte `json:"tx_hash"`
}

type TxNumValue struct {
	TxNum int32 `json:"tx_num"`
}

/*
class TxKey(NamedTuple):
    tx_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(tx_hash={self.tx_hash.hex()})"


class TxValue(NamedTuple):
    raw_tx: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(raw_tx={base64.b64encode(self.raw_tx)})"
*/

type TxKey struct {
	Prefix []byte `json:"prefix"`
	TxHash []byte `json:"tx_hash"`
}

type TxValue struct {
	RawTx []byte `json:"raw_tx"`
}

/*
class BlockHeaderKey(NamedTuple):
    height: int


class BlockHeaderValue(NamedTuple):
    header: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(header={base64.b64encode(self.header)})"
*/

type BlockHeaderKey struct {
	Prefix []byte `json:"prefix"`
	Height int32  `json:"height"`
}

type BlockHeaderValue struct {
	Header []byte `json:"header"`
}

/*

class ClaimToTXOKey(typing.NamedTuple):
    claim_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()})"


class ClaimToTXOValue(typing.NamedTuple):
    tx_num: int
    position: int
    root_tx_num: int
    root_position: int
    amount: int
    # activation: int
    channel_signature_is_valid: bool
    name: str

    @property
    def normalized_name(self) -> str:
        try:
            return normalize_name(self.name)
        except UnicodeDecodeError:
            return self.name
*/

type ClaimToTXOKey struct {
	Prefix    []byte `json:"prefix"`
	ClaimHash []byte `json:"claim_hash"`
}

type ClaimToTXOValue struct {
	TxNum                   int32  `json:"tx_num"`
	Position                int32  `json:"position"`
	RootTxNum               int32  `json:"root_tx_num"`
	RootPosition            int32  `json:"root_position"`
	Amount                  int32  `json:"amount"`
	ChannelSignatureIsValid bool   `json:"channel_signature_is_valid"`
	Name                    string `json:"name"`
}

func (v *ClaimToTXOValue) NormalizedName() string {
	//TODO implemented
	return v.Name
}

/*
class TXOToClaimKey(typing.NamedTuple):
    tx_num: int
    position: int


class TXOToClaimValue(typing.NamedTuple):
    claim_hash: bytes
    name: str

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()}, name={self.name})"
*/

type TXOToClaimKey struct {
	Prefix   []byte `json:"prefix"`
	TxNum    int32  `json:"tx_num"`
	Position int32  `json:"position"`
}

type TXOToClaimValue struct {
	ClaimHash []byte `json:"claim_hash"`
	Name      string `json:"name"`
}

/*
class ClaimShortIDKey(typing.NamedTuple):
    normalized_name: str
    partial_claim_id: str
    root_tx_num: int
    root_position: int

    def __str__(self):
        return f"{self.__class__.__name__}(normalized_name={self.normalized_name}, " \
               f"partial_claim_id={self.partial_claim_id}, " \
               f"root_tx_num={self.root_tx_num}, root_position={self.root_position})"


class ClaimShortIDValue(typing.NamedTuple):
    tx_num: int
    position: int
*/

type ClaimShortIDKey struct {
	Prefix         []byte `json:"prefix"`
	NormalizedName string `json:"normalized_name"`
	PartialClaimId string `json:"partial_claim_id"`
	RootTxNum      int32  `json:"root_tx_num"`
	RootPosition   int32  `json:"root_position"`
}

type ClaimShortIDValue struct {
	TxNum    int32 `json:"tx_num"`
	Position int32 `json:"position"`
}

/*
class ClaimToChannelKey(typing.NamedTuple):
    claim_hash: bytes
    tx_num: int
    position: int

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()}, " \
               f"tx_num={self.tx_num}, position={self.position})"


class ClaimToChannelValue(typing.NamedTuple):
    signing_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(signing_hash={self.signing_hash.hex()})"
*/

type ClaimToChannelKey struct {
	Prefix    []byte `json:"prefix"`
	ClaimHash []byte `json:"claim_hash"`
	TxNum     int32  `json:"tx_num"`
	Position  int32  `json:"position"`
}

type ClaimToChannelValue struct {
	SigningHash []byte `json:"signing_hash"`
}

/*
class ChannelToClaimKey(typing.NamedTuple):
    signing_hash: bytes
    name: str
    tx_num: int
    position: int

    def __str__(self):
        return f"{self.__class__.__name__}(signing_hash={self.signing_hash.hex()}, name={self.name}, " \
               f"tx_num={self.tx_num}, position={self.position})"


class ChannelToClaimValue(typing.NamedTuple):
    claim_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()})"
*/

type ChannelToClaimKey struct {
	Prefix      []byte `json:"prefix"`
	SigningHash []byte `json:"signing_hash"`
	Name        string `json:"name"`
	TxNum       int32  `json:"tx_num"`
	Position    int32  `json:"position"`
}

type ChannelToClaimValue struct {
	ClaimHash []byte `json:"claim_hash"`
}

/*

class ChannelCountKey(typing.NamedTuple):
    channel_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(channel_hash={self.channel_hash.hex()})"


class ChannelCountValue(typing.NamedTuple):
    count: int
*/

type ChannelCountKey struct {
	Prefix      []byte `json:"prefix"`
	ChannelHash []byte `json:"channel_hash"`
}

type ChannelCountValue struct {
	Count int32 `json:"count"`
}

/*
class SupportAmountKey(typing.NamedTuple):
    claim_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()})"


class SupportAmountValue(typing.NamedTuple):
    amount: int
*/

type SupportAmountKey struct {
	Prefix    []byte `json:"prefix"`
	ClaimHash []byte `json:"claim_hash"`
}

type SupportAmountValue struct {
	Amount int32 `json:"amount"`
}

/*

class ClaimToSupportKey(typing.NamedTuple):
    claim_hash: bytes
    tx_num: int
    position: int

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()}, tx_num={self.tx_num}, " \
               f"position={self.position})"


class ClaimToSupportValue(typing.NamedTuple):
    amount: int
*/

type ClaimToSupportKey struct {
	Prefix    []byte `json:"prefix"`
	ClaimHash []byte `json:"claim_hash"`
	TxNum     int32  `json:"tx_num"`
	Position  int32  `json:"position"`
}

type ClaimToSupportValue struct {
	Amount int32 `json:"amount"`
}

/*
class SupportToClaimKey(typing.NamedTuple):
    tx_num: int
    position: int


class SupportToClaimValue(typing.NamedTuple):
    claim_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()})"
*/

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

func SupportToClaimKeyPackPartialNFields(nFields int) func(*SupportToClaimKey) []byte {
	return func(u *SupportToClaimKey) []byte {
		return SupportToClaimKeyPackPartial(u, nFields)
	}
}

func SupportToClaimKeyPackPartial(k *SupportToClaimKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 2 {
		nFields = 2
	}
	if nFields < 0 {
		nFields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
		switch i {
		case 1:
			n += 4
		case 2:
			n += 2
		}
	}

	key := make([]byte, n)

	for i := 0; i <= nFields; i++ {
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

/*
class ClaimExpirationKey(typing.NamedTuple):
    expiration: int
    tx_num: int
    position: int


class ClaimExpirationValue(typing.NamedTuple):
    claim_hash: bytes
    normalized_name: str

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()}, normalized_name={self.normalized_name})"
*/

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

func ClaimExpirationKeyPackPartialNFields(nFields int) func(*ClaimExpirationKey) []byte {
	return func(u *ClaimExpirationKey) []byte {
		return ClaimExpirationKeyPackPartial(u, nFields)
	}
}

func ClaimExpirationKeyPackPartial(k *ClaimExpirationKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 3 {
		nFields = 3
	}
	if nFields < 0 {
		nFields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
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

	for i := 0; i <= nFields; i++ {
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

/*
class ClaimTakeoverKey(typing.NamedTuple):
    normalized_name: str


class ClaimTakeoverValue(typing.NamedTuple):
    claim_hash: bytes
    height: int

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()}, height={self.height})"
*/

type ClaimTakeoverKey struct {
	Prefix         []byte `json:"prefix"`
	NormalizedName string `json:"normalized_name"`
}

type ClaimTakeoverValue struct {
	ClaimHash []byte `json:"claim_hash"`
	Height    uint32 `json:"height"`
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

func ClaimTakeoverKeyPackPartialNFields(nFields int) func(*ClaimTakeoverKey) []byte {
	return func(u *ClaimTakeoverKey) []byte {
		return ClaimTakeoverKeyPackPartial(u, nFields)
	}
}

func ClaimTakeoverKeyPackPartial(k *ClaimTakeoverKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 1 {
		nFields = 1
	}
	if nFields < 0 {
		nFields = 0
	}

	prefixLen := 1
	nameLen := len(k.NormalizedName)
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
		switch i {
		case 1:
			n += 2 + nameLen
		}
	}

	key := make([]byte, n)

	for i := 0; i <= nFields; i++ {
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

/*

class PendingActivationKey(typing.NamedTuple):
    height: int
    txo_type: int
    tx_num: int
    position: int

    @property
    def is_support(self) -> bool:
        return self.txo_type == ACTIVATED_SUPPORT_TXO_TYPE

    @property
    def is_claim(self) -> bool:
        return self.txo_type == ACTIVATED_CLAIM_TXO_TYPE


class PendingActivationValue(typing.NamedTuple):
    claim_hash: bytes
    normalized_name: str

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()}, normalized_name={self.normalized_name})"
*/

type PendingActivationKey struct {
	Prefix   []byte `json:"prefix"`
	Height   uint32 `json:"height"`
	TxoType  uint8  `json:"txo_type"`
	TxNum    uint32 `json:"tx_num"`
	Position uint16 `json:"position"`
}

func (k *PendingActivationKey) IsSupport() bool {
	return k.TxoType == ACTIVATED_SUPPORT_TXO_TYPE
}

func (k *PendingActivationKey) IsClaim() bool {
	return k.TxoType == ACTIVATED_CLAIM_TXO_TYPE
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

func PendingActivationKeyPackPartialNFields(nFields int) func(*PendingActivationKey) []byte {
	return func(u *PendingActivationKey) []byte {
		return PendingActivationKeyPackPartial(u, nFields)
	}
}

func PendingActivationKeyPackPartial(k *PendingActivationKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 4 {
		nFields = 4
	}
	if nFields < 0 {
		nFields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
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

	for i := 0; i <= nFields; i++ {
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

/*
class ActivationKey(typing.NamedTuple):
    txo_type: int
    tx_num: int
    position: int


class ActivationValue(typing.NamedTuple):
    height: int
    claim_hash: bytes
    normalized_name: str

    def __str__(self):
        return f"{self.__class__.__name__}(height={self.height}, claim_hash={self.claim_hash.hex()}, " \
               f"normalized_name={self.normalized_name})"
*/

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

func ActivationKeyPackPartialNFields(nFields int) func(*ActivationKey) []byte {
	return func(u *ActivationKey) []byte {
		return ActivationKeyPackPartial(u, nFields)
	}
}

func ActivationKeyPackPartial(k *ActivationKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 3 {
		nFields = 3
	}
	if nFields < 0 {
		nFields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
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

	for i := 0; i <= nFields; i++ {
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

/*

class ActiveAmountKey(typing.NamedTuple):
    claim_hash: bytes
    txo_type: int
    activation_height: int
    tx_num: int
    position: int

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()}, txo_type={self.txo_type}, " \
               f"activation_height={self.activation_height}, tx_num={self.tx_num}, position={self.position})"


class ActiveAmountValue(typing.NamedTuple):
    amount: int
*/

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

func ActiveAmountKeyPackPartialNFields(nFields int) func(*ActiveAmountKey) []byte {
	return func(u *ActiveAmountKey) []byte {
		return ActiveAmountKeyPackPartial(u, nFields)
	}
}

func ActiveAmountKeyPackPartial(k *ActiveAmountKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 5 {
		nFields = 5
	}
	if nFields < 0 {
		nFields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
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

	for i := 0; i <= nFields; i++ {
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

//
// EffectiveAmountKey / EffectiveAmountValue
//
/*

class EffectiveAmountKey(typing.NamedTuple):
    normalized_name: str
    effective_amount: int
    tx_num: int
    position: int


class EffectiveAmountValue(typing.NamedTuple):
    claim_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()})"
*/

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
	binary.BigEndian.PutUint64(key[prefixLen+nameLenLen:], OnesCompTwiddle-k.EffectiveAmount)
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

func EffectiveAmountKeyPackPartialNFields(nFields int) func(*EffectiveAmountKey) []byte {
	return func(u *EffectiveAmountKey) []byte {
		return EffectiveAmountKeyPackPartial(u, nFields)
	}
}

func EffectiveAmountKeyPackPartial(k *EffectiveAmountKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	nameLen := len(k.NormalizedName)
	nameLenLen := 2 + nameLen
	if nFields > 4 {
		nFields = 4
	}
	if nFields < 0 {
		nFields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
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

	for i := 0; i <= nFields; i++ {
		switch i {
		case 0:
			copy(key, k.Prefix)
		case 1:
			binary.BigEndian.PutUint16(key[prefixLen:], uint16(nameLen))
			copy(key[prefixLen+2:], []byte(k.NormalizedName))
		case 2:
			binary.BigEndian.PutUint64(key[prefixLen+nameLenLen:], OnesCompTwiddle-k.EffectiveAmount)
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
		EffectiveAmount: OnesCompTwiddle - binary.BigEndian.Uint64(key[prefixLen+2+int(nameLen):]),
		TxNum:           binary.BigEndian.Uint32(key[prefixLen+2+int(nameLen)+8:]),
		Position:        binary.BigEndian.Uint16(key[prefixLen+2+int(nameLen)+8+4:]),
	}
}

func EffectiveAmountValueUnpack(value []byte) *EffectiveAmountValue {
	return &EffectiveAmountValue{
		ClaimHash: value[:20],
	}
}

/*

class RepostKey(typing.NamedTuple):
    claim_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()})"


class RepostValue(typing.NamedTuple):
    reposted_claim_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(reposted_claim_hash={self.reposted_claim_hash.hex()})"
*/

type RepostKey struct {
	Prefix    []byte `json:"prefix"`
	ClaimHash []byte `json:"claim_hash"`
}

type RepostValue struct {
	RepostedClaimHash []byte `json:"reposted_claim_hash"`
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

func RepostKeyPackPartialNFields(nFields int) func(*RepostKey) []byte {
	return func(u *RepostKey) []byte {
		return RepostKeyPackPartial(u, nFields)
	}
}

func RepostKeyPackPartial(k *RepostKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 1 {
		nFields = 1
	}
	if nFields < 0 {
		nFields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
		switch i {
		case 1:
			n += 20
		}
	}

	key := make([]byte, n)

	for i := 0; i <= nFields; i++ {
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

/*

class RepostedKey(typing.NamedTuple):
    reposted_claim_hash: bytes
    tx_num: int
    position: int

    def __str__(self):
        return f"{self.__class__.__name__}(reposted_claim_hash={self.reposted_claim_hash.hex()}, " \
               f"tx_num={self.tx_num}, position={self.position})"


class RepostedValue(typing.NamedTuple):
    claim_hash: bytes

    def __str__(self):
        return f"{self.__class__.__name__}(claim_hash={self.claim_hash.hex()})"
*/

type RepostedKey struct {
	Prefix            []byte `json:"prefix"`
	RepostedClaimHash []byte `json:"reposted_claim_hash"`
	TxNum             uint32 `json:"tx_num"`
	Position          uint16 `json:"position"`
}

type RepostedValue struct {
	ClaimHash []byte `json:"claim_hash"`
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

func RepostedKeyPackPartialNFields(nFields int) func(*RepostedKey) []byte {
	return func(u *RepostedKey) []byte {
		return RepostedKeyPackPartial(u, nFields)
	}
}

func RepostedKeyPackPartial(k *RepostedKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 3 {
		nFields = 3
	}
	if nFields < 0 {
		nFields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
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

	for i := 0; i <= nFields; i++ {
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

//
// TouchedOrDeletedClaimKey / TouchedOrDeletedClaimValue
//

/*
class TouchedOrDeletedClaimKey(typing.NamedTuple):
    height: int


class TouchedOrDeletedClaimValue(typing.NamedTuple):
    touched_claims: typing.Set[bytes]
    deleted_claims: typing.Set[bytes]

    def __str__(self):
        return f"{self.__class__.__name__}(" \
               f"touched_claims={','.join(map(lambda x: x.hex(), self.touched_claims))}," \
               f"deleted_claims={','.join(map(lambda x: x.hex(), self.deleted_claims))})"


*/

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

func TouchedOrDeletedClaimPackPartialNFields(nFields int) func(*TouchedOrDeletedClaimKey) []byte {
	return func(u *TouchedOrDeletedClaimKey) []byte {
		return TouchedOrDeletedClaimKeyPackPartial(u, nFields)
	}
}

func TouchedOrDeletedClaimKeyPackPartial(k *TouchedOrDeletedClaimKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 1 {
		nFields = 1
	}
	if nFields < 0 {
		nFields = 0
	}

	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
		switch i {
		case 1:
			n += 4
		}
	}

	key := make([]byte, n)

	for i := 0; i <= nFields; i++ {
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

// HashXUTXOKeyPackPartialNFields creates a pack partial key function for n fields.
func HashXUTXOKeyPackPartialNFields(nFields int) func(*HashXUTXOKey) []byte {
	return func(u *HashXUTXOKey) []byte {
		return HashXUTXOKeyPackPartial(u, nFields)
	}
}

// HashXUTXOKeyPackPartial packs a variable number of fields into a byte
// array
func HashXUTXOKeyPackPartial(k *HashXUTXOKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 3 {
		nFields = 3
	}
	if nFields < 0 {
		nFields = 0
	}

	// b'>4sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
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

	for i := 0; i <= nFields; i++ {
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

//
// UTXOKey / UTXOValue
//

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

// UTXOKeyPackPartialNFields creates a pack partial key function for n fields.
func UTXOKeyPackPartialNFields(nFields int) func(*UTXOKey) []byte {
	return func(u *UTXOKey) []byte {
		return UTXOKeyPackPartial(u, nFields)
	}
}

// UTXOKeyPackPartial packs a variable number of fields for a UTXOKey into
// a byte array.
func UTXOKeyPackPartial(k *UTXOKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix, and we never need to iterate past the number of fields.
	if nFields > 3 {
		nFields = 3
	}
	if nFields < 0 {
		nFields = 0
	}

	// b'>11sLH'
	prefixLen := 1
	var n = prefixLen
	for i := 0; i <= nFields; i++ {
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

	for i := 0; i <= nFields; i++ {
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

func UnpackGenericKey(key []byte) (byte, interface{}, error) {
	if len(key) == 0 {
		return 0x0, nil, errors.Base("key length zero")
	}
	firstByte := key[0]
	switch firstByte {
	case ClaimToSupport:
		return 0x0, nil, errors.Base("key unpack function for %v not implemented", firstByte)
	case SupportToClaim:
		return SupportToClaim, SupportToClaimKeyUnpack(key), nil

	case ClaimToTXO:
		return 0x0, nil, errors.Base("key unpack function for %v not implemented", firstByte)
	case TXOToClaim:

	case ClaimToChannel:
	case ChannelToClaim:

	case ClaimShortIdPrefix:
		return 0x0, nil, errors.Base("key unpack function for %v not implemented", firstByte)
	case EffectiveAmount:
		return EffectiveAmount, EffectiveAmountKeyUnpack(key), nil
	case ClaimExpiration:
		return ClaimExpiration, ClaimExpirationKeyUnpack(key), nil

	case ClaimTakeover:
		return ClaimTakeover, ClaimTakeoverKeyUnpack(key), nil
	case PendingActivation:
		return PendingActivation, PendingActivationKeyUnpack(key), nil
	case ActivatedClaimAndSupport:
		return ActivatedClaimAndSupport, ActivationKeyUnpack(key), nil
	case ActiveAmount:
		return ActiveAmount, ActiveAmountKeyUnpack(key), nil

	case Repost:
		return Repost, RepostKeyUnpack(key), nil
	case RepostedClaim:
		return RepostedClaim, RepostedKeyUnpack(key), nil

	case Undo:
		return 0x0, nil, errors.Base("key unpack function for %v not implemented", firstByte)
	case ClaimDiff:
		return ClaimDiff, TouchedOrDeletedClaimKeyUnpack(key), nil

	case Tx:
	case BlockHash:
	case Header:
	case TxNum:
	case TxCount:
	case TxHash:
		return 0x0, nil, errors.Base("key unpack function for %v not implemented", firstByte)
	case UTXO:
		return UTXO, UTXOKeyUnpack(key), nil
	case HashXUTXO:
		return UTXO, HashXUTXOKeyUnpack(key), nil
	case HashXHistory:
	case DBState:
	case ChannelCount:
	case SupportAmount:
	case BlockTXs:
	}
	return 0x0, nil, errors.Base("key unpack function for %v not implemented", firstByte)
}

func UnpackGenericValue(key, value []byte) (byte, interface{}, error) {
	if len(key) == 0 {
		return 0x0, nil, errors.Base("key length zero")
	}
	if len(value) == 0 {
		return 0x0, nil, errors.Base("value length zero")
	}

	firstByte := key[0]
	switch firstByte {
	case ClaimToSupport:
		return 0x0, nil, errors.Base("value unpack not implemented for key %v", key)
	case SupportToClaim:
		return SupportToClaim, SupportToClaimValueUnpack(value), nil

	case ClaimToTXO:
		return 0x0, nil, errors.Base("value unpack not implemented for key %v", key)
	case TXOToClaim:

	case ClaimToChannel:
	case ChannelToClaim:

	case ClaimShortIdPrefix:
		return 0x0, nil, errors.Base("value unpack not implemented for key %v", key)
	case EffectiveAmount:
		return EffectiveAmount, EffectiveAmountValueUnpack(value), nil
	case ClaimExpiration:
		return ClaimExpiration, ClaimExpirationValueUnpack(value), nil

	case ClaimTakeover:
		return ClaimTakeover, ClaimTakeoverValueUnpack(value), nil
	case PendingActivation:
		return PendingActivation, PendingActivationValueUnpack(value), nil
	case ActivatedClaimAndSupport:
		return ActivatedClaimAndSupport, ActivationValueUnpack(value), nil
	case ActiveAmount:
		return ActiveAmount, ActiveAmountValueUnpack(value), nil

	case Repost:
		return Repost, RepostValueUnpack(value), nil
	case RepostedClaim:
		return RepostedClaim, RepostedValueUnpack(value), nil

	case Undo:
		return 0x0, nil, errors.Base("value unpack not implemented for key %v", key)
	case ClaimDiff:
		return ClaimDiff, TouchedOrDeletedClaimValueUnpack(value), nil

	case Tx:
	case BlockHash:
	case Header:
	case TxNum:
	case TxCount:
	case TxHash:
		return 0x0, nil, errors.Base("value unpack not implemented for key %v", key)
	case UTXO:
		return UTXO, UTXOValueUnpack(value), nil
	case HashXUTXO:
		return HashXUTXO, HashXUTXOValueUnpack(value), nil
	case HashXHistory:
	case DBState:
	case ChannelCount:
	case SupportAmount:
	case BlockTXs:
	}
	return 0x0, nil, errors.Base("value unpack not implemented for key %v", key)
}
