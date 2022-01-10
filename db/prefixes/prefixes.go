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
	TxNum    int32  `json:"tx_num"`
	Position int32  `json:"position"`
}

type SupportToClaimValue struct {
	ClaimHash []byte `json:"claim_hash"`
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
	Expiration int32  `json:"expiration"`
	TxNum      int32  `json:"tx_num"`
	Position   int32  `json:"position"`
}

type ClaimExpirationValue struct {
	ClaimHash      []byte `json:"claim_hash"`
	NormalizedName string `json:"normalized_name"`
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
	Height    int32  `json:"height"`
}

func (v *ClaimTakeoverValue) String() string {
	return fmt.Sprintf(
		"%s(claim_hash=%s, height=%d)",
		reflect.TypeOf(v),
		hex.EncodeToString(v.ClaimHash),
		v.Height,
	)
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
	Height   int32  `json:"height"`
	TxoType  int32  `json:"txo_height"`
	TxNum    int32  `json:"tx_num"`
	Position int32  `json:"position"`
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
	TxType   int32  `json:"txo_type"`
	TxNum    int32  `json:"txo_num"`
	Position int32  `json:"position"`
}

type ActivationValue struct {
	Height         int32  `json:"height"`
	ClaimHash      []byte `json:"claim_hash"`
	NormalizedName string `json:"normalized_name"`
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
	TxoType          int32  `json:"txo_type"`
	ActivationHeight int32  `json:"activation_height"`
	TxNum            int32  `json:"tx_num"`
	Position         int32  `json:"position"`
}

type ActiveAmountValue struct {
	Amount int32 `json:"amount"`
}

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
	EffectiveAmount int32  `json:"effective_amount"`
	TxNum           int32  `json:"tx_num"`
	Position        int32  `json:"position"`
}

type EffectiveAmountValue struct {
	ClaimHash []byte `json:"claim_hash"`
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
	case SupportToClaim:

	case ClaimToTXO:
	case TXOToClaim:

	case ClaimToChannel:
	case ChannelToClaim:

	case ClaimShortIdPrefix:
	case EffectiveAmount:
	case ClaimExpiration:

	case ClaimTakeover:
	case PendingActivation:
	case ActivatedClaimAndSupport:
	case ActiveAmount:
		return 0x0, nil, errors.Base("key unpack function for %v not implemented", firstByte)

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
	case SupportToClaim:

	case ClaimToTXO:
	case TXOToClaim:

	case ClaimToChannel:
	case ChannelToClaim:

	case ClaimShortIdPrefix:
	case EffectiveAmount:
	case ClaimExpiration:

	case ClaimTakeover:
	case PendingActivation:
	case ActivatedClaimAndSupport:
	case ActiveAmount:
		return 0x0, nil, nil

	case Repost:
		return Repost, RepostValueUnpack(value), nil
	case RepostedClaim:
		return RepostedClaim, RepostedValueUnpack(value), nil

	case Undo:
		return 0x0, nil, nil
	case ClaimDiff:
		return ClaimDiff, TouchedOrDeletedClaimValueUnpack(value), nil

	case Tx:
	case BlockHash:
	case Header:
	case TxNum:
	case TxCount:
	case TxHash:
		return 0x0, nil, nil
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
	return 0x0, nil, nil
}
