package prefixes

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-restruct/restruct"
)

func init() {
	restruct.EnableExprBeta()
}

type tableMeta struct {
	newKey         func() interface{}
	newValue       func() interface{}
	newKeyUnpack   func([]byte) interface{}
	newValueUnpack func([]byte) interface{}
}

var tableRegistry = map[byte]tableMeta{
	ClaimToSupport: {
		newKey: func() interface{} {
			return &ClaimToSupportKey{Prefix: []byte{ClaimToSupport}}
		},
		newValue: func() interface{} {
			return &ClaimToSupportValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return ClaimToSupportKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return ClaimToSupportValueUnpack(buf)
		},
	},
	SupportToClaim: {
		newKey: func() interface{} {
			return &SupportToClaimKey{Prefix: []byte{SupportToClaim}}
		},
		newValue: func() interface{} {
			return &SupportToClaimValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return SupportToClaimKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return SupportToClaimValueUnpack(buf)
		},
	},

	ClaimToTXO: {
		newKey: func() interface{} {
			return &ClaimToTXOKey{Prefix: []byte{ClaimToTXO}}
		},
		newValue: func() interface{} {
			return &ClaimToTXOValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return ClaimToTXOKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return ClaimToTXOValueUnpack(buf)
		},
	},
	TXOToClaim: {
		newKey: func() interface{} {
			return &TXOToClaimKey{Prefix: []byte{TXOToClaim}}
		},
		newValue: func() interface{} {
			return &TXOToClaimValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return TXOToClaimKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return TXOToClaimValueUnpack(buf)
		},
	},

	ClaimToChannel: {
		newKey: func() interface{} {
			return &ClaimToChannelKey{Prefix: []byte{ClaimToChannel}}
		},
		newValue: func() interface{} {
			return &ClaimToChannelValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return ClaimToChannelKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return ClaimToChannelValueUnpack(buf)
		},
	},
	ChannelToClaim: {
		newKey: func() interface{} {
			return &ChannelToClaimKey{Prefix: []byte{ChannelToClaim}}
		},
		newValue: func() interface{} {
			return &ChannelToClaimValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return ChannelToClaimKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return ChannelToClaimValueUnpack(buf)
		},
	},

	ClaimShortIdPrefix: {
		newKey: func() interface{} {
			return &ClaimShortIDKey{Prefix: []byte{ClaimShortIdPrefix}}
		},
		newValue: func() interface{} {
			return &ClaimShortIDValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return ClaimShortIDKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return ClaimShortIDValueUnpack(buf)
		},
	},
	EffectiveAmount: {
		newKey: func() interface{} {
			return &EffectiveAmountKey{Prefix: []byte{EffectiveAmount}}
		},
		newValue: func() interface{} {
			return &EffectiveAmountValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return EffectiveAmountKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return EffectiveAmountValueUnpack(buf)
		},
	},
	ClaimExpiration: {
		newKey: func() interface{} {
			return &ClaimExpirationKey{Prefix: []byte{ClaimExpiration}}
		},
		newValue: func() interface{} {
			return &ClaimExpirationValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return ClaimExpirationKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return ClaimExpirationValueUnpack(buf)
		},
	},

	ClaimTakeover: {
		newKey: func() interface{} {
			return &ClaimTakeoverKey{Prefix: []byte{ClaimTakeover}}
		},
		newValue: func() interface{} {
			return &ClaimTakeoverValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return ClaimTakeoverKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return ClaimTakeoverValueUnpack(buf)
		},
	},
	PendingActivation: {
		newKey: func() interface{} {
			return &PendingActivationKey{Prefix: []byte{PendingActivation}}
		},
		newValue: func() interface{} {
			return &PendingActivationValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return PendingActivationKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return PendingActivationValueUnpack(buf)
		},
	},
	ActivatedClaimAndSupport: {
		newKey: func() interface{} {
			return &ActivationKey{Prefix: []byte{ActivatedClaimAndSupport}}
		},
		newValue: func() interface{} {
			return &ActivationValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return ActivationKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return ActivationValueUnpack(buf)
		},
	},
	ActiveAmount: {
		newKey: func() interface{} {
			return &ActiveAmountKey{Prefix: []byte{ActiveAmount}}
		},
		newValue: func() interface{} {
			return &ActiveAmountValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return ActiveAmountKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return ActiveAmountValueUnpack(buf)
		},
	},

	Repost: {
		newKey: func() interface{} {
			return &RepostKey{Prefix: []byte{Repost}}
		},
		newValue: func() interface{} {
			return &RepostValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return RepostKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return RepostValueUnpack(buf)
		},
	},
	RepostedClaim: {
		newKey: func() interface{} {
			return &RepostedKey{Prefix: []byte{RepostedClaim}}
		},
		newValue: func() interface{} {
			return &RepostedValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return RepostedKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return RepostedValueUnpack(buf)
		},
	},

	Undo: {
		newKey: func() interface{} {
			return &UndoKey{Prefix: []byte{Undo}}
		},
		newValue: func() interface{} {
			return &UndoValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return UndoKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return UndoValueUnpack(buf)
		},
	},
	ClaimDiff: {
		newKey: func() interface{} {
			return &TouchedOrDeletedClaimKey{Prefix: []byte{ClaimDiff}}
		},
		newValue: func() interface{} {
			return &TouchedOrDeletedClaimValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return TouchedOrDeletedClaimKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return TouchedOrDeletedClaimValueUnpack(buf)
		},
	},

	Tx: {
		newKey: func() interface{} {
			return &TxKey{Prefix: []byte{Tx}}
		},
		newValue: func() interface{} {
			return &TxValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return TxKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return TxValueUnpack(buf)
		},
	},
	BlockHash: {
		newKey: func() interface{} {
			return &BlockHashKey{Prefix: []byte{BlockHash}}
		},
		newValue: func() interface{} {
			return &BlockHashValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return BlockHashKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return BlockHashValueUnpack(buf)
		},
	},
	Header: {
		newKey: func() interface{} {
			return &BlockHeaderKey{Prefix: []byte{Header}}
		},
		newValue: func() interface{} {
			return &BlockHeaderValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return BlockHeaderKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return BlockHeaderValueUnpack(buf)
		},
	},
	TxNum: {
		newKey: func() interface{} {
			return &TxNumKey{Prefix: []byte{TxNum}}
		},
		newValue: func() interface{} {
			return &TxNumValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return TxNumKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return TxNumValueUnpack(buf)
		},
	},
	TxCount: {
		newKey: func() interface{} {
			return &TxCountKey{Prefix: []byte{TxCount}}
		},
		newValue: func() interface{} {
			return &TxCountValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return TxCountKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return TxCountValueUnpack(buf)
		},
	},
	TxHash: {
		newKey: func() interface{} {
			return &TxHashKey{Prefix: []byte{TxHash}}
		},
		newValue: func() interface{} {
			return &TxHashValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return TxHashKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return TxHashValueUnpack(buf)
		},
	},
	UTXO: {
		newKey: func() interface{} {
			return &UTXOKey{Prefix: []byte{UTXO}}
		},
		newValue: func() interface{} {
			return &UTXOValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return UTXOKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return UTXOValueUnpack(buf)
		},
	},
	HashXUTXO: {
		newKey: func() interface{} {
			return &HashXUTXOKey{Prefix: []byte{HashXUTXO}}
		},
		newValue: func() interface{} {
			return &HashXUTXOValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return HashXUTXOKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return HashXUTXOValueUnpack(buf)
		},
	},
	HashXHistory: {
		newKey: func() interface{} {
			return &HashXHistoryKey{Prefix: []byte{HashXHistory}}
		},
		newValue: func() interface{} {
			return &HashXHistoryValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return HashXHistoryKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return HashXHistoryValueUnpack(buf)
		},
	},
	DBState: {
		newKey: func() interface{} {
			return &DBStateKey{Prefix: []byte{DBState}}
		},
		newValue: func() interface{} {
			return &DBStateValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return DBStateKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return DBStateValueUnpack(buf)
		},
	},
	ChannelCount: {
		newKey: func() interface{} {
			return &ChannelCountKey{Prefix: []byte{ChannelCount}}
		},
		newValue: func() interface{} {
			return &ChannelCountValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return ChannelCountKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return ChannelCountValueUnpack(buf)
		},
	},
	SupportAmount: {
		newKey: func() interface{} {
			return &SupportAmountKey{Prefix: []byte{SupportAmount}}
		},
		newValue: func() interface{} {
			return &SupportAmountValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return SupportAmountKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return SupportAmountValueUnpack(buf)
		},
	},
	BlockTXs: {
		newKey: func() interface{} {
			return &BlockTxsKey{Prefix: []byte{BlockTXs}}
		},
		newValue: func() interface{} {
			return &BlockTxsValue{}
		},
		newKeyUnpack: func(buf []byte) interface{} {
			return BlockTxsKeyUnpack(buf)
		},
		newValueUnpack: func(buf []byte) interface{} {
			return BlockTxsValueUnpack(buf)
		},
	},

	TrendingNotifications: {
		newKey: func() interface{} {
			return &TrendingNotificationKey{Prefix: []byte{TrendingNotifications}}
		},
		newValue: func() interface{} {
			return &TrendingNotificationValue{}
		},
	},
	MempoolTx: {
		newKey: func() interface{} {
			return &MempoolTxKey{Prefix: []byte{MempoolTx}}
		},
		newValue: func() interface{} {
			return &MempoolTxValue{}
		},
	},
	TouchedHashX: {
		newKey: func() interface{} {
			return &TouchedHashXKey{Prefix: []byte{TouchedHashX}}
		},
		newValue: func() interface{} {
			return &TouchedHashXValue{}
		},
	},
	HashXStatus: {
		newKey: func() interface{} {
			return &HashXStatusKey{Prefix: []byte{HashXStatus}}
		},
		newValue: func() interface{} {
			return &HashXStatusValue{}
		},
	},
	HashXMempoolStatus: {
		newKey: func() interface{} {
			return &HashXMempoolStatusKey{Prefix: []byte{HashXMempoolStatus}}
		},
		newValue: func() interface{} {
			return &HashXMempoolStatusValue{}
		},
	},
}

func genericNew(prefix []byte, key bool) (interface{}, error) {
	t, ok := tableRegistry[prefix[0]]
	if !ok {
		panic(fmt.Sprintf("not handled: prefix=%v", prefix))
	}
	if key {
		return t.newKey(), nil
	}
	return t.newValue(), nil
}

func GenericPack(kv interface{}, fields int) ([]byte, error) {
	// Locate the byte offset of the first excluded field.
	offset := 0
	if fields > 0 {
		v := reflect.ValueOf(kv)
		t := v.Type()
		// Handle indirection to reach kind=Struct.
		switch t.Kind() {
		case reflect.Interface, reflect.Pointer:
			v = v.Elem()
			t = v.Type()
		default:
			panic(fmt.Sprintf("not handled: %v", t.Kind()))
		}
		count := 0
		for _, sf := range reflect.VisibleFields(t) {
			if !sf.IsExported() {
				continue
			}
			if sf.Anonymous && strings.HasPrefix(sf.Name, "LengthEncoded") {
				fields += 1 // Skip it but process NameLen and Name instead.
				continue
			}
			if count > fields {
				break
			}
			sz, err := restruct.SizeOf(v.FieldByIndex(sf.Index).Interface())
			if err != nil {
				panic(fmt.Sprintf("not handled: %v: %v", sf.Name, sf.Type.Kind()))
			}
			offset += sz
			count += 1
		}
	}
	// Pack the struct. No ability to partially pack.
	buf, err := restruct.Pack(binary.BigEndian, kv)
	if err != nil {
		panic(fmt.Sprintf("not handled: %v", err))
	}
	// Return a prefix if some fields were excluded.
	if fields > 0 {
		return buf[:offset], nil
	}
	return buf, nil
}

func GenericUnpack(pfx []byte, key bool, buf []byte) (interface{}, error) {
	kv, _ := genericNew(pfx, key)
	err := restruct.Unpack(buf, binary.BigEndian, kv)
	if err != nil {
		panic(fmt.Sprintf("not handled: %v", err))
	}
	return kv, nil
}
