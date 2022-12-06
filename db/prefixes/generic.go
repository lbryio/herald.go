package prefixes

import (
	"encoding/binary"
	"fmt"
	"reflect"
	"strings"

	"github.com/go-restruct/restruct"
	"github.com/lbryio/herald.go/internal"
	"github.com/lbryio/lbcd/chaincfg/chainhash"
)

func init() {
	restruct.EnableExprBeta()
}

// Type OnesComplementEffectiveAmount (uint64) has to be encoded specially
// to get the desired sort ordering.
// Implement the Sizer, Packer, Unpacker interface to handle it manually.

func (amt *OnesComplementEffectiveAmount) SizeOf() int {
	return 8
}

func (amt *OnesComplementEffectiveAmount) Pack(buf []byte, order binary.ByteOrder) ([]byte, error) {
	binary.BigEndian.PutUint64(buf, OnesCompTwiddle64-uint64(*amt))
	return buf[8:], nil
}

func (amt *OnesComplementEffectiveAmount) Unpack(buf []byte, order binary.ByteOrder) ([]byte, error) {
	*amt = OnesComplementEffectiveAmount(OnesCompTwiddle64 - binary.BigEndian.Uint64(buf))
	return buf[8:], nil
}

// Struct BlockTxsValue has a field TxHashes of type []*chainhash.Hash.
// I haven't been able to figure out the right annotations to make
// restruct.Pack,Unpack work automagically.
// Implement the Sizer, Packer, Unpacker interface to handle it manually.

func (kv *BlockTxsValue) SizeOf() int {
	return 32 * len(kv.TxHashes)
}

func (kv *BlockTxsValue) Pack(buf []byte, order binary.ByteOrder) ([]byte, error) {
	offset := 0
	for _, h := range kv.TxHashes {
		offset += copy(buf[offset:], h[:])
	}
	return buf[offset:], nil
}

func (kv *BlockTxsValue) Unpack(buf []byte, order binary.ByteOrder) ([]byte, error) {
	offset := 0
	kv.TxHashes = make([]*chainhash.Hash, len(buf)/32)
	for i := range kv.TxHashes {
		kv.TxHashes[i] = (*chainhash.Hash)(buf[offset:32])
		offset += 32
	}
	return buf[offset:], nil
}

// Struct BigEndianChainHash is a chainhash.Hash stored in external
// byte-order (opposite of other 32 byte chainhash.Hash values). In order
// to reuse chainhash.Hash we need to correct the byte-order.
// Currently this type is used for field Genesis of DBStateValue.

func (kv *BigEndianChainHash) SizeOf() int {
	return chainhash.HashSize
}

func (kv *BigEndianChainHash) Pack(buf []byte, order binary.ByteOrder) ([]byte, error) {
	offset := 0
	hash := kv.CloneBytes()
	// HACK: Instances of chainhash.Hash use the internal byte-order.
	// Python scribe writes bytes of genesis hash in external byte-order.
	internal.ReverseBytesInPlace(hash)
	offset += copy(buf[offset:chainhash.HashSize], hash[:])
	return buf[offset:], nil
}

func (kv *BigEndianChainHash) Unpack(buf []byte, order binary.ByteOrder) ([]byte, error) {
	offset := 0
	offset += copy(kv.Hash[:], buf[offset:32])
	// HACK: Instances of chainhash.Hash use the internal byte-order.
	// Python scribe writes bytes of genesis hash in external byte-order.
	internal.ReverseBytesInPlace(kv.Hash[:])
	return buf[offset:], nil
}

func genericNew(prefix []byte, key bool) (interface{}, error) {
	t, ok := prefixRegistry[prefix[0]]
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

func GetSerializationAPI(prefix []byte) *SerializationAPI {
	t, ok := prefixRegistry[prefix[0]]
	if !ok {
		panic(fmt.Sprintf("not handled: prefix=%v", prefix))
	}
	if t.API != nil {
		return t.API
	}
	return ProductionAPI
}

type SerializationAPI struct {
	PackKey        func(key BaseKey) ([]byte, error)
	PackPartialKey func(key BaseKey, fields int) ([]byte, error)
	PackValue      func(value BaseValue) ([]byte, error)
	UnpackKey      func(key []byte) (BaseKey, error)
	UnpackValue    func(prefix []byte, value []byte) (BaseValue, error)
}

var ProductionAPI = &SerializationAPI{
	PackKey:        PackGenericKey,
	PackPartialKey: PackPartialGenericKey,
	PackValue:      PackGenericValue,
	UnpackKey:      UnpackGenericKey,
	UnpackValue:    UnpackGenericValue,
}

var RegressionAPI_1 = &SerializationAPI{
	PackKey: func(key BaseKey) ([]byte, error) {
		return GenericPack(key, -1)
	},
	PackPartialKey: func(key BaseKey, fields int) ([]byte, error) {
		return GenericPack(key, fields)
	},
	PackValue: func(value BaseValue) ([]byte, error) {
		return GenericPack(value, -1)
	},
	UnpackKey:   UnpackGenericKey,
	UnpackValue: UnpackGenericValue,
}

var RegressionAPI_2 = &SerializationAPI{
	PackKey:        PackGenericKey,
	PackPartialKey: PackPartialGenericKey,
	PackValue:      PackGenericValue,
	UnpackKey: func(key []byte) (BaseKey, error) {
		k, err := GenericUnpack(key, true, key)
		return k.(BaseKey), err
	},
	UnpackValue: func(prefix []byte, value []byte) (BaseValue, error) {
		k, err := GenericUnpack(prefix, false, value)
		return k.(BaseValue), err
	},
}

var RegressionAPI_3 = &SerializationAPI{
	PackKey: func(key BaseKey) ([]byte, error) {
		return GenericPack(key, -1)
	},
	PackPartialKey: func(key BaseKey, fields int) ([]byte, error) {
		return GenericPack(key, fields)
	},
	PackValue: func(value BaseValue) ([]byte, error) {
		return GenericPack(value, -1)
	},
	UnpackKey: func(key []byte) (BaseKey, error) {
		k, err := GenericUnpack(key, true, key)
		return k.(BaseKey), err
	},
	UnpackValue: func(prefix []byte, value []byte) (BaseValue, error) {
		k, err := GenericUnpack(prefix, false, value)
		return k.(BaseValue), err
	},
}
