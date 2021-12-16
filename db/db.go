package db

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"reflect"

	"github.com/lbryio/hub/db/prefixes"
	"github.com/lbryio/lbry.go/v2/extras/errors"
	"github.com/linxGnu/grocksdb"
)

type IterOptions struct {
	FillCache    bool
	Prefix       []byte
	Start        []byte //interface{}
	Stop         []byte //interface{}
	IncludeStart bool
	IncludeStop  bool
	IncludeKey   bool
	IncludeValue bool
	RawKey       bool
	RawValue     bool
}

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

func UnpackGenericKey(key []byte) (byte, interface{}, error) {
	if len(key) == 0 {
		return 0x0, nil, errors.Base("key length zero")
	}
	firstByte := key[0]
	switch firstByte {
	case prefixes.ClaimToSupport:
	case prefixes.SupportToClaim:

	case prefixes.ClaimToTXO:
	case prefixes.TXOToClaim:

	case prefixes.ClaimToChannel:
	case prefixes.ChannelToClaim:

	case prefixes.ClaimShortIdPrefix:
	case prefixes.EffectiveAmount:
	case prefixes.ClaimExpiration:

	case prefixes.ClaimTakeover:
	case prefixes.PendingActivation:
	case prefixes.ActivatedClaimAndSupport:
	case prefixes.ActiveAmount:

	case prefixes.Repost:
	case prefixes.RepostedClaim:

	case prefixes.Undo:
	case prefixes.ClaimDiff:

	case prefixes.Tx:
	case prefixes.BlockHash:
	case prefixes.Header:
	case prefixes.TxNum:
	case prefixes.TxCount:
	case prefixes.TxHash:
		return 0x0, nil, errors.Base("key unpack function for %v not implemented", firstByte)
	case prefixes.UTXO:
		return prefixes.UTXO, UTXOKeyUnpack(key), nil
	case prefixes.HashXUTXO:
	case prefixes.HashXHistory:
	case prefixes.DBState:
	case prefixes.ChannelCount:
	case prefixes.SupportAmount:
	case prefixes.BlockTXs:
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
	case prefixes.ClaimToSupport:
	case prefixes.SupportToClaim:

	case prefixes.ClaimToTXO:
	case prefixes.TXOToClaim:

	case prefixes.ClaimToChannel:
	case prefixes.ChannelToClaim:

	case prefixes.ClaimShortIdPrefix:
	case prefixes.EffectiveAmount:
	case prefixes.ClaimExpiration:

	case prefixes.ClaimTakeover:
	case prefixes.PendingActivation:
	case prefixes.ActivatedClaimAndSupport:
	case prefixes.ActiveAmount:

	case prefixes.Repost:
	case prefixes.RepostedClaim:

	case prefixes.Undo:
	case prefixes.ClaimDiff:

	case prefixes.Tx:
	case prefixes.BlockHash:
	case prefixes.Header:
	case prefixes.TxNum:
	case prefixes.TxCount:
	case prefixes.TxHash:
		return 0x0, nil, nil
	case prefixes.UTXO:
		return prefixes.UTXO, UTXOValueUnpack(value), nil
	case prefixes.HashXUTXO:
	case prefixes.HashXHistory:
	case prefixes.DBState:
	case prefixes.ChannelCount:
	case prefixes.SupportAmount:
	case prefixes.BlockTXs:
	}
	return 0x0, nil, nil
}

// NewIterateOptions creates a defualt options structure for a db iterator.
func NewIterateOptions() *IterOptions {
	return &IterOptions{
		FillCache:    false,
		Prefix:       []byte{},
		Start:        nil,
		Stop:         nil,
		IncludeStart: true,
		IncludeStop:  false,
		IncludeKey:   true,
		IncludeValue: false,
		RawKey:       false,
		RawValue:     false,
	}
}

func (o *IterOptions) WithFillCache(fillCache bool) *IterOptions {
	o.FillCache = fillCache
	return o
}

func (o *IterOptions) WithPrefix(prefix []byte) *IterOptions {
	o.Prefix = prefix
	return o
}

func (o *IterOptions) WithStart(start []byte) *IterOptions {
	o.Start = start
	return o
}

func (o *IterOptions) WithStop(stop []byte) *IterOptions {
	o.Stop = stop
	return o
}

func (o *IterOptions) WithIncludeStart(includeStart bool) *IterOptions {
	o.IncludeStart = includeStart
	return o
}

func (o *IterOptions) WithIncludeStop(includeStop bool) *IterOptions {
	o.IncludeStop = includeStop
	return o
}

func (o *IterOptions) WithIncludeKey(includeKey bool) *IterOptions {
	o.IncludeKey = includeKey
	return o
}

func (o *IterOptions) WithIncludeValue(includeValue bool) *IterOptions {
	o.IncludeValue = includeValue
	return o
}

func (o *IterOptions) WithRawKey(rawKey bool) *IterOptions {
	o.RawKey = rawKey
	return o
}

func (o *IterOptions) WithRawValue(rawValue bool) *IterOptions {
	o.RawValue = rawValue
	return o
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

func Iter(db *grocksdb.DB, opts *IterOptions) <-chan *PrefixRowKV {
	ch := make(chan *PrefixRowKV)

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(opts.FillCache)
	it := db.NewIterator(ro)

	it.Seek(opts.Prefix)
	if opts.Start != nil {
		it.Seek(opts.Start)
	}

	stopIteration := func(key []byte) bool {
		if key == nil {
			return false
		}

		if opts.Stop != nil &&
			(bytes.HasPrefix(key, opts.Stop) || bytes.Compare(opts.Stop, key[:len(opts.Stop)]) < 0) {
			return true
		} else if opts.Start != nil &&
			bytes.Compare(opts.Start, key[:len(opts.Start)]) > 0 {
			return true
		} else if opts.Prefix != nil && !bytes.HasPrefix(key, opts.Prefix) {
			return true
		}

		return false
	}

	go func() {
		defer it.Close()
		defer close(ch)

		if !opts.IncludeStart {
			it.Next()
		}
		var prevKey []byte = nil
		for ; !stopIteration(prevKey) && it.Valid(); it.Next() {
			key := it.Key()
			keyData := key.Data()
			keyLen := len(keyData)
			value := it.Value()
			valueData := value.Data()
			valueLen := len(valueData)

			var outKey interface{} = nil
			var outValue interface{} = nil
			var err error = nil

			// We need to check the current key is we're not including the stop
			// key.
			if !opts.IncludeStop && stopIteration(keyData) {
				return
			}

			// We have to copy the key no matter what because we need to check
			// it on the next iterations to see if we're going to stop.
			newKeyData := make([]byte, keyLen)
			copy(newKeyData, keyData)
			if opts.IncludeKey && !opts.RawKey {
				//unpackKeyFnValue := reflect.ValueOf(KeyUnpackFunc)
				//keyArgs := []reflect.Value{reflect.ValueOf(newKeyData)}
				//unpackKeyFnResult := unpackKeyFnValue.Call(keyArgs)
				//outKey = unpackKeyFnResult[0].Interface()
				_, outKey, err = UnpackGenericKey(newKeyData)
				if err != nil {
					log.Println(err)
				}
			} else if opts.IncludeKey {
				outKey = newKeyData
			}

			// Value could be quite large, so this setting could be important
			// for performance in some cases.
			if opts.IncludeValue {
				newValueData := make([]byte, valueLen)
				copy(newValueData, valueData)
				//unpackValueFnValue := reflect.ValueOf(ValueUnpackFunc)
				//valueArgs := []reflect.Value{reflect.ValueOf(newValueData)}
				//unpackValueFnResult := unpackValueFnValue.Call(valueArgs)
				//outValue = unpackValueFnResult[0].Interface()
				if !opts.RawValue {
					_, outValue, err = UnpackGenericValue(newKeyData, newValueData)
					if err != nil {
						log.Println(err)
					}
				} else {
					outValue = newValueData
				}
			}

			key.Free()
			value.Free()

			ch <- &PrefixRowKV{
				Key:   outKey,
				Value: outValue,
			}
			prevKey = newKeyData

		}
	}()

	return ch
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
	return &UTXOKey{
		Prefix: key[:1],
		HashX:  key[1:12],
		TxNum:  binary.BigEndian.Uint32(key[12:]),
		Nout:   binary.BigEndian.Uint16(key[16:]),
	}
}

func (k *UTXOValue) PackValue() []byte {
	value := make([]byte, 8)
	binary.BigEndian.PutUint64(value, k.Amount)

	return value
}

func UTXOValueUnpack(value []byte) *UTXOValue {
	return &UTXOValue{
		Amount: binary.BigEndian.Uint64(value),
	}
}

func GetDB(name string) (*grocksdb.DB, error) {
	opts := grocksdb.NewDefaultOptions()
	// db, err := grocksdb.OpenDb(opts, name)
	db, err := grocksdb.OpenDbAsSecondary(opts, name, "asdf")
	if err != nil {
		return nil, err
	}

	return db, nil
}

func ReadPrefixN(db *grocksdb.DB, prefix []byte, n int) []*PrefixRowKV {
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	it := db.NewIterator(ro)
	defer it.Close()

	res := make([]*PrefixRowKV, n)

	var i = 0
	it.Seek(prefix)
	for ; it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()

		res[i] = &PrefixRowKV{
			Key:   key.Data(),
			Value: value.Data(),
		}

		key.Free()
		value.Free()
		i++
		if i >= n {
			break
		}
	}

	return res
}

func OpenDB(name string, start string) int {
	// Read db
	opts := grocksdb.NewDefaultOptions()
	db, err := grocksdb.OpenDb(opts, name)
	if err != nil {
		log.Println(err)
	}
	defer db.Close()
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	log.Println(db.Name())

	it := db.NewIterator(ro)
	defer it.Close()

	var i = 0
	it.Seek([]byte(start))
	for ; it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()

		fmt.Printf("Key: %v Value: %v\n", hex.EncodeToString(key.Data()), hex.EncodeToString(value.Data()))

		key.Free()
		value.Free()
		i++
	}
	if err := it.Err(); err != nil {
		log.Println(err)
	}

	return i
}

func OpenAndWriteDB(db *grocksdb.DB, options *IterOptions, out string) {

	ch := Iter(db, options)

	var i = 0
	for kv := range ch {
		key := kv.Key.(*UTXOKey)
		value := kv.Value.(*UTXOValue)

		keyMarshal, err := json.Marshal(key)
		if err != nil {
			log.Println(err)
		}
		valMarshal, err := json.Marshal(value)
		if err != nil {
			log.Println(err)
		}

		log.Println(string(keyMarshal), string(valMarshal))

		i++
	}
}
