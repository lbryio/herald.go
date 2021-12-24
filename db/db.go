package db

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"os"

	"github.com/lbryio/hub/db/prefixes"
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

func Iter(db *grocksdb.DB, opts *IterOptions) <-chan *prefixes.PrefixRowKV {
	ch := make(chan *prefixes.PrefixRowKV)

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
				_, outKey, err = prefixes.UnpackGenericKey(newKeyData)
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
					_, outValue, err = prefixes.UnpackGenericValue(newKeyData, newValueData)
					if err != nil {
						log.Println(err)
					}
				} else {
					outValue = newValueData
				}
			}

			key.Free()
			value.Free()

			ch <- &prefixes.PrefixRowKV{
				Key:   outKey,
				Value: outValue,
			}
			prevKey = newKeyData

		}
	}()

	return ch
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

func ReadPrefixN(db *grocksdb.DB, prefix []byte, n int) []*prefixes.PrefixRowKV {
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

	it := db.NewIterator(ro)
	defer it.Close()

	res := make([]*prefixes.PrefixRowKV, n)

	var i = 0
	it.Seek(prefix)
	for ; it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()

		res[i] = &prefixes.PrefixRowKV{
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
		key := kv.Key.(*prefixes.UTXOKey)
		value := kv.Value.(*prefixes.UTXOValue)

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

func ReadWriteRawN(db *grocksdb.DB, options *IterOptions, out string, n int) {

	options.RawKey = true
	options.RawValue = true
	ch := Iter(db, options)

	file, err := os.Create(out)
	if err != nil {
		log.Println(err)
		return
	}
	defer file.Close()

	var i = 0
	for kv := range ch {
		log.Println(i)
		if i >= n {
			return
		}
		key := kv.Key.([]byte)
		value := kv.Value.([]byte)
		keyHex := hex.EncodeToString(key)
		valueHex := hex.EncodeToString(value)
		log.Println(keyHex)
		log.Println(valueHex)
		file.WriteString(keyHex)
		file.WriteString(",")
		file.WriteString(valueHex)
		file.WriteString("\n")

		i++
	}
}

func GenerateTestData() {
	dbVal, err := GetDB("/mnt/d/data/wallet/lbry-rocksdb/")
	if err != nil {
		log.Fatalln(err)
	}

	// options := &IterOptions{
	// 	FillCache:    false,
	// 	Prefix:       []byte{prefixes.HashXUTXO},
	// 	Start:        nil,
	// 	Stop:         nil,
	// 	IncludeStart: true,
	// 	IncludeStop:  false,
	// 	IncludeKey:   true,
	// 	IncludeValue: true,
	// 	RawKey:       true,
	// 	RawValue:     true,
	// }
	options := NewIterateOptions()
	options.WithRawKey(true).WithRawValue(true)
	options.WithPrefix([]byte{prefixes.HashXUTXO})

	ReadWriteRawN(dbVal, options, "./resources/hashx_utxo.csv", 10)
}
