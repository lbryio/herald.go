package db

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"log"
	"reflect"

	"github.com/lbryio/hub/db/prefixes"
	"github.com/linxGnu/grocksdb"
)

type IterOptions struct {
	FillCache bool
	Start     []byte //interface{}
	Stop      []byte //interface{}
}

type PrefixRow struct {
	//KeyStruct     interface{}
	//ValueStruct   interface{}
	Prefix        []byte
	KeyPackFunc   interface{}
	ValuePackFunc interface{}
	DB            *grocksdb.DB
}

type PrefixRowKV struct {
	Key   []byte
	Value []byte
}

type UTXOKey struct {
	Prefix []byte
	HashX  []byte
	TxNum  uint32
	Nout   uint16
}

type UTXOValue struct {
	Amount uint64
}

func NewIterateOptions() *IterOptions {
	return &IterOptions{
		FillCache: false,
		Start:     nil,
		Stop:      nil,
	}
}

func (o *IterOptions) WithFillCache(fillCache bool) *IterOptions {
	o.FillCache = fillCache
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

func (k *UTXOKey) String() string {
	return fmt.Sprintf(
		"%s(hashX=%s, tx_num=%d, nout=%d)",
		reflect.TypeOf(k),
		hex.EncodeToString(k.HashX),
		k.TxNum,
		k.Nout,
	)
}

func (pr *PrefixRow) Iter(options *IterOptions) <-chan *PrefixRowKV {
	ch := make(chan *PrefixRowKV)

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(options.FillCache)
	it := pr.DB.NewIterator(ro)

	it.Seek(pr.Prefix)
	if options.Start != nil {
		it.Seek(options.Start)
	}

	/*
	   def _check_stop_iteration(self, key: bytes):
	       if self.stop is not None and (key.startswith(self.stop) or self.stop < key[:len(self.stop)]):
	           raise StopIteration
	       elif self.start is not None and self.start > key[:len(self.start)]:
	           raise StopIteration
	       elif self.prefix is not None and not key.startswith(self.prefix):
	           raise StopIteration
	*/
	terminateFunc := func(key []byte) bool {
		if key == nil {
			return true
		}

		if options.Stop != nil &&
			(bytes.HasPrefix(key, options.Stop) || bytes.Compare(options.Stop, key[:len(options.Stop)]) < 0) {
			return false
		} else if options.Start != nil &&
			bytes.Compare(options.Start, key[:len(options.Start)]) > 0 {
			return false
		} else if pr.Prefix != nil && !bytes.HasPrefix(key, pr.Prefix) {
			return false
		}

		return true
	}

	var prevKey []byte = nil
	go func() {
		for ; terminateFunc(prevKey); it.Next() {
			key := it.Key()
			prevKey = key.Data()
			value := it.Value()

			ch <- &PrefixRowKV{
				Key:   key.Data(),
				Value: value.Data(),
			}

			key.Free()
			value.Free()
		}
		close(ch)
	}()

	return ch
}

func (k *UTXOKey) PackKey() []byte {
	prefixLen := len(prefixes.UTXO)
	// b'>11sLH'
	n := prefixLen + 11 + 4 + 2
	key := make([]byte, n)
	copy(key, k.Prefix)
	copy(key[prefixLen:], k.HashX)
	binary.BigEndian.PutUint32(key[prefixLen+11:], k.TxNum)
	binary.BigEndian.PutUint16(key[prefixLen+15:], k.Nout)

	return key
}

// UTXOKeyPackPartial packs a variable number of fields for a UTXOKey into
// a byte array.
func UTXOKeyPackPartial(k *UTXOKey, nFields int) []byte {
	// Limit nFields between 0 and number of fields, we always at least need
	// the prefix and we never need to iterate past the number of fields.
	if nFields > 3 {
		nFields = 3
	}
	if nFields < 0 {
		nFields = 0
	}

	// b'>11sLH'
	prefixLen := len(prefixes.UTXO)
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
	db, err := grocksdb.OpenDb(opts, name)
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

func OpenDB(name string) int {
	// Read db
	opts := grocksdb.NewDefaultOptions()
	db, err := grocksdb.OpenDb(opts, name)
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	if err != nil {
		log.Println(err)
	}

	log.Println(db.Name())

	it := db.NewIterator(ro)
	defer it.Close()

	var i = 0
	it.Seek([]byte("foo"))
	for ; it.Valid(); it.Next() {
		key := it.Key()
		value := it.Value()

		fmt.Printf("Key: %v Value: %v\n", key.Data(), value.Data())

		key.Free()
		value.Free()
		i++
	}
	if err := it.Err(); err != nil {
		log.Println(err)
	}

	return i
}

func OpenAndWriteDB(in string, out string) {
	// Read db
	opts := grocksdb.NewDefaultOptions()
	db, err := grocksdb.OpenDb(opts, in)
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	if err != nil {
		log.Println(err)
	}
	// Write db
	opts.SetCreateIfMissing(true)
	db2, err := grocksdb.OpenDb(opts, out)
	if err != nil {
		log.Println(err)
	}
	wo := grocksdb.NewDefaultWriteOptions()
	defer db2.Close()

	log.Println(db.Name())
	log.Println(db2.Name())

	it := db.NewIterator(ro)
	defer it.Close()

	var i = 0
	it.Seek([]byte("foo"))
	for ; it.Valid() && i < 10; it.Next() {
		key := it.Key()
		value := it.Value()
		fmt.Printf("Key: %v Value: %v\n", key.Data(), value.Data())

		if err := db2.Put(wo, key.Data(), value.Data()); err != nil {
			log.Println(err)
		}

		key.Free()
		value.Free()
		i++
	}
	if err := it.Err(); err != nil {
		log.Println(err)
	}
}
