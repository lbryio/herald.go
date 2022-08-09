package db

// iteroptions.go contains the implementation for iterators on rocksdb used by the hub

import (
	"bytes"

	"github.com/lbryio/herald.go/db/prefixes"
	"github.com/linxGnu/grocksdb"

	log "github.com/sirupsen/logrus"
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
	CfHandle     *grocksdb.ColumnFamilyHandle
	It           *grocksdb.Iterator
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
		CfHandle:     nil,
		It:           nil,
	}
}

func (o *IterOptions) WithCfHandle(cfHandle *grocksdb.ColumnFamilyHandle) *IterOptions {
	o.CfHandle = cfHandle
	return o
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

// ReadRow reads a row from the db, returns nil when no more rows are available.
func (opts *IterOptions) ReadRow(prevKey *[]byte) *prefixes.PrefixRowKV {
	it := opts.It
	if !it.Valid() {
		log.Trace("ReadRow iterator not valid returning nil")
		return nil
	}

	key := it.Key()
	defer key.Free()
	keyData := key.Data()
	keyLen := len(keyData)

	value := it.Value()
	defer value.Free()
	valueData := value.Data()
	valueLen := len(valueData)

	var outKey interface{} = nil
	var outValue interface{} = nil
	var err error = nil

	log.Trace("keyData:", keyData)
	log.Trace("valueData:", valueData)

	// We need to check the current key if we're not including the stop
	// key.
	if !opts.IncludeStop && opts.StopIteration(keyData) {
		log.Trace("ReadRow returning nil")
		return nil
	}

	// We have to copy the key no matter what because we need to check
	// it on the next iterations to see if we're going to stop.
	newKeyData := make([]byte, keyLen)
	copy(newKeyData, keyData)
	if opts.IncludeKey && !opts.RawKey {
		outKey, err = prefixes.UnpackGenericKey(newKeyData)
		if err != nil {
			log.Error(err)
		}
	} else if opts.IncludeKey {
		outKey = newKeyData
	}

	// Value could be quite large, so this setting could be important
	// for performance in some cases.
	if opts.IncludeValue {
		newValueData := make([]byte, valueLen)
		copy(newValueData, valueData)
		if !opts.RawValue {
			outValue, err = prefixes.UnpackGenericValue(newKeyData, newValueData)
			if err != nil {
				log.Error(err)
			}
		} else {
			outValue = newValueData
		}
	}

	kv := &prefixes.PrefixRowKV{
		Key:   outKey,
		Value: outValue,
	}
	*prevKey = newKeyData

	return kv
}

// StopIteration returns true if we've hit the criteria to end iteration on this key
func (o *IterOptions) StopIteration(key []byte) bool {
	if key == nil {
		return false
	}

	maxLenStop := intMin(len(key), len(o.Stop))
	maxLenStart := intMin(len(key), len(o.Start))
	if o.Stop != nil &&
		(bytes.HasPrefix(key, o.Stop) || bytes.Compare(o.Stop, key[:maxLenStop]) < 0) {
		return true
	} else if o.Start != nil &&
		bytes.Compare(o.Start, key[:maxLenStart]) > 0 {
		return true
	} else if o.Prefix != nil && !bytes.HasPrefix(key, o.Prefix) {
		return true
	}

	return false
}
