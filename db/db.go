package db

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"time"

	"github.com/lbryio/hub/db/prefixes"
	"github.com/lbryio/lbry.go/v2/extras/util"
	"github.com/linxGnu/grocksdb"
)

//
// Constants
//

const (
	NOriginalClaimExpirationTime       = 262974
	NExtendedClaimExpirationTime       = 2102400
	NExtendedClaimExpirationForkHeight = 400155
	NNormalizedNameForkHeight          = 539940 // targeting 21 March 2019
	NMinTakeoverWorkaroundHeight       = 496850
	NMaxTakeoverWorkaroundHeight       = 658300 // targeting 30 Oct 2019
	NWitnessForkHeight                 = 680770 // targeting 11 Dec 2019
	NAllClaimsInMerkleForkHeight       = 658310 // targeting 30 Oct 2019
	ProportionalDelayFactor            = 32
	MaxTakeoverDelay                   = 4032
)

//
// Types and constructors, getters, setters, etc.
//

type ReadOnlyDBColumnFamily struct {
	DB               *grocksdb.DB
	Handles          map[string]*grocksdb.ColumnFamilyHandle
	Opts             *grocksdb.ReadOptions
	TxCounts         []uint32
	Height           uint32
	BlockedStreams   map[string][]byte
	BlockedChannels  map[string][]byte
	FilteredStreams  map[string][]byte
	FilteredChannels map[string][]byte
}

type ResolveResult struct {
	Name               string
	NormalizedName     string
	ClaimHash          []byte
	TxNum              uint32
	Position           uint16
	TxHash             []byte
	Height             uint32
	Amount             uint64
	ShortUrl           string
	IsControlling      bool
	CanonicalUrl       string
	CreationHeight     uint32
	ActivationHeight   uint32
	ExpirationHeight   uint32
	EffectiveAmount    uint64
	SupportAmount      uint64
	Reposted           int
	LastTakeoverHeight uint32
	ClaimsInChannel    uint32
	ChannelHash        []byte
	RepostedClaimHash  []byte
	SignatureValid     bool
}

type ResolveError struct {
	Error error
}

type OptionalResolveResultOrError interface {
	GetResult() *ResolveResult
	GetError() *ResolveError
	String() string
}

type optionalResolveResultOrError struct {
	res *ResolveResult
	err *ResolveError
}

func (x *optionalResolveResultOrError) GetResult() *ResolveResult {
	return x.res
}

func (x *optionalResolveResultOrError) GetError() *ResolveError {
	return x.err
}

func (x *ResolveResult) String() string {
	// return fmt.Sprintf("ResolveResult{Name: %s, NormalizedName: %s, ClaimHash: %s, TxNum: %d, Position: %d, TxHash: %s, Height: %d, Amount: %d, ShortUrl: %s, IsControlling: %v, CanonicalUrl: %s, CreationHeight: %d, ActivationHeight: %d, ExpirationHeight: %d, EffectiveAmount: %d, SupportAmount: %d, Reposted: %d, LastTakeoverHeight: %d, ClaimsInChannel: %d, ChannelHash: %s, RepostedClaimHash: %s, SignatureValid: %v}",
	// 	x.Name, x.NormalizedName, hex.EncodeToString(x.ClaimHash), x.TxNum, x.Position, hex.EncodeToString(x.TxHash), x.Height, x.Amount, x.ShortUrl, x.IsControlling, x.CanonicalUrl, x.CreationHeight, x.ActivationHeight, x.ExpirationHeight, x.EffectiveAmount, x.SupportAmount, x.Reposted, x.LastTakeoverHeight, x.ClaimsInChannel, hex.EncodeToString(x.ChannelHash), hex.EncodeToString(x.RepostedClaimHash), x.SignatureValid)
	return fmt.Sprintf("%#v", x)
}

func (x *ResolveError) String() string {
	return fmt.Sprintf("ResolveError{Error: %#v}", x.Error)
}

func (x *optionalResolveResultOrError) String() string {
	if x.res != nil {
		return x.res.String()
	}
	if x.err != nil {
		return x.err.String()
	}
	return fmt.Sprintf("%#v", x)
}

type ExpandedResolveResult struct {
	Stream          OptionalResolveResultOrError
	Channel         OptionalResolveResultOrError
	Repost          OptionalResolveResultOrError
	RepostedChannel OptionalResolveResultOrError
}

func (x *ExpandedResolveResult) String() string {
	return fmt.Sprintf("ExpandedResolveResult{Stream: %s, Channel: %s, Repost: %s, RepostedChannel: %s}", x.Stream, x.Channel, x.Repost, x.RepostedChannel)
}

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

type PathSegment struct {
	name        string
	claimId     string
	amountOrder int
}

func (ps *PathSegment) Normalized() string {
	return util.NormalizeName(ps.name)
}

func (ps *PathSegment) IsShortId() bool {
	return ps.claimId != "" && len(ps.claimId) < 40
}

func (ps *PathSegment) IsFullId() bool {
	return len(ps.claimId) == 40
}

func (ps *PathSegment) String() string {
	if ps.claimId != "" {
		return fmt.Sprintf("%s:%s", ps.name, ps.claimId)
	} else if ps.amountOrder != 0 {
		return fmt.Sprintf("%s:%d", ps.name, ps.amountOrder)
	}
	return ps.name
}

// BisectRight returns the index of the first element in the list that is greater than or equal to the value.
// https://stackoverflow.com/questions/29959506/is-there-a-go-analog-of-pythons-bisect-module
func BisectRight(arr []uint32, val uint32) uint32 {
	i := sort.Search(len(arr), func(i int) bool { return arr[i] >= val })
	return uint32(i)
}

//
// Iterators / db construction functions
//

func (o *IterOptions) StopIteration(key []byte) bool {
	if key == nil {
		return false
	}

	// TODO: Look at not doing floating point conversions for this
	maxLenStop := int(math.Min(float64(len(key)), float64(len(o.Stop))))
	maxLenStart := int(math.Min(float64(len(key)), float64(len(o.Start))))
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

func (opts *IterOptions) ReadRow(ch chan *prefixes.PrefixRowKV, prevKey *[]byte) bool {
	it := opts.It
	key := it.Key()
	keyData := key.Data()
	keyLen := len(keyData)
	value := it.Value()
	valueData := value.Data()
	valueLen := len(valueData)

	var outKey interface{} = nil
	var outValue interface{} = nil
	var err error = nil

	// log.Println("keyData:", keyData)
	// log.Println("valueData:", valueData)

	// We need to check the current key if we're not including the stop
	// key.
	if !opts.IncludeStop && opts.StopIteration(keyData) {
		log.Println("returning false")
		return false
	}

	// We have to copy the key no matter what because we need to check
	// it on the next iterations to see if we're going to stop.
	newKeyData := make([]byte, keyLen)
	copy(newKeyData, keyData)
	if opts.IncludeKey && !opts.RawKey {
		outKey, err = prefixes.UnpackGenericKey(newKeyData)
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
		if !opts.RawValue {
			outValue, err = prefixes.UnpackGenericValue(newKeyData, newValueData)
			if err != nil {
				log.Println(err)
			}
		} else {
			outValue = newValueData
		}
	}

	key.Free()
	value.Free()

	// log.Println("sending to channel")
	ch <- &prefixes.PrefixRowKV{
		Key:   outKey,
		Value: outValue,
	}
	*prevKey = newKeyData
	// log.Println("*prevKey:", *prevKey)

	return true
}

func IterCF(db *grocksdb.DB, opts *IterOptions) <-chan *prefixes.PrefixRowKV {
	ch := make(chan *prefixes.PrefixRowKV)

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(opts.FillCache)
	it := db.NewIteratorCF(ro, opts.CfHandle)
	// it := db.NewIterator(ro)
	opts.It = it

	it.Seek(opts.Prefix)
	if opts.Start != nil {
		log.Println("Seeking to start")
		it.Seek(opts.Start)
	}

	go func() {
		defer it.Close()
		defer close(ch)

		var prevKey []byte = nil
		if !opts.IncludeStart {
			log.Println("Not including start")
			it.Next()
		}
		if !it.Valid() && opts.IncludeStop {
			log.Println("Not valid, but including stop")
			opts.ReadRow(ch, &prevKey)
		}
		var continueIter bool = true
		for ; continueIter && !opts.StopIteration(prevKey) && it.Valid(); it.Next() {
			//log.Println("Main loop")
			continueIter = opts.ReadRow(ch, &prevKey)
		}
	}()

	return ch
}

func Iter(db *grocksdb.DB, opts *IterOptions) <-chan *prefixes.PrefixRowKV {
	ch := make(chan *prefixes.PrefixRowKV)

	ro := grocksdb.NewDefaultReadOptions()
	/*
		FIXME:
		ro.SetIterateLowerBound()
		ro.SetIterateUpperBound()
		ro.PrefixSameAsStart() -> false
		ro.AutoPrefixMode() -> on
	*/
	ro.SetFillCache(opts.FillCache)
	it := db.NewIterator(ro)
	opts.It = it

	it.Seek(opts.Prefix)
	if opts.Start != nil {
		it.Seek(opts.Start)
	}

	go func() {
		defer it.Close()
		defer close(ch)

		var prevKey []byte = nil
		if !opts.IncludeStart {
			it.Next()
		}
		if !it.Valid() && opts.IncludeStop {
			opts.ReadRow(ch, &prevKey)
		}
		for ; !opts.StopIteration(prevKey) && it.Valid(); it.Next() {
			opts.ReadRow(ch, &prevKey)
		}
	}()

	return ch
}

//
// GetDB functions that open and return a db
//

func GetWriteDBCF(name string) (*grocksdb.DB, []*grocksdb.ColumnFamilyHandle, error) {
	opts := grocksdb.NewDefaultOptions()
	cfOpt := grocksdb.NewDefaultOptions()
	cfNames, err := grocksdb.ListColumnFamilies(opts, name)
	if err != nil {
		return nil, nil, err
	}
	cfOpts := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts[i] = cfOpt
	}
	db, handles, err := grocksdb.OpenDbColumnFamilies(opts, name, cfNames, cfOpts)
	if err != nil {
		return nil, nil, err
	}

	for i, handle := range handles {
		log.Printf("%d: %s, %+v\n", i, cfNames[i], handle)
	}

	return db, handles, nil
}

func GetDBColumnFamlies(name string, cfNames []string) (*ReadOnlyDBColumnFamily, error) {
	opts := grocksdb.NewDefaultOptions()
	roOpts := grocksdb.NewDefaultReadOptions()
	cfOpt := grocksdb.NewDefaultOptions()

	//cfNames := []string{"default", cf}
	cfOpts := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts[i] = cfOpt
	}

	db, handles, err := grocksdb.OpenDbAsSecondaryColumnFamilies(opts, name, "asdf", cfNames, cfOpts)
	// db, handles, err := grocksdb.OpenDbColumnFamilies(opts, name, cfNames, cfOpts)

	if err != nil {
		return nil, err
	}

	var handlesMap = make(map[string]*grocksdb.ColumnFamilyHandle)
	for i, handle := range handles {
		log.Printf("%d: %+v\n", i, handle)
		handlesMap[cfNames[i]] = handle
	}

	myDB := &ReadOnlyDBColumnFamily{
		DB:               db,
		Handles:          handlesMap,
		Opts:             roOpts,
		BlockedStreams:   make(map[string][]byte),
		BlockedChannels:  make(map[string][]byte),
		FilteredStreams:  make(map[string][]byte),
		FilteredChannels: make(map[string][]byte),
		TxCounts:         nil,
		Height:           0,
	}

	err = InitTxCounts(myDB)
	if err != nil {
		return nil, err
	}

	return myDB, nil
}

// DetectChanges keep the rocksdb db in sync
func DetectChanges(db *ReadOnlyDBColumnFamily) error {
	err := db.DB.TryCatchUpWithPrimary()
	if err != nil {
		log.Printf("error trying to catch up with primary: %#v", err)
		return err
	}

	return nil
}

// InitTxCounts initializes the txCounts map
func InitTxCounts(db *ReadOnlyDBColumnFamily) error {
	start := time.Now()
	handle, ok := db.Handles[string([]byte{prefixes.TxCount})]
	if !ok {
		return fmt.Errorf("TxCount prefix not found")
	}

	//TODO: figure out a reasonable default and make it a constant
	txCounts := make([]uint32, 0, 1200000)

	options := NewIterateOptions().WithPrefix([]byte{prefixes.TxCount}).WithCfHandle(handle)
	options = options.WithIncludeKey(false).WithIncludeValue(true)

	ch := IterCF(db.DB, options)

	for txCount := range ch {
		txCounts = append(txCounts, txCount.Value.(*prefixes.TxCountValue).TxCount)
	}

	duration := time.Since(start)
	log.Println("len(txCounts), cap(txCounts):", len(txCounts), cap(txCounts))
	log.Println("Time to get txCounts:", duration)

	db.TxCounts = txCounts
	db.Height = uint32(len(txCounts))
	return nil
}

func GetDBCF(name string, cf string) (*grocksdb.DB, []*grocksdb.ColumnFamilyHandle, error) {
	opts := grocksdb.NewDefaultOptions()
	cfOpt := grocksdb.NewDefaultOptions()

	cfNames := []string{"default", cf}
	cfOpts := []*grocksdb.Options{cfOpt, cfOpt}

	db, handles, err := grocksdb.OpenDbAsSecondaryColumnFamilies(opts, name, "asdf", cfNames, cfOpts)

	for i, handle := range handles {
		log.Printf("%d: %+v\n", i, handle)
	}
	if err != nil {
		return nil, nil, err
	}

	return db, handles, nil
}

func GetDB(name string) (*grocksdb.DB, error) {
	opts := grocksdb.NewDefaultOptions()
	db, err := grocksdb.OpenDbAsSecondary(opts, name, "asdf")
	if err != nil {
		return nil, err
	}

	return db, nil
}

//
// Reading utility functions
//

// ReadPrefixN Reads n entries from a rocksdb db starting at the given prefix
// Does not use column families
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

// ReadWriteRawNColumnFamilies reads n entries from a given column famliy of a rocksdb db
// and writes then to a given file.
func ReadWriteRawNColumnFamilies(db *grocksdb.DB, options *IterOptions, out string, n int) {
	readWriteRawNCF(db, options, out, n, 1)
}

// ReadWriteRawNColumnFamilies reads n entries from a given column famliy of a rocksdb db
// and writes then to a given file.
func ReadWriteRawNCF(db *grocksdb.DB, options *IterOptions, out string, n int) {
	readWriteRawNCF(db, options, out, n, 0)
}

// readWriteRawNCF reads n entries from a given column famliy of a rocksdb db and
// writes them as a csv to a give file.
func readWriteRawNCF(db *grocksdb.DB, options *IterOptions, out string, n int, fileVersion int) {
	var formatStr string = ""
	switch fileVersion {
	case 0:
		formatStr = "%s,\n"
	case 1:
		formatStr = "%s,,\n"
	}

	options.RawKey = true
	options.RawValue = true
	ch := IterCF(db, options)

	file, err := os.Create(out)
	if err != nil {
		log.Println(err)
		return
	}
	defer file.Close()

	var i = 0
	log.Println(options.Prefix)
	cf := string(options.Prefix)
	file.Write([]byte(fmt.Sprintf(formatStr, options.Prefix)))
	for kv := range ch {
		log.Println(i)
		if i >= n {
			return
		}
		key := kv.Key.([]byte)
		value := kv.Value.([]byte)
		keyHex := hex.EncodeToString(key)
		valueHex := hex.EncodeToString(value)
		//log.Println(keyHex)
		//log.Println(valueHex)
		if fileVersion == 1 {
			file.WriteString(cf)
			file.WriteString(",")
		}
		file.WriteString(keyHex)
		file.WriteString(",")
		file.WriteString(valueHex)
		file.WriteString("\n")

		i++
	}
}

// ReadWriteRawN reads n entries from a given rocksdb db and writes them as a
// csv to a give file.
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

// GenerateTestData generates a test data file for a prefix.
func GenerateTestData(prefix byte, fileName string) {
	dbVal, err := GetDB("/mnt/d/data/wallet/lbry-rocksdb/")
	if err != nil {
		log.Fatalln(err)
	}

	options := NewIterateOptions()
	options.WithRawKey(true).WithRawValue(true).WithIncludeValue(true)
	options.WithPrefix([]byte{prefix})

	ReadWriteRawN(dbVal, options, fileName, 10)
}
