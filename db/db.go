package db

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"sort"
	"time"

	"github.com/lbryio/hub/db/db_stack"
	"github.com/lbryio/hub/db/prefixes"
	"github.com/lbryio/hub/internal/metrics"
	"github.com/lbryio/lbry.go/v2/extras/util"
	"github.com/linxGnu/grocksdb"

	log "github.com/sirupsen/logrus"
)

//
// Constants
//

const (
	// Blochchain height / expiration constants
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
	// Initial size constants
	InitialTxCountSize = 1200000
)

//
// Types and constructors, getters, setters, etc.
//

type ReadOnlyDBColumnFamily struct {
	DB               *grocksdb.DB
	Handles          map[string]*grocksdb.ColumnFamilyHandle
	Opts             *grocksdb.ReadOptions
	TxCounts         *db_stack.SliceBackedStack
	Height           uint32
	LastState        *prefixes.DBStateValue
	Headers          *db_stack.SliceBackedStack
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

func NewExpandedResolveResult() *ExpandedResolveResult {
	return &ExpandedResolveResult{
		Stream:          &optionalResolveResultOrError{},
		Channel:         &optionalResolveResultOrError{},
		Repost:          &optionalResolveResultOrError{},
		RepostedChannel: &optionalResolveResultOrError{},
	}
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
func BisectRight(arr []interface{}, val uint32) uint32 {
	i := sort.Search(len(arr), func(i int) bool { return arr[i].(uint32) >= val })
	return uint32(i)
}

//
// Iterators / db construction functions
//

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
}

func (o *IterOptions) StopIteration(key []byte) bool {
	if key == nil {
		return false
	}

	// TODO: Look at not doing floating point conversions for this
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

func (opts *IterOptions) ReadRow(prevKey *[]byte) *prefixes.PrefixRowKV {
	it := opts.It
	if !it.Valid() {
		log.Trace("ReadRow iterator not valid returning nil")
		return nil
	}

	key := it.Key()
	keyData := key.Data()
	keyLen := len(keyData)
	value := it.Value()
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

	key.Free()
	value.Free()

	kv := &prefixes.PrefixRowKV{
		Key:   outKey,
		Value: outValue,
	}
	*prevKey = newKeyData

	return kv
}

func IterCF(db *grocksdb.DB, opts *IterOptions) <-chan *prefixes.PrefixRowKV {
	ch := make(chan *prefixes.PrefixRowKV)

	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(opts.FillCache)
	it := db.NewIteratorCF(ro, opts.CfHandle)
	opts.It = it

	it.Seek(opts.Prefix)
	if opts.Start != nil {
		it.Seek(opts.Start)
	}

	go func() {
		defer it.Close()
		defer close(ch)

		var prevKey []byte
		// FIXME: There's messy uses of kv being nil / not nil here.
		var kv *prefixes.PrefixRowKV = nil
		if !opts.IncludeStart {
			kv = opts.ReadRow(&prevKey)
			it.Next()
		}

		if !it.Valid() && opts.IncludeStop && kv != nil {
			ch <- kv
		}

		kv = &prefixes.PrefixRowKV{}
		for ; kv != nil && !opts.StopIteration(prevKey) && it.Valid(); it.Next() {
			if kv = opts.ReadRow(&prevKey); kv != nil {
				ch <- kv
			}
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

		var prevKey []byte
		var kv *prefixes.PrefixRowKV = &prefixes.PrefixRowKV{}
		if !opts.IncludeStart {
			kv = opts.ReadRow(&prevKey)
			it.Next()
		}

		if !it.Valid() && opts.IncludeStop && kv != nil {
			ch <- kv
		}

		for ; kv != nil && !opts.StopIteration(prevKey) && it.Valid(); it.Next() {
			if kv = opts.ReadRow(&prevKey); kv != nil {
				ch <- kv
			}
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

// GetProdDB returns a db that is used for production.
func GetProdDB(name string, secondaryPath string) (*ReadOnlyDBColumnFamily, func(), error) {
	prefixNames := prefixes.GetPrefixes()
	// additional prefixes that aren't in the code explicitly
	cfNames := []string{"default", "e", "d", "c"}
	for _, prefix := range prefixNames {
		cfName := string(prefix)
		cfNames = append(cfNames, cfName)
	}

	db, err := GetDBColumnFamlies(name, secondaryPath, cfNames)

	cleanup := func() {
		db.DB.Close()
		err = os.RemoveAll(fmt.Sprintf("./%s", secondaryPath))
		if err != nil {
			log.Println(err)
		}
	}

	if err != nil {
		return nil, cleanup, err
	}

	return db, cleanup, nil
}

func GetDBColumnFamlies(name string, secondayPath string, cfNames []string) (*ReadOnlyDBColumnFamily, error) {
	opts := grocksdb.NewDefaultOptions()
	roOpts := grocksdb.NewDefaultReadOptions()
	cfOpt := grocksdb.NewDefaultOptions()

	//cfNames := []string{"default", cf}
	cfOpts := make([]*grocksdb.Options, len(cfNames))
	for i := range cfNames {
		cfOpts[i] = cfOpt
	}

	db, handles, err := grocksdb.OpenDbAsSecondaryColumnFamilies(opts, name, secondayPath, cfNames, cfOpts)
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
		LastState:        nil,
		Height:           0,
		Headers:          nil,
	}

	err = ReadDBState(myDB) //TODO: Figure out right place for this
	if err != nil {
		return nil, err
	}

	err = InitTxCounts(myDB)
	if err != nil {
		return nil, err
	}

	err = InitHeaders(myDB)
	if err != nil {
		return nil, err
	}

	return myDB, nil
}

func Advance(db *ReadOnlyDBColumnFamily, height uint32) {
	// TODO: assert tx_count not in self.db.tx_counts, f'boom {tx_count} in {len(self.db.tx_counts)} tx counts'
	if db.TxCounts.Len() != height {
		log.Error("tx count len:", db.TxCounts.Len(), "height:", height)
		return
	}

	headerObj, err := GetHeader(db, height)
	if err != nil {
		log.Error("getting header:", err)
		return
	}

	txCountObj, err := GetTxCount(db, height)
	if err != nil {
		log.Error("getting tx count:", err)
		return
	}
	txCount := txCountObj.TxCount

	if db.TxCounts.GetTip().(uint32) >= txCount {
		log.Error("current tip should be less than new txCount",
			"tx count tip:", db.TxCounts.GetTip(), "tx count:", txCount)
	}

	db.TxCounts.Push(txCount)
	db.Headers.Push(headerObj)
}

func Unwind(db *ReadOnlyDBColumnFamily) {
	db.TxCounts.Pop()
	db.Headers.Pop()
}

// RunDetectChanges Go routine the runs continuously while the hub is active
// to keep the db readonly view up to date and handle reorgs on the
// blockchain.
func RunDetectChanges(db *ReadOnlyDBColumnFamily) {
	go func() {
		for {
			// FIXME: Figure out best sleep interval
			err := DetectChanges(db)
			if err != nil {
				log.Printf("Error detecting changes: %#v\n", err)
			}
			time.Sleep(time.Second)
		}
	}()
}

// DetectChanges keep the rocksdb db in sync and handle reorgs
func DetectChanges(db *ReadOnlyDBColumnFamily) error {
	err := db.DB.TryCatchUpWithPrimary()
	if err != nil {
		return err
	}

	state, err := GetDBState(db)
	if err != nil {
		return err
	}

	if state == nil || state.Height <= 0 {
		return nil
	}

	log.Debugf("db.LastState %#v, state: %#v", db.LastState, state)

	if db.LastState != nil && db.LastState.Height > state.Height {
		log.Info("reorg detected, waiting until the writer has flushed the new blocks to advance")
		return nil
	}

	var lastHeight uint32 = 0
	var rewound bool = false
	if db.LastState != nil {
		lastHeight = db.LastState.Height
		for {
			lastHeightHeader, err := GetHeader(db, lastHeight)
			if err != nil {
				return err
			}
			curHeader := db.Headers.GetTip().([]byte)
			log.Debugln("lastHeightHeader: ", hex.EncodeToString(lastHeightHeader))
			log.Debugln("curHeader: ", hex.EncodeToString(curHeader))
			if bytes.Equal(curHeader, lastHeightHeader) {
				log.Traceln("connects to block", lastHeight)
				break
			} else {
				log.Infoln("disconnect block", lastHeight)
				Unwind(db)
				rewound = true
				lastHeight -= 1
				time.Sleep(time.Second)
			}
		}
	}
	if rewound {
		metrics.ReorgCount.Inc()
	}

	err = ReadDBState(db)
	if err != nil {
		return err
	}

	if db.LastState == nil || lastHeight < state.Height {
		for height := lastHeight + 1; height <= state.Height; height++ {
			log.Info("advancing to: ", height)
			Advance(db, height)
		}
		//TODO: ClearCache
		log.Warn("implement cache clearing")

		db.LastState = state
		metrics.BlockCount.Inc()

		//TODO: update blocked streams
		//TODO: update filtered streams
		log.Warn("implement updating blocked streams")
		log.Warn("implement updating filtered streams")
	}

	return nil
	/*
	   self.db.blocked_streams, self.db.blocked_channels = self.db.get_streams_and_channels_reposted_by_channel_hashes(
	       self.db.blocking_channel_hashes
	   )
	   self.db.filtered_streams, self.db.filtered_channels = self.db.get_streams_and_channels_reposted_by_channel_hashes(
	       self.db.filtering_channel_hashes
	   )
	*/
}

/*
   def read_db_state(self):
       state = self.prefix_db.db_state.get()

       if not state:
           self.db_height = -1
           self.db_tx_count = 0
           self.db_tip = b'\0' * 32
           self.db_version = max(self.DB_VERSIONS)
           self.utxo_flush_count = 0
           self.wall_time = 0
           self.first_sync = True
           self.hist_flush_count = 0
           self.hist_comp_flush_count = -1
           self.hist_comp_cursor = -1
           self.hist_db_version = max(self.DB_VERSIONS)
           self.es_sync_height = 0
       else:
           self.db_version = state.db_version
           if self.db_version not in self.DB_VERSIONS:
               raise DBError(f'your DB version is {self.db_version} but this '
                                  f'software only handles versions {self.DB_VERSIONS}')
           # backwards compat
           genesis_hash = state.genesis
           if genesis_hash.hex() != self.coin.GENESIS_HASH:
               raise DBError(f'DB genesis hash {genesis_hash} does not '
                                  f'match coin {self.coin.GENESIS_HASH}')
           self.db_height = state.height
           self.db_tx_count = state.tx_count
           self.db_tip = state.tip
           self.utxo_flush_count = state.utxo_flush_count
           self.wall_time = state.wall_time
           self.first_sync = state.first_sync
           self.hist_flush_count = state.hist_flush_count
           self.hist_comp_flush_count = state.comp_flush_count
           self.hist_comp_cursor = state.comp_cursor
           self.hist_db_version = state.db_version
           self.es_sync_height = state.es_sync_height
       return state
*/
func ReadDBState(db *ReadOnlyDBColumnFamily) error {
	state, err := GetDBState(db)
	if err != nil {
		return err
	}
	if state != nil {
		db.LastState = state
	} else {
		db.LastState = prefixes.NewDBStateValue()
	}

	return nil
}

func InitHeaders(db *ReadOnlyDBColumnFamily) error {
	handle, err := EnsureHandle(db, prefixes.Header)
	if err != nil {
		return err
	}

	//TODO: figure out a reasonable default and make it a constant
	db.Headers = db_stack.NewSliceBackedStack(12000)

	startKey := prefixes.NewHeaderKey(0)
	endKey := prefixes.NewHeaderKey(db.LastState.Height)
	startKeyRaw := startKey.PackKey()
	endKeyRaw := endKey.PackKey()
	options := NewIterateOptions().WithPrefix([]byte{prefixes.Header}).WithCfHandle(handle)
	options = options.WithIncludeKey(false).WithIncludeValue(true).WithIncludeStop(true)
	options = options.WithStart(startKeyRaw).WithStop(endKeyRaw)

	ch := IterCF(db.DB, options)

	for header := range ch {
		db.Headers.Push(header.Value.(*prefixes.BlockHeaderValue).Header)
	}

	return nil
}

// InitTxCounts initializes the txCounts map
func InitTxCounts(db *ReadOnlyDBColumnFamily) error {
	start := time.Now()
	handle, err := EnsureHandle(db, prefixes.TxCount)
	if err != nil {
		return err
	}

	db.TxCounts = db_stack.NewSliceBackedStack(InitialTxCountSize)

	options := NewIterateOptions().WithPrefix([]byte{prefixes.TxCount}).WithCfHandle(handle)
	options = options.WithIncludeKey(false).WithIncludeValue(true).WithIncludeStop(true)

	ch := IterCF(db.DB, options)

	for txCount := range ch {
		db.TxCounts.Push(txCount.Value.(*prefixes.TxCountValue).TxCount)
	}

	duration := time.Since(start)
	log.Println("len(db.TxCounts), cap(db.TxCounts):", db.TxCounts.Len(), db.TxCounts.Cap())
	log.Println("Time to get txCounts:", duration)

	// whjy not needs to be len-1 because we start loading with the zero block
	// and the txcounts start at one???
	// db.Height = db.TxCounts.Len()
	// if db.TxCounts.Len() > 0 {
	// 	db.Height = db.TxCounts.Len() - 1
	// } else {
	// 	log.Println("db.TxCounts.Len() == 0 ???")
	// }

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
		log.Error(err)
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
		log.Error(err)
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
