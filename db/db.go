package db

// db.go contains basic functions for representing and accessing the state of
// a read-only version of the rocksdb database.

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"os"
	"time"

	"github.com/lbryio/herald.go/db/prefixes"
	"github.com/lbryio/herald.go/db/stack"
	"github.com/lbryio/herald.go/internal"
	"github.com/lbryio/herald.go/internal/metrics"
	pb "github.com/lbryio/herald.go/protobuf/go"
	"github.com/linxGnu/grocksdb"

	log "github.com/sirupsen/logrus"
)

//
// Constants
//

const (
	// Blochchain height / expiration constants
	OriginalClaimExpirationTime       = 262974
	ExtendedClaimExpirationTime       = 2102400
	ExtendedClaimExpirationForkHeight = 400155
	NormalizedNameForkHeight          = 539940 // targeting 21 March 2019
	MinTakeoverWorkaroundHeight       = 496850
	MaxTakeoverWorkaroundHeight       = 658300 // targeting 30 Oct 2019
	WitnessForkHeight                 = 680770 // targeting 11 Dec 2019
	AllClaimsInMerkleForkHeight       = 658310 // targeting 30 Oct 2019
	ProportionalDelayFactor           = 32
	MaxTakeoverDelay                  = 4032
	// Initial size constants
	InitialTxCountSize = 1200000
)

//
// Types and constructors, getters, setters, etc.
//

type ReadOnlyDBColumnFamily struct {
	DB                     *grocksdb.DB
	Handles                map[string]*grocksdb.ColumnFamilyHandle
	Opts                   *grocksdb.ReadOptions
	TxCounts               *stack.SliceBacked[uint32]
	Height                 uint32
	LastState              *prefixes.DBStateValue
	Headers                *stack.SliceBacked[[]byte]
	BlockingChannelHashes  [][]byte
	FilteringChannelHashes [][]byte
	BlockedStreams         map[string][]byte
	BlockedChannels        map[string][]byte
	FilteredStreams        map[string][]byte
	FilteredChannels       map[string][]byte
	ShutdownChan           chan struct{}
	DoneChan               chan struct{}
	Cleanup                func()
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
	RepostTxHash       []byte
	RepostTxPostition  uint16
	RepostHeight       uint32
	ChannelTxHash      []byte
	ChannelTxPostition uint16
	ChannelHeight      uint32
}

type ResolveError struct {
	Error     error
	ErrorType uint8
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

func (res *ExpandedResolveResult) ToOutputs() ([]*pb.Output, []*pb.Output, error) {
	txos := make([]*pb.Output, 0)
	extraTxos := make([]*pb.Output, 0)
	// Errors
	if x := res.Channel.GetError(); x != nil {
		log.Warn("Channel error: ", x)
		outputErr := &pb.Output_Error{
			Error: &pb.Error{
				Text: x.Error.Error(),
				Code: pb.Error_Code(x.ErrorType),
			},
		}
		res := &pb.Output{Meta: outputErr}
		txos = append(txos, res)
		return txos, nil, nil
	}
	if x := res.Stream.GetError(); x != nil {
		log.Warn("Stream error: ", x)
		outputErr := &pb.Output_Error{
			Error: &pb.Error{
				Text: x.Error.Error(),
				Code: pb.Error_Code(x.ErrorType),
			},
		}
		res := &pb.Output{Meta: outputErr}
		txos = append(txos, res)
		return txos, nil, nil
	}

	// Not errors
	var channel, stream, repost, repostedChannel *ResolveResult

	channel = res.Channel.GetResult()
	stream = res.Stream.GetResult()
	repost = res.Repost.GetResult()
	repostedChannel = res.RepostedChannel.GetResult()

	if channel != nil && stream == nil {
		// Channel
		output := channel.ToOutput()
		txos = append(txos, output)

		if repost != nil {
			output := repost.ToOutput()
			extraTxos = append(extraTxos, output)
		}
		if repostedChannel != nil {
			output := repostedChannel.ToOutput()
			extraTxos = append(extraTxos, output)
		}

		return txos, extraTxos, nil
	} else if stream != nil {
		output := stream.ToOutput()
		txos = append(txos, output)
		if channel != nil {
			output := channel.ToOutput()
			extraTxos = append(extraTxos, output)
		}
		if repost != nil {
			output := repost.ToOutput()
			extraTxos = append(extraTxos, output)
		}
		if repostedChannel != nil {
			output := repostedChannel.ToOutput()
			extraTxos = append(extraTxos, output)
		}

		return txos, extraTxos, nil
	}

	return nil, nil, nil
}

// ToOutput
func (res *ResolveResult) ToOutput() *pb.Output {
	// func ResolveResultToOutput(res *db.ResolveResult, outputType byte) *OutputWType {
	// res.ClaimHash
	var channelOutput *pb.Output
	var repostOutput *pb.Output

	if res.ChannelTxHash != nil {
		channelOutput = &pb.Output{
			TxHash: res.ChannelTxHash,
			Nout:   uint32(res.ChannelTxPostition),
			Height: res.ChannelHeight,
		}
	}

	if res.RepostTxHash != nil {
		repostOutput = &pb.Output{
			TxHash: res.RepostTxHash,
			Nout:   uint32(res.RepostTxPostition),
			Height: res.RepostHeight,
		}
	}

	claimMeta := &pb.ClaimMeta{
		Channel:          channelOutput,
		Repost:           repostOutput,
		ShortUrl:         res.ShortUrl,
		Reposted:         uint32(res.Reposted),
		IsControlling:    res.IsControlling,
		CreationHeight:   res.CreationHeight,
		ExpirationHeight: res.ExpirationHeight,
		ClaimsInChannel:  res.ClaimsInChannel,
		EffectiveAmount:  res.EffectiveAmount,
		SupportAmount:    res.SupportAmount,
	}

	claim := &pb.Output_Claim{
		Claim: claimMeta,
	}

	output := &pb.Output{
		TxHash: res.TxHash,
		Nout:   uint32(res.Position),
		Height: res.Height,
		Meta:   claim,
	}

	return output
}

type PathSegment struct {
	name        string
	claimId     string
	amountOrder int
}

func (ps *PathSegment) Normalized() string {
	return internal.NormalizeName(ps.name)
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

//
// Iterators / db construction functions
//

func intMin(a, b int) int {
	if a < b {
		return a
	}
	return b
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
		defer func() {
			it.Close()
			close(ch)
			ro.Destroy()
		}()

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

func (db *ReadOnlyDBColumnFamily) selectFrom(prefix []byte, startKey, stopKey prefixes.BaseKey) ([]*IterOptions, error) {
	handle, err := db.EnsureHandle(prefixes.HashXHistory)
	if err != nil {
		return nil, err
	}
	// Prefix and handle
	options := NewIterateOptions().WithPrefix(prefix).WithCfHandle(handle)
	// Start and stop bounds
	options = options.WithStart(startKey.PackKey()).WithStop(stopKey.PackKey()).WithIncludeStop(true)
	// Don't include the key
	options = options.WithIncludeKey(true).WithIncludeValue(true)
	return []*IterOptions{options}, nil
}

func iterate(db *grocksdb.DB, opts []*IterOptions) <-chan []*prefixes.PrefixRowKV {
	out := make(chan []*prefixes.PrefixRowKV)
	routine := func() {
		for _, o := range opts {
			for kv := range IterCF(db, o) {
				row := make([]*prefixes.PrefixRowKV, 0, 1)
				row = append(row, kv)
				out <- row
			}
		}
		close(out)
	}
	go routine()
	return out
}

func innerJoin(db *grocksdb.DB, in <-chan []*prefixes.PrefixRowKV, selectFn func([]*prefixes.PrefixRowKV) ([]*IterOptions, error)) <-chan []*prefixes.PrefixRowKV {
	out := make(chan []*prefixes.PrefixRowKV)
	routine := func() {
		for kvs := range in {
			selected, err := selectFn(kvs)
			if err != nil {
				out <- []*prefixes.PrefixRowKV{{Error: err}}
				close(out)
				return
			}
			for kv := range iterate(db, selected) {
				row := make([]*prefixes.PrefixRowKV, 0, len(kvs)+1)
				row = append(row, kvs...)
				row = append(row, kv...)
				out <- row
			}
		}
		close(out)
		return
	}
	go routine()
	return out
}

func checkForError(kvs []*prefixes.PrefixRowKV) error {
	for _, kv := range kvs {
		if kv.Error != nil {
			return kv.Error
		}
	}
	return nil
}

//
// GetDB functions that open and return a db
//

// GetWriteDBCF opens a db for writing with all columns families opened.
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

	db, err := GetDBColumnFamilies(name, secondaryPath, cfNames)

	cleanupFiles := func() {
		err = os.RemoveAll(secondaryPath)
		if err != nil {
			log.Println(err)
		}
	}

	if err != nil {
		return nil, cleanupFiles, err
	}

	cleanupDB := func() {
		db.DB.Close()
		cleanupFiles()
	}
	db.Cleanup = cleanupDB

	return db, cleanupDB, nil
}

// GetDBColumnFamilies gets a db with the specified column families and secondary path.
func GetDBColumnFamilies(name string, secondayPath string, cfNames []string) (*ReadOnlyDBColumnFamily, error) {
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
		log.Printf("handle %d(%s): %+v\n", i, cfNames[i], handle)
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
		ShutdownChan:     make(chan struct{}),
		DoneChan:         make(chan struct{}),
	}

	err = myDB.ReadDBState() //TODO: Figure out right place for this
	if err != nil {
		return nil, err
	}

	err = myDB.InitTxCounts()
	if err != nil {
		return nil, err
	}

	err = myDB.InitHeaders()
	if err != nil {
		return nil, err
	}

	err = myDB.GetBlocksAndFilters()
	if err != nil {
		return nil, err
	}

	return myDB, nil
}

// Advance advance the db to the given height.
func (db *ReadOnlyDBColumnFamily) Advance(height uint32) {
	// DB wasn't created when we initialized headers, reinit
	if db.TxCounts.Len() == 0 {
		db.InitHeaders()
		db.InitTxCounts()
	}
	// TODO: assert tx_count not in self.db.tx_counts, f'boom {tx_count} in {len(self.db.tx_counts)} tx counts'
	if db.TxCounts.Len() != height {
		log.Error("tx count len:", db.TxCounts.Len(), "height:", height)
		return
	}

	headerObj, err := db.GetHeader(height)
	if err != nil {
		log.Error("getting header:", err)
		return
	}

	txCountObj, err := db.GetTxCount(height)
	if err != nil {
		log.Error("getting tx count:", err)
		return
	}
	txCount := txCountObj.TxCount

	if db.TxCounts.GetTip() >= txCount {
		log.Error("current tip should be less than new txCount",
			"tx count tip:", db.TxCounts.GetTip(), "tx count:", txCount)
	}

	db.TxCounts.Push(txCount)
	db.Headers.Push(headerObj)
}

// Unwind unwinds the db one block height
func (db *ReadOnlyDBColumnFamily) Unwind() {
	db.TxCounts.Pop()
	db.Headers.Pop()
}

// Shutdown shuts down the db.
func (db *ReadOnlyDBColumnFamily) Shutdown() {
	db.ShutdownChan <- struct{}{}
	<-db.DoneChan
	db.Cleanup()
}

// RunDetectChanges Go routine the runs continuously while the hub is active
// to keep the db readonly view up to date and handle reorgs on the
// blockchain.
func (db *ReadOnlyDBColumnFamily) RunDetectChanges(notifCh chan *internal.HeightHash) {
	go func() {
		lastPrint := time.Now()
		for {
			// FIXME: Figure out best sleep interval
			if time.Since(lastPrint) > time.Second {
				log.Debug("DetectChanges:", db.LastState)
				lastPrint = time.Now()
			}
			err := db.detectChanges(notifCh)
			if err != nil {
				log.Infof("Error detecting changes: %#v", err)
			}
			select {
			case <-db.ShutdownChan:
				db.DoneChan <- struct{}{}
				return
			case <-time.After(time.Millisecond * 10):
			}
		}
	}()
}

// DetectChanges keep the rocksdb db in sync and handle reorgs
func (db *ReadOnlyDBColumnFamily) detectChanges(notifCh chan *internal.HeightHash) error {
	err := db.DB.TryCatchUpWithPrimary()
	if err != nil {
		return err
	}

	state, err := db.GetDBState()
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
			lastHeightHeader, err := db.GetHeader(lastHeight)
			if err != nil {
				return err
			}
			curHeader := db.Headers.GetTip()
			if curHeader == nil {
				break
			}
			log.Debugln("lastHeightHeader: ", hex.EncodeToString(lastHeightHeader))
			log.Debugln("curHeader: ", hex.EncodeToString(curHeader))
			if bytes.Equal(curHeader, lastHeightHeader) {
				log.Traceln("connects to block", lastHeight)
				break
			} else {
				log.Infoln("disconnect block", lastHeight)
				db.Unwind()
				rewound = true
				lastHeight -= 1
				time.Sleep(time.Second)
			}
		}
	}
	if rewound {
		metrics.ReorgCount.Inc()
		hash, err := db.GetBlockHash(lastHeight)
		if err != nil {
			return err
		}
		notifCh <- &internal.HeightHash{Height: uint64(lastHeight), BlockHash: hash}
	}

	err = db.ReadDBState()
	if err != nil {
		return err
	}

	if db.LastState == nil || lastHeight < state.Height {
		for height := lastHeight + 1; height <= state.Height; height++ {
			log.Info("advancing to: ", height)
			db.Advance(height)
			hash, err := db.GetBlockHash(height)
			if err != nil {
				log.Info("error getting block hash: ", err)
				return err
			}
			notifCh <- &internal.HeightHash{Height: uint64(height), BlockHash: hash}
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
}

func (db *ReadOnlyDBColumnFamily) ReadDBState() error {
	state, err := db.GetDBState()
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

func (db *ReadOnlyDBColumnFamily) InitHeaders() error {
	handle, err := db.EnsureHandle(prefixes.Header)
	if err != nil {
		return err
	}

	//TODO: figure out a reasonable default and make it a constant
	db.Headers = stack.NewSliceBacked[[]byte](12000)

	startKey := prefixes.NewHeaderKey(0)
	// endKey := prefixes.NewHeaderKey(db.LastState.Height)
	startKeyRaw := startKey.PackKey()
	// endKeyRaw := endKey.PackKey()
	options := NewIterateOptions().WithPrefix([]byte{prefixes.Header}).WithCfHandle(handle)
	options = options.WithIncludeKey(false).WithIncludeValue(true) //.WithIncludeStop(true)
	options = options.WithStart(startKeyRaw)                       //.WithStop(endKeyRaw)

	ch := IterCF(db.DB, options)

	for header := range ch {
		db.Headers.Push(header.Value.(*prefixes.BlockHeaderValue).Header)
	}

	return nil
}

// InitTxCounts initializes the txCounts map
func (db *ReadOnlyDBColumnFamily) InitTxCounts() error {
	start := time.Now()
	handle, err := db.EnsureHandle(prefixes.TxCount)
	if err != nil {
		return err
	}

	db.TxCounts = stack.NewSliceBacked[uint32](InitialTxCountSize)

	options := NewIterateOptions().WithPrefix([]byte{prefixes.TxCount}).WithCfHandle(handle)
	options = options.WithIncludeKey(false).WithIncludeValue(true).WithIncludeStop(true)

	ch := IterCF(db.DB, options)

	for txCount := range ch {
		db.TxCounts.Push(txCount.Value.(*prefixes.TxCountValue).TxCount)
	}

	duration := time.Since(start)
	log.Println("len(db.TxCounts), cap(db.TxCounts):", db.TxCounts.Len(), db.TxCounts.Cap())
	log.Println("Time to get txCounts:", duration)

	return nil
}

// RunGetBlocksAndFilters Go routine that runs continuously while the hub is active
// to keep the blocked and filtered channels and streams up to date.
func (db *ReadOnlyDBColumnFamily) RunGetBlocksAndFilters() {
	go func() {
		for {
			// FIXME: Figure out best sleep interval
			err := db.GetBlocksAndFilters()
			if err != nil {
				log.Printf("Error getting blocked and filtered chanels: %#v\n", err)
			}
			time.Sleep(time.Second * 10)
		}
	}()
}

// GetBlocksAndFilters gets the blocked and filtered channels and streams from the database.
func (db *ReadOnlyDBColumnFamily) GetBlocksAndFilters() error {
	blockedChannels, blockedStreams, err := db.GetStreamsAndChannelRepostedByChannelHashes(db.BlockingChannelHashes)
	if err != nil {
		return err
	}

	db.BlockedChannels = blockedChannels
	db.BlockedStreams = blockedStreams

	filteredChannels, filteredStreams, err := db.GetStreamsAndChannelRepostedByChannelHashes(db.FilteringChannelHashes)
	if err != nil {
		return err
	}

	db.FilteredChannels = filteredChannels
	db.FilteredStreams = filteredStreams

	return nil
}

// GetDBCF Get the database and open given column families.
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

// GetDB Get the database.
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
			RawKey:   key.Data(),
			RawValue: value.Data(),
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
		key := kv.RawKey
		value := kv.RawValue
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
		key := kv.RawKey
		value := kv.RawValue
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
