package db

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"log"
	"math"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/lbryio/hub/db/prefixes"
	"github.com/lbryio/lbry.go/v2/extras/util"
	lbryurl "github.com/lbryio/lbry.go/v2/url"
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

type ExpandedResolveResult struct {
	Stream          OptionalResolveResultOrError
	Channel         OptionalResolveResultOrError
	Repost          OptionalResolveResultOrError
	RepostedChannel OptionalResolveResultOrError
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
func BisectRight(arr []uint32, val uint32) uint32 {
	i := sort.Search(len(arr), func(i int) bool { return arr[i] >= val })
	return uint32(i)
}

func GetExpirationHeight(lastUpdatedHeight uint32) uint32 {
	return GetExpirationHeightFull(lastUpdatedHeight, false)
}

func GetExpirationHeightFull(lastUpdatedHeight uint32, extended bool) uint32 {
	if extended {
		return lastUpdatedHeight + NExtendedClaimExpirationTime
	}
	if lastUpdatedHeight < NExtendedClaimExpirationForkHeight {
		return lastUpdatedHeight + NOriginalClaimExpirationTime
	}
	return lastUpdatedHeight + NExtendedClaimExpirationTime
}

//
// DB Get functions
//

func GetClaimsInChannelCount(db *ReadOnlyDBColumnFamily, channelHash []byte) (uint32, error) {
	prefix := []byte{prefixes.ChannelCount}
	handle := db.Handles[string(prefix)]
	key := prefixes.NewChannelCountKey(channelHash)
	rawKey := key.PackKey()

	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	if err != nil {
		return 0, err
	} else if slice.Size() == 0 {
		return 0, nil
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.ChannelCountValueUnpack(rawValue)

	return value.Count, nil
}

func GetShortClaimIdUrl(db *ReadOnlyDBColumnFamily, name string, normalizedName string, claimHash []byte, rootTxNum uint32, rootPosition uint16) (string, error) {
	prefix := []byte{prefixes.ClaimShortIdPrefix}
	handle := db.Handles[string(prefix)]
	claimId := hex.EncodeToString(claimHash)
	claimIdLen := len(claimId)
	for prefixLen := 0; prefixLen < 10; prefixLen++ {
		var j int = prefixLen + 1
		if j > claimIdLen {
			j = claimIdLen
		}
		partialClaimId := claimId[:j]
		partialKey := prefixes.NewClaimShortIDKey(normalizedName, partialClaimId)
		keyPrefix := prefixes.ClaimShortIDKeyPackPartial(partialKey, 2)
		// Prefix and handle
		options := NewIterateOptions().WithPrefix(prefix).WithCfHandle(handle)
		// Start and stop bounds
		options = options.WithStart(keyPrefix)
		// Don't include the key
		options = options.WithIncludeValue(false)

		ch := IterCF(db.DB, options)
		row := <-ch
		key := row.Key.(*prefixes.ClaimShortIDKey)
		if key.RootTxNum == rootTxNum && key.RootPosition == rootPosition {
			return fmt.Sprintf("%s#%s", name, key.PartialClaimId), nil
		}
	}
	return "", nil
}

func GetRepost(db *ReadOnlyDBColumnFamily, claimHash []byte) ([]byte, error) {
	prefix := []byte{prefixes.Repost}
	handle := db.Handles[string(prefix)]
	key := prefixes.NewRepostKey(claimHash)
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	if err != nil {
		return nil, err
	} else if slice.Size() == 0 {
		return nil, nil
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.RepostValueUnpack(rawValue)
	return value.RepostedClaimHash, nil
}

func GetRepostedCount(db *ReadOnlyDBColumnFamily, claimHash []byte) (int, error) {
	prefix := []byte{prefixes.RepostedClaim}
	handle := db.Handles[string(prefix)]
	key := prefixes.NewRepostedKey(claimHash)
	keyPrefix := prefixes.RepostedKeyPackPartial(key, 1)
	// Prefix and handle
	options := NewIterateOptions().WithPrefix(prefix).WithCfHandle(handle)
	// Start and stop bounds
	options = options.WithStart(keyPrefix)
	// Don't include the key
	options = options.WithIncludeValue(false)

	var i int = 0
	ch := IterCF(db.DB, options)

	for range ch {
		i++
	}

	return i, nil
}

func GetChannelForClaim(db *ReadOnlyDBColumnFamily, claimHash []byte, txNum uint32, position uint16) ([]byte, error) {
	key := prefixes.NewClaimToChannelKey(claimHash, txNum, position)
	cfName := string(prefixes.ClaimToChannel)
	handle := db.Handles[cfName]
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	if err != nil {
		return nil, err
	} else if slice.Size() == 0 {
		return nil, nil
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.ClaimToChannelValueUnpack(rawValue)
	return value.SigningHash, nil
}

func GetActiveAmount(db *ReadOnlyDBColumnFamily, claimHash []byte, txoType uint8, height uint32) (uint64, error) {
	cfName := string(prefixes.ActiveAmount)
	handle := db.Handles[cfName]
	startKey := &prefixes.ActiveAmountKey{
		Prefix:           []byte{prefixes.ActiveAmount},
		ClaimHash:        claimHash,
		TxoType:          txoType,
		ActivationHeight: 0,
	}
	endKey := &prefixes.ActiveAmountKey{
		Prefix:           []byte{prefixes.ActiveAmount},
		ClaimHash:        claimHash,
		TxoType:          txoType,
		ActivationHeight: height,
	}
	startKeyRaw := prefixes.ActiveAmountKeyPackPartial(startKey, 3)
	endKeyRaw := prefixes.ActiveAmountKeyPackPartial(endKey, 3)
	// Prefix and handle
	options := NewIterateOptions().WithPrefix([]byte{prefixes.ActiveAmount}).WithCfHandle(handle)
	// Start and stop bounds
	options = options.WithStart(startKeyRaw).WithStop(endKeyRaw)
	// Don't include the key
	options = options.WithIncludeKey(false).WithIncludeValue(true)

	ch := IterCF(db.DB, options)
	var sum uint64 = 0
	for kv := range ch {
		sum += kv.Value.(*prefixes.ActiveAmountValue).Amount
	}

	return sum, nil
}

func GetEffectiveAmount(db *ReadOnlyDBColumnFamily, claimHash []byte, supportOnly bool) (uint64, error) {
	supportAmount, err := GetActiveAmount(db, claimHash, prefixes.ACTIVATED_SUPPORT_TXO_TYPE, db.Height+1)
	if err != nil {
		return 0, err
	}

	if supportOnly {
		return supportAmount, nil
	}

	activationAmount, err := GetActiveAmount(db, claimHash, prefixes.ACTIVATED_CLAIM_TXO_TYPE, db.Height+1)
	if err != nil {
		return 0, err
	}

	return activationAmount + supportAmount, nil
}

func GetSupportAmount(db *ReadOnlyDBColumnFamily, claimHash []byte) (uint64, error) {
	key := prefixes.NewSupportAmountKey(claimHash)
	cfName := string(prefixes.SupportAmount)
	handle := db.Handles[cfName]
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	if err != nil {
		return 0, err
	} else if slice.Size() == 0 {
		return 0, nil
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.SupportAmountValueUnpack(rawValue)
	return value.Amount, nil
}

func GetTxHash(db *ReadOnlyDBColumnFamily, txNum uint32) ([]byte, error) {
	/*
	   if self._cache_all_tx_hashes:
	       return self.total_transactions[tx_num]
	   return self.prefix_db.tx_hash.get(tx_num, deserialize_value=False)
	*/
	// TODO: caching
	key := prefixes.NewTxHashKey(txNum)
	cfName := string(prefixes.TxHash)
	handle := db.Handles[cfName]
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	if err != nil {
		return nil, err
	}
	if slice.Size() == 0 {
		return nil, nil
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	return rawValue, nil
}

func GetActivation(db *ReadOnlyDBColumnFamily, txNum uint32, postition uint16) (uint32, error) {
	return GetActivationFull(db, txNum, postition, false)
}

func GetActivationFull(db *ReadOnlyDBColumnFamily, txNum uint32, postition uint16, isSupport bool) (uint32, error) {
	var typ uint8
	if isSupport {
		typ = prefixes.ACTIVATED_SUPPORT_TXO_TYPE
	} else {
		typ = prefixes.ACTIVATED_CLAIM_TXO_TYPE
	}

	key := prefixes.NewActivationKey(typ, txNum, postition)
	cfName := string(prefixes.ActivatedClaimAndSupport)
	handle := db.Handles[cfName]
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	if err != nil {
		return 0, err
	}
	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.ActivationValueUnpack(rawValue)
	// Does this need to explicitly return an int64, in case the uint32 overflows the max of an int?
	return value.Height, nil
}

func GetCachedClaimTxo(db *ReadOnlyDBColumnFamily, claim []byte) (*prefixes.ClaimToTXOValue, error) {
	// TODO: implement cache
	key := prefixes.NewClaimToTXOKey(claim)
	cfName := string(prefixes.ClaimToTXO)
	handle := db.Handles[cfName]
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	if err != nil {
		return nil, err
	}
	if slice.Size() == 0 {
		return nil, nil
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.ClaimToTXOValueUnpack(rawValue)
	return value, nil
}

func GetControllingClaim(db *ReadOnlyDBColumnFamily, name string) (*prefixes.ClaimTakeoverValue, error) {
	key := prefixes.NewClaimTakeoverKey(name)
	cfName := string(prefixes.ClaimTakeover)
	handle := db.Handles[cfName]
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	if err != nil {
		return nil, err
	}
	if slice.Size() == 0 {
		return nil, nil
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.ClaimTakeoverValueUnpack(rawValue)
	return value, nil
}

func FsGetClaimByHash(db *ReadOnlyDBColumnFamily, claimHash []byte) (*ResolveResult, error) {
	claim, err := GetCachedClaimTxo(db, claimHash)
	if err != nil {
		return nil, err
	}

	activation, err := GetActivation(db, claim.TxNum, claim.Position)
	if err != nil {
		return nil, err
	}

	log.Printf("%#v\n%#v\n%#v\n", claim, hex.EncodeToString(claimHash), activation)
	return PrepareResolveResult(
		db,
		claim.TxNum,
		claim.Position,
		claimHash,
		claim.Name,
		claim.RootTxNum,
		claim.RootPosition,
		activation,
		claim.ChannelSignatureIsValid,
	)
}

func GetDBState(db *ReadOnlyDBColumnFamily) (*prefixes.DBStateValue, error) {
	prefix := []byte{prefixes.DBState}
	key := prefixes.NewDBStateKey()
	handle := db.Handles[string(prefix)]
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	if err != nil {
		return nil, err
	} else if slice.Size() == 0 {
		return nil, nil
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.DBStateValueUnpack(rawValue)
	return value, nil
}

func ClaimShortIdIter(db *ReadOnlyDBColumnFamily, normalizedName string, claimId string) <-chan *prefixes.PrefixRowKV {
	prefix := []byte{prefixes.ClaimShortIdPrefix}
	handle := db.Handles[string(prefix)]
	key := prefixes.NewClaimShortIDKey(normalizedName, claimId)
	var rawKeyPrefix []byte = nil
	if claimId != "" {
		rawKeyPrefix = prefixes.ClaimShortIDKeyPackPartial(key, 2)
	} else {
		rawKeyPrefix = prefixes.ClaimShortIDKeyPackPartial(key, 1)
	}
	options := NewIterateOptions().WithCfHandle(handle).WithPrefix(rawKeyPrefix)
	options = options.WithIncludeValue(true) //.WithIncludeStop(true)
	ch := IterCF(db.DB, options)
	return ch
}

func GetCachedClaimHash(db *ReadOnlyDBColumnFamily, txNum uint32, position uint16) (*prefixes.TXOToClaimValue, error) {
	// TODO: implement cache
	key := prefixes.NewTXOToClaimKey(txNum, position)
	cfName := string(prefixes.TXOToClaim)
	handle := db.Handles[cfName]
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	if err != nil {
		return nil, err
	} else if slice.Size() == 0 {
		return nil, nil
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.TXOToClaimValueUnpack(rawValue)
	return value, nil
}

// GetBlockerHash get the hash of the blocker or filterer of the claim.
// TODO: this currently converts the byte arrays to strings, which is not
// very efficient. Might want to figure out a better way to do this.
func GetBlockerHash(db *ReadOnlyDBColumnFamily, claimHash, repostedClaimHash, channelHash []byte) ([]byte, []byte, error) {
	claimHashStr := string(claimHash)
	respostedClaimHashStr := string(repostedClaimHash)
	channelHashStr := string(channelHash)

	var blockedHash []byte = nil
	var filteredHash []byte = nil

	blockedHash = db.BlockedStreams[claimHashStr]
	if blockedHash == nil {
		blockedHash = db.BlockedStreams[respostedClaimHashStr]
	}
	if blockedHash == nil {
		blockedHash = db.BlockedChannels[claimHashStr]
	}
	if blockedHash == nil {
		blockedHash = db.BlockedChannels[respostedClaimHashStr]
	}
	if blockedHash == nil {
		blockedHash = db.BlockedChannels[channelHashStr]
	}

	filteredHash = db.FilteredStreams[claimHashStr]
	if filteredHash == nil {
		filteredHash = db.FilteredStreams[respostedClaimHashStr]
	}
	if filteredHash == nil {
		filteredHash = db.FilteredChannels[claimHashStr]
	}
	if filteredHash == nil {
		filteredHash = db.FilteredChannels[respostedClaimHashStr]
	}
	if filteredHash == nil {
		filteredHash = db.FilteredChannels[channelHashStr]
	}

	return blockedHash, filteredHash, nil
}

//
// Resolve functions
//

// PrepareResolveResult prepares a ResolveResult to return
func PrepareResolveResult(
	db *ReadOnlyDBColumnFamily,
	txNum uint32,
	position uint16,
	claimHash []byte,
	name string,
	rootTxNum uint32,
	rootPosition uint16,
	activationHeight uint32,
	signatureValid bool) (*ResolveResult, error) {

	normalizedName := util.NormalizeName(name)
	controllingClaim, err := GetControllingClaim(db, normalizedName)
	if err != nil {
		return nil, err
	}

	txHash, err := GetTxHash(db, txNum)
	if err != nil {
		return nil, err
	}

	height := BisectRight(db.TxCounts, txNum)
	createdHeight := BisectRight(db.TxCounts, rootTxNum)
	lastTakeoverHeight := controllingClaim.Height

	expirationHeight := GetExpirationHeight(height)

	supportAmount, err := GetSupportAmount(db, claimHash)
	if err != nil {
		return nil, err
	}

	claimToTxo, err := GetCachedClaimTxo(db, claimHash)
	if err != nil {
		return nil, err
	}
	claimAmount := claimToTxo.Amount

	effectiveAmount, err := GetEffectiveAmount(db, claimHash, false)
	if err != nil {
		return nil, err
	}

	channelHash, err := GetChannelForClaim(db, claimHash, txNum, position)
	if err != nil {
		return nil, err
	}

	repostedClaimHash, err := GetRepost(db, claimHash)
	if err != nil {
		return nil, err
	}

	shortUrl, err := GetShortClaimIdUrl(db, name, normalizedName, claimHash, txNum, rootPosition)
	if err != nil {
		return nil, err
	}

	var canonicalUrl string = shortUrl
	claimsInChannel, err := GetClaimsInChannelCount(db, claimHash)
	if err != nil {
		return nil, err
	}

	if channelHash != nil {
		// Ignore error because we already have this set if this doesn't work
		channelVals, _ := GetCachedClaimTxo(db, channelHash)
		log.Printf("channelVals: %#v\n", channelVals)
		if channelVals != nil {
			channelShortUrl, _ := GetShortClaimIdUrl(
				db,
				channelVals.Name,
				channelVals.NormalizedName(),
				channelHash, channelVals.RootTxNum,
				channelVals.RootPosition,
			)
			canonicalUrl = fmt.Sprintf("%s/%s", channelShortUrl, shortUrl)
		}
	}

	reposted, err := GetRepostedCount(db, claimHash)
	if err != nil {
		return nil, err
	}

	isControlling := bytes.Equal(controllingClaim.ClaimHash, claimHash)

	return &ResolveResult{
		Name:               name,
		NormalizedName:     normalizedName,
		ClaimHash:          claimHash,
		TxNum:              txNum,
		Position:           position,
		TxHash:             txHash,
		Height:             height,
		Amount:             claimAmount,
		ShortUrl:           shortUrl,
		IsControlling:      isControlling,
		CanonicalUrl:       canonicalUrl,
		CreationHeight:     createdHeight,
		ActivationHeight:   activationHeight,
		ExpirationHeight:   expirationHeight,
		EffectiveAmount:    effectiveAmount,
		SupportAmount:      supportAmount,
		Reposted:           reposted,
		LastTakeoverHeight: lastTakeoverHeight,
		ClaimsInChannel:    claimsInChannel,
		ChannelHash:        channelHash,
		RepostedClaimHash:  repostedClaimHash,
		SignatureValid:     signatureValid,
	}, nil
}

func ResolveParsedUrl(db *ReadOnlyDBColumnFamily, parsed *PathSegment) (*ResolveResult, error) {
	normalizedName := util.NormalizeName(parsed.name)
	if (parsed.amountOrder == -1 && parsed.claimId == "") || parsed.amountOrder == 1 {
		controlling, err := GetControllingClaim(db, normalizedName)
		if err != nil {
			return nil, err
		}
		if controlling == nil {
			return nil, nil
		}
		return FsGetClaimByHash(db, controlling.ClaimHash)
	}

	var amountOrder int = int(math.Max(float64(parsed.amountOrder), 1))

	log.Println("amountOrder:", amountOrder)

	if parsed.claimId != "" {
		if len(parsed.claimId) == 40 {
			claimHash, err := hex.DecodeString(parsed.claimId)
			if err != nil {
				return nil, err
			}

			// Maybe don't use caching version, when I actually implement the cache
			claimTxo, err := GetCachedClaimTxo(db, claimHash)
			if err != nil {
				return nil, err
			}

			if claimTxo == nil || claimTxo.NormalizedName() != normalizedName {
				return nil, nil
			}

			activation, err := GetActivation(db, claimTxo.TxNum, claimTxo.Position)
			if err != nil {
				return nil, err
			}

			return PrepareResolveResult(
				db,
				claimTxo.TxNum,
				claimTxo.Position,
				claimHash,
				claimTxo.Name,
				claimTxo.RootTxNum,
				claimTxo.RootPosition,
				activation,
				claimTxo.ChannelSignatureIsValid,
			)
		}
		log.Println("nomalizedName:", normalizedName)
		log.Println("claimId:", parsed.claimId)
		var j int = 10
		if len(parsed.claimId) < j {
			j = len(parsed.claimId)
		}

		ch := ClaimShortIdIter(db, normalizedName, parsed.claimId[:j])
		row := <-ch
		key := row.Key.(*prefixes.ClaimShortIDKey)
		claimTxo := row.Value.(*prefixes.ClaimShortIDValue)

		fullClaimHash, err := GetCachedClaimHash(db, claimTxo.TxNum, claimTxo.Position)
		if err != nil {
			return nil, err
		}

		c, err := GetCachedClaimTxo(db, fullClaimHash.ClaimHash)
		if err != nil {
			return nil, err
		}

		nonNormalizedName := c.Name
		signatureIsValid := c.ChannelSignatureIsValid
		activation, err := GetActivation(db, claimTxo.TxNum, claimTxo.Position)

		if err != nil {
			return nil, err
		}

		return PrepareResolveResult(
			db,
			claimTxo.TxNum,
			claimTxo.Position,
			fullClaimHash.ClaimHash,
			nonNormalizedName,
			key.RootTxNum,
			key.RootPosition,
			activation,
			signatureIsValid,
		)
	}

	return nil, nil
}

func ResolveClaimInChannel(db *ReadOnlyDBColumnFamily, channelHash []byte, normalizedName string) (*ResolveResult, error) {
	prefix := []byte{prefixes.ChannelToClaim}
	handle := db.Handles[string(prefix)]
	key := prefixes.NewChannelToClaimKey(channelHash, normalizedName)
	rawKeyPrefix := prefixes.ChannelToClaimKeyPackPartial(key, 2)
	options := NewIterateOptions().WithCfHandle(handle).WithPrefix(rawKeyPrefix)
	options = options.WithIncludeValue(true) //.WithIncludeStop(true)
	ch := IterCF(db.DB, options)
	// TODO: what's a good default size for this?
	var candidates []*ResolveResult = make([]*ResolveResult, 0, 100)
	var i = 0
	for row := range ch {
		key := row.Key.(*prefixes.ChannelToClaimKey)
		stream := row.Value.(*prefixes.ChannelToClaimValue)
		effectiveAmount, err := GetEffectiveAmount(db, stream.ClaimHash, false)
		if err != nil {
			return nil, err
		}
		if i == 0 || candidates[i-1].Amount == effectiveAmount {
			candidates = append(
				candidates,
				&ResolveResult{
					TxNum:          key.TxNum,
					Position:       key.Position,
					ClaimHash:      stream.ClaimHash,
					Amount:         effectiveAmount,
					ChannelHash:    channelHash,
					NormalizedName: normalizedName,
				},
			)
			i++
		} else {
			break
		}
	}
	log.Printf("candidates: %#v\n", candidates)
	if len(candidates) == 0 {
		return nil, nil
	} else {
		// return list(sorted(candidates, key=lambda item: item[1]))[0]
		sort.Slice(candidates, func(i, j int) bool {
			return candidates[i].Amount < candidates[j].Amount
		})
		return candidates[0], nil
	}
}

func Resolve(db *ReadOnlyDBColumnFamily, url string) *ExpandedResolveResult {
	var res = &ExpandedResolveResult{
		Stream:          nil,
		Channel:         nil,
		Repost:          nil,
		RepostedChannel: nil,
	}

	var channel *PathSegment = nil
	var stream *PathSegment = nil
	parsed, err := lbryurl.Parse(url, false)
	if err != nil {
		res.Stream = &optionalResolveResultOrError{
			err: &ResolveError{err},
		}
		return res
	}

	log.Printf("parsed: %#v\n", parsed)

	// has stream in channel
	if strings.Compare(parsed.StreamName, "") != 0 && strings.Compare(parsed.ClaimName, "") != 0 {
		channel = &PathSegment{
			name:        parsed.ClaimName,
			claimId:     parsed.ChannelClaimId,
			amountOrder: parsed.PrimaryBidPosition,
		}
		stream = &PathSegment{
			name:        parsed.StreamName,
			claimId:     parsed.StreamClaimId,
			amountOrder: parsed.SecondaryBidPosition,
		}
	} else if strings.Compare(parsed.ClaimName, "") != 0 {
		channel = &PathSegment{
			name:        parsed.ClaimName,
			claimId:     parsed.ChannelClaimId,
			amountOrder: parsed.PrimaryBidPosition,
		}
	} else if strings.Compare(parsed.StreamName, "") != 0 {
		stream = &PathSegment{
			name:        parsed.StreamName,
			claimId:     parsed.StreamClaimId,
			amountOrder: parsed.SecondaryBidPosition,
		}
	}

	log.Printf("channel: %#v\n", channel)
	log.Printf("stream: %#v\n", stream)

	var resolvedChannel *ResolveResult = nil
	var resolvedStream *ResolveResult = nil
	if channel != nil {
		resolvedChannel, err = ResolveParsedUrl(db, channel)
		if err != nil {
			res.Channel = &optionalResolveResultOrError{
				err: &ResolveError{err},
			}
			return res
		} else if resolvedChannel == nil {
			res.Channel = &optionalResolveResultOrError{
				err: &ResolveError{fmt.Errorf("could not find channel in \"%s\"", url)},
			}
			return res
		}
	}
	log.Printf("resolvedChannel: %#v\n", resolvedChannel)
	log.Printf("resolvedChannel.TxHash: %s\n", hex.EncodeToString(resolvedChannel.TxHash))
	log.Printf("resolvedChannel.ClaimHash: %s\n", hex.EncodeToString(resolvedChannel.ClaimHash))
	log.Printf("resolvedChannel.ChannelHash: %s\n", hex.EncodeToString(resolvedChannel.ChannelHash))
	log.Printf("stream %#v\n", stream)
	if stream != nil {
		if resolvedChannel != nil {
			streamClaim, err := ResolveClaimInChannel(db, resolvedChannel.ClaimHash, stream.Normalized())
			log.Printf("streamClaim %#v\n", streamClaim)
			log.Printf("streamClaim.ClaimHash: %s\n", hex.EncodeToString(streamClaim.ClaimHash))
			log.Printf("streamClaim.ChannelHash: %s\n", hex.EncodeToString(streamClaim.ChannelHash))
			// TODO: Confirm error case
			if err != nil {
				res.Stream = &optionalResolveResultOrError{
					err: &ResolveError{err},
				}
				return res
			}
			if streamClaim != nil {
				resolvedStream, err = FsGetClaimByHash(db, streamClaim.ClaimHash)
				// TODO: Confirm error case
				if err != nil {
					res.Stream = &optionalResolveResultOrError{
						err: &ResolveError{err},
					}
					return res
				}
			}
		} else {
			resolvedStream, err = ResolveParsedUrl(db, stream)
			// TODO: Confirm error case
			if err != nil {
				res.Stream = &optionalResolveResultOrError{
					err: &ResolveError{err},
				}
				return res
			}
			if channel == nil && resolvedChannel == nil && resolvedStream != nil && len(resolvedStream.ChannelHash) > 0 {
				resolvedChannel, err = FsGetClaimByHash(db, resolvedStream.ChannelHash)
				// TODO: Confirm error case
				if err != nil {
					res.Channel = &optionalResolveResultOrError{
						err: &ResolveError{err},
					}
					return res
				}
			}
		}
		if resolvedStream == nil {
			res.Stream = &optionalResolveResultOrError{
				err: &ResolveError{fmt.Errorf("could not find stream in \"%s\"", url)},
			}
			return res
		}
	}

	// Getting blockers and filters
	var repost *ResolveResult = nil
	var repostedChannel *ResolveResult = nil
	log.Printf("about to get blockers and filters: %#v, %#v\n", resolvedChannel, resolvedStream)

	if resolvedStream != nil || resolvedChannel != nil {
		var claim *ResolveResult = nil
		var claimHash []byte = nil
		var respostedClaimHash []byte = nil
		var blockerHash []byte = nil
		if resolvedStream != nil {
			claim = resolvedStream
			claimHash = resolvedStream.ClaimHash
			respostedClaimHash = resolvedStream.RepostedClaimHash
		} else {
			claim = resolvedChannel
			claimHash = resolvedChannel.ClaimHash
		}
		blockerHash, _, err = GetBlockerHash(db, claimHash, respostedClaimHash, claim.ChannelHash)
		log.Printf("blockerHash: %s\n", hex.EncodeToString(blockerHash))
		if err != nil {
			res.Channel = &optionalResolveResultOrError{
				err: &ResolveError{err},
			}
			return res
		}
		if blockerHash != nil {
			reasonRow, err := FsGetClaimByHash(db, blockerHash)
			if err != nil {
				res.Channel = &optionalResolveResultOrError{
					err: &ResolveError{err},
				}
				return res
			}
			res.Channel = &optionalResolveResultOrError{
				err: &ResolveError{fmt.Errorf("%s, %v, %v", url, blockerHash, reasonRow)},
			}
			return res
		}
		if claim.RepostedClaimHash != nil {
			repost, err = FsGetClaimByHash(db, claim.RepostedClaimHash)
			if err != nil {
				res.Channel = &optionalResolveResultOrError{
					err: &ResolveError{err},
				}
				return res
			}
			if repost != nil && repost.ChannelHash != nil && repost.SignatureValid {
				repostedChannel, err = FsGetClaimByHash(db, repost.ChannelHash)
				if err != nil {
					res.Channel = &optionalResolveResultOrError{
						err: &ResolveError{err},
					}
					return res
				}
			}
		}
	}

	res.Channel = &optionalResolveResultOrError{
		res: resolvedChannel,
	}
	res.Stream = &optionalResolveResultOrError{
		res: resolvedStream,
	}
	res.Repost = &optionalResolveResultOrError{
		res: repost,
	}
	res.RepostedChannel = &optionalResolveResultOrError{
		res: repostedChannel,
	}

	log.Printf("parsed: %+v\n", parsed)
	return res
}

//
// Iterators / db construction functions
//

func (o *IterOptions) StopIteration(key []byte) bool {
	if key == nil {
		return false
	}

	maxLen := int(math.Min(float64(len(key)), float64(len(o.Stop))))
	if o.Stop != nil &&
		(bytes.HasPrefix(key, o.Stop) || bytes.Compare(o.Stop, key[:maxLen]) < 0) {
		return true
	} else if o.Start != nil &&
		bytes.Compare(o.Start, key[:len(o.Start)]) > 0 {
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
