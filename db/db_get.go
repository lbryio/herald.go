package db

// db_get.go contains the basic access functions to the database.

import (
	"encoding/hex"
	"fmt"
	"log"

	"github.com/lbryio/hub/db/prefixes"
	"github.com/linxGnu/grocksdb"
)

func GetExpirationHeight(lastUpdatedHeight uint32) uint32 {
	return GetExpirationHeightFull(lastUpdatedHeight, false)
}

func GetExpirationHeightFull(lastUpdatedHeight uint32, extended bool) uint32 {
	if extended {
		return lastUpdatedHeight + ExtendedClaimExpirationTime
	}
	if lastUpdatedHeight < ExtendedClaimExpirationForkHeight {
		return lastUpdatedHeight + OriginalClaimExpirationTime
	}
	return lastUpdatedHeight + ExtendedClaimExpirationTime
}

// EnsureHandle is a helper function to ensure that the db has a handle to the given column family.
func (db *ReadOnlyDBColumnFamily) EnsureHandle(prefix byte) (*grocksdb.ColumnFamilyHandle, error) {
	cfName := string(prefix)
	handle := db.Handles[cfName]
	if handle == nil {
		return nil, fmt.Errorf("%s handle not found", cfName)
	}
	return handle, nil
}

func (db *ReadOnlyDBColumnFamily) GetBlockHash(height uint32) ([]byte, error) {
	handle, err := db.EnsureHandle(prefixes.BlockHash)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewBlockHashKey(height)
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
	if err != nil {
		return nil, err
	} else if slice.Size() == 0 {
		return nil, err
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	return rawValue, nil
}

func (db *ReadOnlyDBColumnFamily) GetHeader(height uint32) ([]byte, error) {
	handle, err := db.EnsureHandle(prefixes.Header)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewHeaderKey(height)
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
	if err != nil {
		return nil, err
	} else if slice.Size() == 0 {
		return nil, err
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	return rawValue, nil
}

func (db *ReadOnlyDBColumnFamily) GetStreamsAndChannelRepostedByChannelHashes(reposterChannelHashes [][]byte) (map[string][]byte, map[string][]byte, error) {
	handle, err := db.EnsureHandle(prefixes.ChannelToClaim)
	if err != nil {
		return nil, nil, err
	}

	streams := make(map[string][]byte)
	channels := make(map[string][]byte)

	for _, reposterChannelHash := range reposterChannelHashes {
		key := prefixes.NewChannelToClaimKeyWHash(reposterChannelHash)
		rawKeyPrefix := prefixes.ChannelToClaimKeyPackPartial(key, 1)
		options := NewIterateOptions().WithCfHandle(handle).WithPrefix(rawKeyPrefix)
		options = options.WithIncludeKey(false).WithIncludeValue(true)
		ch := IterCF(db.DB, options)
		// for stream := range Iterate(db.DB, prefixes.ChannelToClaim, []byte{reposterChannelHash}, false) {
		for stream := range ch {
			value := stream.Value.(*prefixes.ChannelToClaimValue)
			repost, err := db.GetRepost(value.ClaimHash)
			if err != nil {
				return nil, nil, err
			}
			if repost != nil {
				txo, err := db.GetClaimTxo(repost)
				if err != nil {
					return nil, nil, err
				}
				if txo != nil {
					repostStr := hex.EncodeToString(repost)
					if normalName := txo.NormalizedName(); len(normalName) > 0 && normalName[0] == '@' {
						channels[repostStr] = reposterChannelHash
					} else {
						streams[repostStr] = reposterChannelHash
					}
				}
			}
		}
	}

	return streams, channels, nil
}

func (db *ReadOnlyDBColumnFamily) GetClaimsInChannelCount(channelHash []byte) (uint32, error) {
	handle, err := db.EnsureHandle(prefixes.ChannelCount)
	if err != nil {
		return 0, err
	}

	key := prefixes.NewChannelCountKey(channelHash)
	rawKey := key.PackKey()

	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
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

func (db *ReadOnlyDBColumnFamily) GetShortClaimIdUrl(name string, normalizedName string, claimHash []byte, rootTxNum uint32, rootPosition uint16) (string, error) {
	prefix := []byte{prefixes.ClaimShortIdPrefix}
	handle, err := db.EnsureHandle(prefixes.ClaimShortIdPrefix)
	if err != nil {
		return "", err
	}

	claimId := hex.EncodeToString(claimHash)
	claimIdLen := len(claimId)
	for prefixLen := 0; prefixLen < 10; prefixLen++ {
		var j int = prefixLen + 1
		if j > claimIdLen {
			j = claimIdLen
		}
		partialClaimId := claimId[:j]
		partialKey := prefixes.NewClaimShortIDKey(normalizedName, partialClaimId)
		log.Printf("partialKey: %#v\n", partialKey)
		keyPrefix := prefixes.ClaimShortIDKeyPackPartial(partialKey, 2)
		// Prefix and handle
		options := NewIterateOptions().WithPrefix(prefix).WithCfHandle(handle)
		// Start and stop bounds
		options = options.WithStart(keyPrefix)
		// Don't include the key
		options = options.WithIncludeValue(false)

		ch := IterCF(db.DB, options)
		row := <-ch
		if row == nil {
			continue
		}

		key := row.Key.(*prefixes.ClaimShortIDKey)
		if key.RootTxNum == rootTxNum && key.RootPosition == rootPosition {
			return fmt.Sprintf("%s#%s", name, key.PartialClaimId), nil
		}
	}
	return "", nil
}

func (db *ReadOnlyDBColumnFamily) GetRepost(claimHash []byte) ([]byte, error) {
	handle, err := db.EnsureHandle(prefixes.Repost)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewRepostKey(claimHash)
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
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

func (db *ReadOnlyDBColumnFamily) GetRepostedCount(claimHash []byte) (int, error) {
	handle, err := db.EnsureHandle(prefixes.RepostedClaim)
	if err != nil {
		return 0, err
	}

	key := prefixes.NewRepostedKey(claimHash)
	keyPrefix := prefixes.RepostedKeyPackPartial(key, 1)
	// Prefix and handle
	options := NewIterateOptions().WithPrefix(keyPrefix).WithCfHandle(handle)
	// Start and stop bounds
	// options = options.WithStart(keyPrefix)
	// Don't include the key
	options = options.WithIncludeValue(false)

	var i int = 0
	ch := IterCF(db.DB, options)

	for range ch {
		i++
	}

	return i, nil
}

func (db *ReadOnlyDBColumnFamily) GetChannelForClaim(claimHash []byte, txNum uint32, position uint16) ([]byte, error) {
	handle, err := db.EnsureHandle(prefixes.ClaimToChannel)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewClaimToChannelKey(claimHash, txNum, position)
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
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

func (db *ReadOnlyDBColumnFamily) GetActiveAmount(claimHash []byte, txoType uint8, height uint32) (uint64, error) {
	handle, err := db.EnsureHandle(prefixes.ActiveAmount)
	if err != nil {
		return 0, err
	}

	startKey := prefixes.NewActiveAmountKey(claimHash, txoType, 0)
	endKey := prefixes.NewActiveAmountKey(claimHash, txoType, height)

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

func (db *ReadOnlyDBColumnFamily) GetEffectiveAmount(claimHash []byte, supportOnly bool) (uint64, error) {
	supportAmount, err := db.GetActiveAmount(claimHash, prefixes.ActivatedSupportTXOType, db.Height+1)
	if err != nil {
		return 0, err
	}

	if supportOnly {
		return supportAmount, nil
	}

	activationAmount, err := db.GetActiveAmount(claimHash, prefixes.ActivateClaimTXOType, db.Height+1)
	if err != nil {
		return 0, err
	}

	return activationAmount + supportAmount, nil
}

func (db *ReadOnlyDBColumnFamily) GetSupportAmount(claimHash []byte) (uint64, error) {
	handle, err := db.EnsureHandle(prefixes.SupportAmount)
	if err != nil {
		return 0, err
	}

	key := prefixes.NewSupportAmountKey(claimHash)
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
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

func (db *ReadOnlyDBColumnFamily) GetTxHash(txNum uint32) ([]byte, error) {
	// TODO: caching
	handle, err := db.EnsureHandle(prefixes.TxHash)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewTxHashKey(txNum)
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
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

func (db *ReadOnlyDBColumnFamily) GetActivation(txNum uint32, postition uint16) (uint32, error) {
	return db.GetActivationFull(txNum, postition, false)
}

func (db *ReadOnlyDBColumnFamily) GetActivationFull(txNum uint32, postition uint16, isSupport bool) (uint32, error) {
	var typ uint8

	handle, err := db.EnsureHandle(prefixes.ActivatedClaimAndSupport)
	if err != nil {
		return 0, err
	}

	if isSupport {
		typ = prefixes.ActivatedSupportTXOType
	} else {
		typ = prefixes.ActivateClaimTXOType
	}

	key := prefixes.NewActivationKey(typ, txNum, postition)
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
	if err != nil {
		return 0, err
	}
	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.ActivationValueUnpack(rawValue)
	// Does this need to explicitly return an int64, in case the uint32 overflows the max of an int?
	return value.Height, nil
}

func (db *ReadOnlyDBColumnFamily) GetClaimTxo(claim []byte) (*prefixes.ClaimToTXOValue, error) {
	return db.GetCachedClaimTxo(claim, false)
}

func (db *ReadOnlyDBColumnFamily) GetCachedClaimTxo(claim []byte, useCache bool) (*prefixes.ClaimToTXOValue, error) {
	// TODO: implement cache
	handle, err := db.EnsureHandle(prefixes.ClaimToTXO)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewClaimToTXOKey(claim)
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
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

func (db *ReadOnlyDBColumnFamily) ControllingClaimIter() <-chan *prefixes.PrefixRowKV {
	handle, err := db.EnsureHandle(prefixes.ClaimTakeover)
	if err != nil {
		return nil
	}

	key := prefixes.NewClaimTakeoverKey("")
	var rawKeyPrefix []byte = nil
	rawKeyPrefix = prefixes.ClaimTakeoverKeyPackPartial(key, 0)
	options := NewIterateOptions().WithCfHandle(handle).WithPrefix(rawKeyPrefix)
	options = options.WithIncludeValue(true) //.WithIncludeStop(true)
	ch := IterCF(db.DB, options)
	return ch
}

func (db *ReadOnlyDBColumnFamily) GetControllingClaim(name string) (*prefixes.ClaimTakeoverValue, error) {
	handle, err := db.EnsureHandle(prefixes.ClaimTakeover)
	if err != nil {
		return nil, err
	}

	log.Println(name)
	key := prefixes.NewClaimTakeoverKey(name)
	rawKey := key.PackKey()
	log.Println(hex.EncodeToString(rawKey))
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
	log.Printf("slice: %#v", slice)
	log.Printf("err: %#v", err)

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

func (db *ReadOnlyDBColumnFamily) FsGetClaimByHash(claimHash []byte) (*ResolveResult, error) {
	claim, err := db.GetCachedClaimTxo(claimHash, true)
	if err != nil {
		return nil, err
	}

	activation, err := db.GetActivation(claim.TxNum, claim.Position)
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

func (db *ReadOnlyDBColumnFamily) GetTxCount(height uint32) (*prefixes.TxCountValue, error) {
	handle, err := db.EnsureHandle(prefixes.TxCount)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewTxCountKey(height)
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
	if err != nil {
		return nil, err
	}
	if slice.Size() == 0 {
		return nil, nil
	}

	rawValue := make([]byte, len(slice.Data()))
	copy(rawValue, slice.Data())
	value := prefixes.TxCountValueUnpack(rawValue)
	return value, nil
}

func (db *ReadOnlyDBColumnFamily) GetDBState() (*prefixes.DBStateValue, error) {
	handle, err := db.EnsureHandle(prefixes.DBState)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewDBStateKey()
	rawKey := key.PackKey()
	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
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

func (db *ReadOnlyDBColumnFamily) EffectiveAmountNameIter(normalizedName string) <-chan *prefixes.PrefixRowKV {
	handle, err := db.EnsureHandle(prefixes.EffectiveAmount)
	if err != nil {
		return nil
	}

	key := prefixes.NewEffectiveAmountKey(normalizedName)
	var rawKeyPrefix []byte = nil
	rawKeyPrefix = prefixes.EffectiveAmountKeyPackPartial(key, 1)
	options := NewIterateOptions().WithCfHandle(handle).WithPrefix(rawKeyPrefix)
	options = options.WithIncludeValue(true) //.WithIncludeStop(true)
	ch := IterCF(db.DB, options)
	return ch
}

func (db *ReadOnlyDBColumnFamily) ClaimShortIdIter(normalizedName string, claimId string) <-chan *prefixes.PrefixRowKV {
	handle, err := db.EnsureHandle(prefixes.ClaimShortIdPrefix)
	if err != nil {
		return nil
	}
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

func (db *ReadOnlyDBColumnFamily) GetCachedClaimHash(txNum uint32, position uint16) (*prefixes.TXOToClaimValue, error) {
	// TODO: implement cache
	handle, err := db.EnsureHandle(prefixes.TXOToClaim)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewTXOToClaimKey(txNum, position)
	rawKey := key.PackKey()

	slice, err := db.DB.GetCF(db.Opts, handle, rawKey)
	defer slice.Free()
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
func (db *ReadOnlyDBColumnFamily) GetBlockerHash(claimHash, repostedClaimHash, channelHash []byte) ([]byte, []byte, error) {
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
