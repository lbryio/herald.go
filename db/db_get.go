package db

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
		return lastUpdatedHeight + NExtendedClaimExpirationTime
	}
	if lastUpdatedHeight < NExtendedClaimExpirationForkHeight {
		return lastUpdatedHeight + NOriginalClaimExpirationTime
	}
	return lastUpdatedHeight + NExtendedClaimExpirationTime
}

// EnsureHandle is a helper function to ensure that the db has a handle to the given column family.
func EnsureHandle(db *ReadOnlyDBColumnFamily, prefix byte) (*grocksdb.ColumnFamilyHandle, error) {
	cfName := string(prefix)
	handle := db.Handles[cfName]
	if handle == nil {
		return nil, fmt.Errorf("%s handle not found", cfName)
	}
	return handle, nil
}

//
// DB Get functions
//

func GetClaimsInChannelCount(db *ReadOnlyDBColumnFamily, channelHash []byte) (uint32, error) {
	handle, err := EnsureHandle(db, prefixes.ChannelCount)
	if err != nil {
		return 0, err
	}

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
	handle, err := EnsureHandle(db, prefixes.ClaimShortIdPrefix)
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

func GetRepost(db *ReadOnlyDBColumnFamily, claimHash []byte) ([]byte, error) {
	handle, err := EnsureHandle(db, prefixes.Repost)
	if err != nil {
		return nil, err
	}

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
	handle, err := EnsureHandle(db, prefixes.RepostedClaim)
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

func GetChannelForClaim(db *ReadOnlyDBColumnFamily, claimHash []byte, txNum uint32, position uint16) ([]byte, error) {
	handle, err := EnsureHandle(db, prefixes.ClaimToChannel)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewClaimToChannelKey(claimHash, txNum, position)
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
	handle, err := EnsureHandle(db, prefixes.ActiveAmount)
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
	handle, err := EnsureHandle(db, prefixes.SupportAmount)
	if err != nil {
		return 0, err
	}

	key := prefixes.NewSupportAmountKey(claimHash)
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
	handle, err := EnsureHandle(db, prefixes.TxHash)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewTxHashKey(txNum)
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

	handle, err := EnsureHandle(db, prefixes.ActivatedClaimAndSupport)
	if err != nil {
		return 0, err
	}

	if isSupport {
		typ = prefixes.ACTIVATED_SUPPORT_TXO_TYPE
	} else {
		typ = prefixes.ACTIVATED_CLAIM_TXO_TYPE
	}

	key := prefixes.NewActivationKey(typ, txNum, postition)
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
	handle, err := EnsureHandle(db, prefixes.ClaimToTXO)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewClaimToTXOKey(claim)
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
	handle, err := EnsureHandle(db, prefixes.ClaimTakeover)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewClaimTakeoverKey(name)
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
	handle, err := EnsureHandle(db, prefixes.DBState)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewDBStateKey()
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
	handle, err := EnsureHandle(db, prefixes.ClaimShortIdPrefix)
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

func GetCachedClaimHash(db *ReadOnlyDBColumnFamily, txNum uint32, position uint16) (*prefixes.TXOToClaimValue, error) {
	// TODO: implement cache
	handle, err := EnsureHandle(db, prefixes.TXOToClaim)
	if err != nil {
		return nil, err
	}

	key := prefixes.NewTXOToClaimKey(txNum, position)
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
