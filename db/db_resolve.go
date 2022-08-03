package db

// db_resolve.go contains functions relevant to resolving a claim.

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/lbryio/herald.go/db/prefixes"
	"github.com/lbryio/herald.go/internal"
	pb "github.com/lbryio/herald.go/protobuf/go"
	lbryurl "github.com/lbryio/lbry.go/v3/url"
	log "github.com/sirupsen/logrus"
)

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

	normalizedName := internal.NormalizeName(name)
	controllingClaim, err := db.GetControllingClaim(normalizedName)
	if err != nil {
		return nil, err
	}

	txHash, err := db.GetTxHash(txNum)
	if err != nil {
		return nil, err
	}

	height, createdHeight := db.TxCounts.TxCountsBisectRight(txNum, rootTxNum)
	lastTakeoverHeight := controllingClaim.Height

	expirationHeight := GetExpirationHeight(height)

	supportAmount, err := db.GetSupportAmount(claimHash)
	if err != nil {
		return nil, err
	}

	claimToTxo, err := db.GetCachedClaimTxo(claimHash, true)
	if err != nil {
		return nil, err
	}
	claimAmount := claimToTxo.Amount

	effectiveAmount, err := db.GetEffectiveAmount(claimHash, false)
	if err != nil {
		return nil, err
	}

	channelHash, err := db.GetChannelForClaim(claimHash, txNum, position)
	if err != nil {
		return nil, err
	}

	repostedClaimHash, err := db.GetRepost(claimHash)
	if err != nil {
		return nil, err
	}

	var repostTxHash []byte
	var repostTxPostition uint16
	var repostHeight uint32

	if repostedClaimHash != nil {
		repostTxo, err := db.GetCachedClaimTxo(repostedClaimHash, true)
		if err != nil {
			return nil, err
		}
		if repostTxo != nil {
			repostTxHash, err = db.GetTxHash(repostTxo.TxNum)
			if err != nil {
				return nil, err
			}
			repostTxPostition = repostTxo.Position
			repostHeight, _ = db.TxCounts.TxCountsBisectRight(repostTxo.TxNum, rootTxNum)
		}
	}

	shortUrl, err := db.GetShortClaimIdUrl(name, normalizedName, claimHash, txNum, rootPosition)
	if err != nil {
		return nil, err
	}

	var canonicalUrl string = shortUrl
	claimsInChannel, err := db.GetClaimsInChannelCount(claimHash)
	if err != nil {
		return nil, err
	}

	var channelTxHash []byte
	var channelTxPostition uint16
	var channelHeight uint32

	if channelHash != nil {
		// Ignore error because we already have this set if this doesn't work
		channelVals, _ := db.GetCachedClaimTxo(channelHash, true)
		log.Printf("channelVals: %#v\n", channelVals)
		if channelVals != nil {
			channelShortUrl, _ := db.GetShortClaimIdUrl(
				channelVals.Name,
				channelVals.NormalizedName(),
				channelHash, channelVals.RootTxNum,
				channelVals.RootPosition,
			)
			canonicalUrl = fmt.Sprintf("%s/%s", channelShortUrl, shortUrl)
			channelTxHash, err = db.GetTxHash(channelVals.TxNum)
			if err != nil {
				return nil, err
			}
			channelTxPostition = channelVals.Position
			channelHeight, _ = db.TxCounts.TxCountsBisectRight(channelVals.TxNum, rootTxNum)
		}
	}

	reposted, err := db.GetRepostedCount(claimHash)
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
		RepostTxHash:       repostTxHash,
		RepostTxPostition:  repostTxPostition,
		RepostHeight:       repostHeight,
		ChannelTxHash:      channelTxHash,
		ChannelTxPostition: channelTxPostition,
		ChannelHeight:      channelHeight,
	}, nil
}

func (db *ReadOnlyDBColumnFamily) ResolveParsedUrl(parsed *PathSegment) (*ResolveResult, error) {
	normalizedName := internal.NormalizeName(parsed.name)
	if (parsed.amountOrder == -1 && parsed.claimId == "") || parsed.amountOrder == 1 {
		log.Warn("Resolving claim by name")
		ch := db.ControllingClaimIter()
		for kv := range ch {
			key := kv.Key.(*prefixes.ClaimTakeoverKey)
			val := kv.Value.(*prefixes.ClaimTakeoverValue)
			log.Warnf("ClaimTakeoverKey: %#v", key)
			log.Warnf("ClaimTakeoverValue: %#v", val)
		}
		controlling, err := db.GetControllingClaim(normalizedName)
		log.Warnf("controlling: %#v", controlling)
		log.Warnf("err: %#v", err)
		if err != nil {
			return nil, err
		}
		if controlling == nil {
			return nil, nil
		}
		return db.FsGetClaimByHash(controlling.ClaimHash)
	}

	var amountOrder int = int(math.Max(float64(parsed.amountOrder), 1))

	log.Println("amountOrder:", amountOrder)

	// Resolve by claimId
	if parsed.claimId != "" {
		if len(parsed.claimId) == 40 {
			claimHash, err := hex.DecodeString(parsed.claimId)
			if err != nil {
				return nil, err
			}

			// Maybe don't use caching version, when I actually implement the cache
			claimTxo, err := db.GetCachedClaimTxo(claimHash, true)
			if err != nil {
				return nil, err
			}

			if claimTxo == nil || claimTxo.NormalizedName() != normalizedName {
				return nil, nil
			}

			activation, err := db.GetActivation(claimTxo.TxNum, claimTxo.Position)
			if err != nil {
				return nil, err
			}

			log.Warn("claimTxo.ChannelSignatureIsValid:", claimTxo.ChannelSignatureIsValid)

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
		// max short id length
		var j int = 10
		if len(parsed.claimId) < j {
			j = len(parsed.claimId)
		}

		ch := db.ClaimShortIdIter(normalizedName, parsed.claimId[:j])
		row := <-ch
		if row == nil {
			return nil, nil
		}

		key := row.Key.(*prefixes.ClaimShortIDKey)
		claimTxo := row.Value.(*prefixes.ClaimShortIDValue)

		fullClaimHash, err := db.GetCachedClaimHash(claimTxo.TxNum, claimTxo.Position)
		if err != nil {
			return nil, err
		}

		c, err := db.GetCachedClaimTxo(fullClaimHash.ClaimHash, true)
		if err != nil {
			return nil, err
		}

		nonNormalizedName := c.Name
		signatureIsValid := c.ChannelSignatureIsValid
		activation, err := db.GetActivation(claimTxo.TxNum, claimTxo.Position)

		if err != nil {
			return nil, err
		}

		log.Warn("signatureIsValid:", signatureIsValid)

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

	// Resolve by amount ordering
	log.Warn("resolving by amount ordering")
	ch := db.EffectiveAmountNameIter(normalizedName)
	var i = 0
	for kv := range ch {
		if i+1 < amountOrder {
			i++
			continue
		}
		key := kv.Key.(*prefixes.EffectiveAmountKey)
		claimVal := kv.Value.(*prefixes.EffectiveAmountValue)
		claimTxo, err := db.GetCachedClaimTxo(claimVal.ClaimHash, true)
		if err != nil {
			return nil, err
		}

		activation, err := db.GetActivation(key.TxNum, key.Position)
		if err != nil {
			return nil, err
		}

		return PrepareResolveResult(
			db,
			key.TxNum,
			key.Position,
			claimVal.ClaimHash,
			key.NormalizedName,
			claimTxo.RootTxNum,
			claimTxo.RootPosition,
			activation,
			claimTxo.ChannelSignatureIsValid,
		)
	}

	return nil, nil
}

func (db *ReadOnlyDBColumnFamily) ResolveClaimInChannel(channelHash []byte, normalizedName string) (*ResolveResult, error) {
	handle, err := db.EnsureHandle(prefixes.ChannelToClaim)
	if err != nil {
		return nil, err
	}

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
		effectiveAmount, err := db.GetEffectiveAmount(stream.ClaimHash, false)
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

func (db *ReadOnlyDBColumnFamily) Resolve(url string) *ExpandedResolveResult {
	var res = NewExpandedResolveResult()

	var channel *PathSegment = nil
	var stream *PathSegment = nil
	parsed, err := lbryurl.Parse(url, false)

	log.Warnf("parsed: %#v", parsed)

	if err != nil {
		log.Warn("lbryurl.Parse:", err)
		res.Stream = &optionalResolveResultOrError{
			err: &ResolveError{Error: err},
		}
		return res
	}

	// has stream in channel
	if strings.Compare(parsed.StreamName, "") != 0 && strings.Compare(parsed.ChannelName, "") != 0 {
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
	} else if parsed.IsChannelUrl() {
		channel = &PathSegment{
			name:        parsed.ClaimName,
			claimId:     parsed.ChannelClaimId,
			amountOrder: parsed.PrimaryBidPosition,
		}
	} else if strings.Compare(parsed.StreamName, "") != 0 {
		stream = &PathSegment{
			name:        parsed.StreamName,
			claimId:     parsed.StreamClaimId,
			amountOrder: parsed.PrimaryBidPosition,
		}
	}

	log.Printf("channel: %#v\n", channel)
	log.Printf("stream: %#v\n", stream)

	var resolvedChannel *ResolveResult = nil
	var resolvedStream *ResolveResult = nil
	if channel != nil {
		resolvedChannel, err = db.ResolveParsedUrl(channel)
		if err != nil {
			res.Channel = &optionalResolveResultOrError{
				err: &ResolveError{Error: err},
			}
			return res
		} else if resolvedChannel == nil {
			res.Channel = &optionalResolveResultOrError{
				err: &ResolveError{
					Error:     fmt.Errorf("Could not find claim at \"%s\".", url),
					ErrorType: uint8(pb.Error_NOT_FOUND),
				},
			}
			return res
		}
	}
	if resolvedChannel != nil {
		log.Printf("resolvedChannel: %#v\n", resolvedChannel)
		log.Printf("resolvedChannel.TxHash: %s\n", hex.EncodeToString(resolvedChannel.TxHash))
		log.Printf("resolvedChannel.ClaimHash: %s\n", hex.EncodeToString(resolvedChannel.ClaimHash))
		log.Printf("resolvedChannel.ChannelHash: %s\n", hex.EncodeToString(resolvedChannel.ChannelHash))
	}
	if stream != nil {
		if resolvedChannel != nil {
			streamClaim, err := db.ResolveClaimInChannel(resolvedChannel.ClaimHash, stream.Normalized())
			log.Printf("streamClaim %#v\n", streamClaim)
			if streamClaim != nil {
				log.Printf("streamClaim.ClaimHash: %s\n", hex.EncodeToString(streamClaim.ClaimHash))
				log.Printf("streamClaim.ChannelHash: %s\n", hex.EncodeToString(streamClaim.ChannelHash))
			}
			// TODO: Confirm error case
			if err != nil {
				res.Stream = &optionalResolveResultOrError{
					err: &ResolveError{Error: err},
				}
				return res
			}

			if streamClaim != nil {
				resolvedStream, err = db.FsGetClaimByHash(streamClaim.ClaimHash)
				// TODO: Confirm error case
				if err != nil {
					res.Stream = &optionalResolveResultOrError{
						err: &ResolveError{Error: err},
					}
					return res
				}
			}
		} else {
			resolvedStream, err = db.ResolveParsedUrl(stream)
			// TODO: Confirm error case
			if err != nil {
				res.Stream = &optionalResolveResultOrError{
					err: &ResolveError{Error: err},
				}
				return res
			}
			if channel == nil && resolvedChannel == nil && resolvedStream != nil && len(resolvedStream.ChannelHash) > 0 {
				resolvedChannel, err = db.FsGetClaimByHash(resolvedStream.ChannelHash)
				// TODO: Confirm error case
				if err != nil {
					res.Channel = &optionalResolveResultOrError{
						err: &ResolveError{Error: err},
					}
					return res
				}
			}
		}
		if resolvedStream == nil {
			res.Stream = &optionalResolveResultOrError{
				err: &ResolveError{
					Error:     fmt.Errorf("Could not find claim at \"%s\".", url),
					ErrorType: uint8(pb.Error_NOT_FOUND),
				},
			}
			return res
		}
	}

	// Getting blockers and filters
	var repost *ResolveResult = nil
	var repostedChannel *ResolveResult = nil
	if resolvedChannel != nil && resolvedStream != nil {
		log.Printf("about to get blockers and filters: %#v, %#v\n", resolvedChannel, resolvedStream)
	}

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
		blockerHash, _, err = db.GetBlockerHash(claimHash, respostedClaimHash, claim.ChannelHash)
		log.Printf("blockerHash: %s\n", hex.EncodeToString(blockerHash))
		if err != nil {
			res.Channel = &optionalResolveResultOrError{
				err: &ResolveError{Error: err},
			}
			return res
		}
		if blockerHash != nil {
			reasonRow, err := db.FsGetClaimByHash(blockerHash)
			if err != nil {
				res.Channel = &optionalResolveResultOrError{
					err: &ResolveError{Error: err},
				}
				return res
			}
			res.Channel = &optionalResolveResultOrError{
				err: &ResolveError{Error: fmt.Errorf("%s, %v, %v", url, blockerHash, reasonRow)},
			}
			return res
		}
		if claim.RepostedClaimHash != nil {
			repost, err = db.FsGetClaimByHash(claim.RepostedClaimHash)
			if err != nil {
				res.Channel = &optionalResolveResultOrError{
					err: &ResolveError{Error: err},
				}
				return res
			}
			if repost != nil && repost.ChannelHash != nil && repost.SignatureValid {
				repostedChannel, err = db.FsGetClaimByHash(repost.ChannelHash)
				if err != nil {
					res.Channel = &optionalResolveResultOrError{
						err: &ResolveError{Error: err},
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

	log.Warnf("leaving Resolve, parsed: %#v\n", parsed)
	log.Warnf("leaving Resolve, res: %s\n", res)
	return res
}
