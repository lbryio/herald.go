package db

import (
	"bytes"
	"encoding/hex"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/lbryio/hub/db/prefixes"
	"github.com/lbryio/lbry.go/v2/extras/util"
	lbryurl "github.com/lbryio/lbry.go/v2/url"
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

	normalizedName := util.NormalizeName(name)
	controllingClaim, err := GetControllingClaim(db, normalizedName)
	if err != nil {
		return nil, err
	}

	txHash, err := GetTxHash(db, txNum)
	if err != nil {
		return nil, err
	}

	var txCounts []interface{}
	txCounts = db.TxCounts.GetSlice()
	txCounts = txCounts[:db.TxCounts.Len()]
	height := BisectRight(txCounts, txNum)
	createdHeight := BisectRight(txCounts, rootTxNum)
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
		// max short id length
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
	handle, err := EnsureHandle(db, prefixes.ChannelToClaim)
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
	var res = NewExpandedResolveResult()

	var channel *PathSegment = nil
	var stream *PathSegment = nil
	parsed, err := lbryurl.Parse(url, false)

	log.Printf("parsed: %#v", parsed)

	if err != nil {
		res.Stream = &optionalResolveResultOrError{
			err: &ResolveError{err},
		}
		return res
	}

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
			if streamClaim != nil {
				log.Printf("streamClaim.ClaimHash: %s\n", hex.EncodeToString(streamClaim.ClaimHash))
				log.Printf("streamClaim.ChannelHash: %s\n", hex.EncodeToString(streamClaim.ChannelHash))
			}
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

	log.Printf("parsed: %#v\n", parsed)
	return res
}
