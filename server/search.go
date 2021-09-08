package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"reflect"
	"strings"
	"time"

	//"github.com/lbryio/hub/schema"

	"github.com/btcsuite/btcutil/base58"
	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/lbryio/lbry.go/v2/extras/util"
	"github.com/olivere/elastic/v7"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"gopkg.in/karalabe/cookiejar.v1/collections/deque"
)

const DefaultSearchSize = 1000

type record struct {
	Txid               string  `json:"tx_id"`
	Nout               uint32  `json:"tx_nout"`
	Height             uint32  `json:"height"`
	ClaimId            string  `json:"claim_id"`
	ChannelId          string  `json:"channel_id"`
	RepostedClaimId    string  `json:"reposted_claim_id"`
	CensorType         uint32  `json:"censor_type"`
	CensoringChannelId string  `json:"censoring_channel_id"`
	ShortUrl           string  `json:"short_url"`
	CanonicalUrl       string  `json:"canonical_url"`
	IsControlling      bool    `json:"is_controlling"`
	TakeOverHeight     uint32  `json:"last_take_over_height"`
	CreationHeight     uint32  `json:"creation_height"`
	ActivationHeight   uint32  `json:"activation_height"`
	ExpirationHeight   uint32  `json:"expiration_height"`
	ClaimsInChannel    uint32  `json:"claims_in_channel"`
	Reposted           uint32  `json:"repost_count"`
	EffectiveAmount    uint64  `json:"effective_amount"`
	SupportAmount      uint64  `json:"support_amount"`
	TrendingGroup      uint32  `json:"trending_group"`
	TrendingMixed      float32 `json:"trending_mixed"`
	TrendingLocal      float32 `json:"trending_local"`
	TrendingGlobal     float32 `json:"trending_global"`
	Name               string  `json:"name"`
}

type orderField struct {
	Field string
	IsAsc bool
}

func StrArrToInterface(arr []string) []interface{} {
	searchVals := make([]interface{}, len(arr))
	for i := 0; i < len(arr); i++ {
		searchVals[i] = arr[i]
	}
	return searchVals
}

func AddTermsField(q *elastic.BoolQuery, arr []string, name string) *elastic.BoolQuery {
	if len(arr) == 0 {
		return q
	}
	searchVals := StrArrToInterface(arr)
	return q.Must(elastic.NewTermsQuery(name, searchVals...))
}

func AddTermField(q *elastic.BoolQuery, value string, name string) *elastic.BoolQuery {
	if value != "" {
		return q.Must(elastic.NewTermQuery(name, value))
	}
	return q
}

func AddIndividualTermFields(q *elastic.BoolQuery, arr []string, name string, invert bool) *elastic.BoolQuery {
	for _, x := range arr {
		if invert {
			q = q.MustNot(elastic.NewTermQuery(name, x))
		} else {
			q = q.Must(elastic.NewTermQuery(name, x))
		}
	}
	return q
}

func AddRangeField(q *elastic.BoolQuery, rq *pb.RangeField, name string) *elastic.BoolQuery {
	if rq == nil {
		return q
	}

	if len(rq.Value) > 1 {
		if rq.Op != pb.RangeField_EQ {
			return q
		}
		return AddTermsField(q, rq.Value, name)
	}
	if rq.Op == pb.RangeField_EQ {
		return q.Must(elastic.NewTermQuery(name, rq.Value[0]))
	} else if rq.Op == pb.RangeField_LT {
		return q.Must(elastic.NewRangeQuery(name).Lt(rq.Value[0]))
	} else if rq.Op == pb.RangeField_LTE {
		return q.Must(elastic.NewRangeQuery(name).Lte(rq.Value[0]))
	} else if rq.Op == pb.RangeField_GT {
		return q.Must(elastic.NewRangeQuery(name).Gt(rq.Value[0]))
	} else { // pb.RangeField_GTE
		return q.Must(elastic.NewRangeQuery(name).Gte(rq.Value[0]))
	}
}

func AddInvertibleField(q *elastic.BoolQuery, field *pb.InvertibleField, name string) *elastic.BoolQuery {
	if field == nil {
		return q
	}
	searchVals := StrArrToInterface(field.Value)
	if field.Invert {
		q = q.MustNot(elastic.NewTermsQuery(name, searchVals...))
		if name == "channel_id.keyword" {
			q = q.MustNot(elastic.NewTermsQuery("_id", searchVals...))
		}
		return q
	} else {
		return q.Must(elastic.NewTermsQuery(name, searchVals...))
	}
}

// Search /*
// Search logic is as follows:
// 1) Setup query with params given
// 2) Do query with limit of 1000
// 3) remove blocked content (these are returned separately)
// 4) remove duplicates (these are not returned)
// 5) limit claims per channel logic
// 6) get claims referenced by reposts
// 7) get channels references by claims and repost claims
// 8) return streams referenced by repost and all channel referenced in extra_txos
//*/
func (s *Server) Search(ctx context.Context, in *pb.SearchRequest) (*pb.Outputs, error) {
	var from = 0
	var pageSize = 10
	var orderBy []orderField
	var searchIndices []string
	client := s.EsClient
	searchIndices = make([]string, 0, 1)
	searchIndices = append(searchIndices, s.Args.EsIndex)

	q := elastic.NewBoolQuery()

	err := s.checkQuery(in)
	if err != nil {
		return nil, err
	}
	q = s.setupEsQuery(q, in, &pageSize, &from, &orderBy)

	fsc := elastic.NewFetchSourceContext(true).Exclude("description", "title")
	search := client.Search().
		Index(searchIndices...).
		FetchSourceContext(fsc).
		Query(q). // specify the query
		From(0).Size(DefaultSearchSize)

	for _, x := range orderBy {
		search = search.Sort(x.Field, x.IsAsc)
	}

	searchResult, err := search.Do(ctx) // execute
	if err != nil && elastic.IsNotFound(err) {
		log.Println("Index returned 404! Check writer. Index: ", searchIndices)
		return &pb.Outputs{}, nil

	} else if err != nil {
		log.Println("Error executing query: ", err)
		return nil, err
	}

	log.Printf("%s: found %d results in %dms\n", in.Text, len(searchResult.Hits.Hits), searchResult.TookInMillis)

	txos, extraTxos, blocked := s.postProcessResults(ctx, client, searchResult, in, pageSize, from, searchIndices)

	if in.NoTotals {
		return &pb.Outputs{
			Txos:      txos,
			ExtraTxos: extraTxos,
			Offset:    uint32(int64(from) + searchResult.TotalHits()),
			Blocked:   blocked,
		}, nil
	}

	var blockedTotal uint32 = 0
	for _, b := range blocked {
		blockedTotal += b.Count
	}
	return &pb.Outputs{
		Txos:         txos,
		ExtraTxos:    extraTxos,
		Total:        uint32(searchResult.TotalHits()),
		Offset:       uint32(int64(from) + searchResult.TotalHits()),
		Blocked:      blocked,
		BlockedTotal: blockedTotal,
	}, nil
}

func (s *Server) normalizeTag(tag string) string {
	c := cases.Lower(language.English)
	res := s.MultiSpaceRe.ReplaceAll(
		s.WeirdCharsRe.ReplaceAll(
			[]byte(strings.TrimSpace(strings.Replace(c.String(tag), "'", "", -1))),
			[]byte(" ")),
		[]byte(" "))

	return string(res)
}

func (s *Server) cleanTags(tags []string) []string {
	cleanedTags := make([]string, len(tags))
	for i, tag := range tags {
		cleanedTags[i] = s.normalizeTag(tag)
	}
	return cleanedTags
}

func (s *Server) postProcessResults(
	ctx context.Context,
	client *elastic.Client,
	searchResult *elastic.SearchResult,
	in *pb.SearchRequest,
	pageSize int,
	from int,
	searchIndices []string) ([]*pb.Output, []*pb.Output, []*pb.Blocked) {
	var txos []*pb.Output
	var records []*record
	var blockedRecords []*record
	var blocked []*pb.Blocked
	var blockedMap map[string]*pb.Blocked

	records = make([]*record, 0, searchResult.TotalHits())

	var r record
	for _, item := range searchResult.Each(reflect.TypeOf(r)) {
		if t, ok := item.(record); ok {
			records = append(records, &t)
		}
	}

	//printJsonFullResults(searchResult)
	records, blockedRecords, blockedMap = removeBlocked(records)

	if in.RemoveDuplicates {
		records = removeDuplicates(records)
	}

	if in.LimitClaimsPerChannel > 0 {
		records = searchAhead(records, pageSize, int(in.LimitClaimsPerChannel))
	}

	finalLength := int(math.Min(float64(len(records)), float64(pageSize)))
	txos = make([]*pb.Output, 0, finalLength)
	var j = 0
	for i := from; i < from+finalLength && i < len(records) && j < finalLength; i++ {
		t := records[i]
		res := t.recordToOutput()
		txos = append(txos, res)
		j += 1
	}
	//Get claims for reposts
	repostClaims, repostRecords, repostedMap := getClaimsForReposts(ctx, client, records, searchIndices)
	//get all unique channels
	channels, channelMap := getUniqueChannels(append(append(records, repostRecords...), blockedRecords...), client, ctx, searchIndices)
	//add these to extra txos
	extraTxos := append(repostClaims, channels...)

	//Fill in channel / repost data for txos and blocked
	for i, txo := range txos {
		channel, cOk := channelMap[records[i].ChannelId]
		repostClaim, rOk := repostedMap[records[i].RepostedClaimId]
		if cOk {
			txo.GetClaim().Channel = channel
		}
		if rOk {
			txo.GetClaim().Repost = repostClaim
		}
	}

	blocked = make([]*pb.Blocked, 0, len(blockedMap))
	for k, v := range blockedMap {
		if channel, ok := channelMap[k]; ok {
			v.Channel = channel
		}
		blocked = append(blocked, v)
	}

	return txos, extraTxos, blocked
}

func (s *Server) checkQuery(in *pb.SearchRequest) error {
	limit := 2048
	checks := map[string]bool{
		"claim_ids":       in.ClaimId != nil && !in.ClaimId.Invert && len(in.ClaimId.Value) > limit,
		"not_claim_ids":   in.ClaimId != nil && in.ClaimId.Invert && len(in.ClaimId.Value) > limit,
		"channel_ids":     in.ChannelId != nil && !in.ChannelId.Invert && len(in.ChannelId.Value) > limit,
		"not_channel_ids": in.ChannelId != nil && in.ChannelId.Invert && len(in.ChannelId.Value) > limit,
		"not_tags":        len(in.NotTags) > limit,
		"all_tags":        len(in.AllTags) > limit,
		"any_tags":        len(in.AnyTags) > limit,
		"any_languages":   len(in.AnyLanguages) > limit,
	}
	for name, failed := range checks {
		if failed {
			time.Sleep(2) // throttle
			return fmt.Errorf("%s cant have more than %d items", name, limit)
		}
	}
	return nil
}

func (s *Server) setupEsQuery(
	q *elastic.BoolQuery,
	in *pb.SearchRequest,
	pageSize *int,
	from *int,
	orderBy *[]orderField) *elastic.BoolQuery {
	claimTypes := map[string]int{
		"stream":     1,
		"channel":    2,
		"repost":     3,
		"collection": 4,
	}

	streamTypes := map[string]int{
		"video":    1,
		"audio":    2,
		"image":    3,
		"document": 4,
		"binary":   5,
		"model":    6,
	}

	replacements := map[string]string{
		"name":       "normalized_name",
		"txid":       "tx_id",
		"claim_hash": "_id",
	}

	textFields := map[string]bool{
		"author":            true,
		"canonical_url":     true,
		"channel_id":        true,
		"claim_name":        true,
		"description":       true,
		"claim_id":          true,
		"media_type":        true,
		"normalized_name":   true,
		"public_key_bytes":  true,
		"public_key_id":     true,
		"short_url":         true,
		"signature":         true,
		"signature_digest":  true,
		"stream_type":       true,
		"title":             true,
		"tx_id":             true,
		"fee_currency":      true,
		"reposted_claim_id": true,
		"tags":              true,
	}

	if in.IsControlling {
		q = q.Must(elastic.NewTermQuery("is_controlling", in.IsControlling))
	}

	if in.Limit > 0 {
		*pageSize = int(in.Limit)
	}

	if in.Offset > 0 {
		*from = int(in.Offset)
	}

	if len(in.ClaimName) > 0 {
		in.NormalizedName = util.NormalizeName(in.ClaimName)
	}

	if len(in.OrderBy) > 0 {
		for _, x := range in.OrderBy {
			var toAppend string
			var isAsc = false
			if x[0] == '^' {
				isAsc = true
				x = x[1:]
			}
			if _, ok := replacements[x]; ok {
				toAppend = replacements[x]
			} else {
				toAppend = x
			}

			if _, ok := textFields[toAppend]; ok {
				toAppend = toAppend + ".keyword"
			}
			*orderBy = append(*orderBy, orderField{toAppend, isAsc})
		}
	}

	if len(in.ClaimType) > 0 {
		searchVals := make([]interface{}, len(in.ClaimType))
		for i := 0; i < len(in.ClaimType); i++ {
			searchVals[i] = claimTypes[in.ClaimType[i]]
		}
		q = q.Must(elastic.NewTermsQuery("claim_type", searchVals...))
	}

	if len(in.StreamType) > 0 {
		searchVals := make([]interface{}, len(in.StreamType))
		for i := 0; i < len(in.StreamType); i++ {
			searchVals[i] = streamTypes[in.StreamType[i]]
		}
		q = q.Must(elastic.NewTermsQuery("stream_type", searchVals...))
	}

	if in.ClaimId != nil {
		searchVals := StrArrToInterface(in.ClaimId.Value)
		if len(in.ClaimId.Value) == 1 && len(in.ClaimId.Value[0]) < 20 {
			if in.ClaimId.Invert {
				q = q.MustNot(elastic.NewPrefixQuery("claim_id.keyword", in.ClaimId.Value[0]))
			} else {
				q = q.Must(elastic.NewPrefixQuery("claim_id.keyword", in.ClaimId.Value[0]))
			}
		} else {
			if in.ClaimId.Invert {
				q = q.MustNot(elastic.NewTermsQuery("claim_id.keyword", searchVals...))
			} else {
				q = q.Must(elastic.NewTermsQuery("claim_id.keyword", searchVals...))
			}
		}
	}

	if in.PublicKeyId != "" {
		value := hex.EncodeToString(base58.Decode(in.PublicKeyId)[1:21])
		q = q.Must(elastic.NewTermQuery("public_key_id.keyword", value))
	}

	if in.HasChannelSignature {
		q = q.Must(elastic.NewExistsQuery("signature_digest"))
		if in.IsSignatureValid != nil {
			q = q.Must(elastic.NewTermQuery("is_signature_valid", in.IsSignatureValid.Value))
		}
	} else if in.IsSignatureValid != nil {
		q = q.MinimumNumberShouldMatch(1)
		q = q.Should(elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery("signature_digest")))
		q = q.Should(elastic.NewTermQuery("is_signature_valid", in.IsSignatureValid.Value))
	}

	if in.HasSource != nil {
		q = q.MinimumNumberShouldMatch(1)
		isStreamOrRepost := elastic.NewTermsQuery("claim_type", claimTypes["stream"], claimTypes["repost"])
		q = q.Should(elastic.NewBoolQuery().Must(isStreamOrRepost, elastic.NewMatchQuery("has_source", in.HasSource.Value)))
		q = q.Should(elastic.NewBoolQuery().MustNot(isStreamOrRepost))
		q = q.Should(elastic.NewBoolQuery().Must(elastic.NewTermQuery("reposted_claim_type", claimTypes["channel"])))
	}

	if in.TxNout != nil {
		q = q.Must(elastic.NewTermQuery("tx_nout", in.TxNout.Value))
	}

	q = AddTermField(q, in.Author, "author.keyword")
	q = AddTermField(q, in.Title, "title.keyword")
	q = AddTermField(q, in.CanonicalUrl, "canonical_url.keyword")
	q = AddTermField(q, in.ClaimName, "claim_name.keyword")
	q = AddTermField(q, in.Description, "description.keyword")
	q = AddTermsField(q, in.MediaType, "media_type.keyword")
	q = AddTermField(q, in.NormalizedName, "normalized_name.keyword")
	q = AddTermField(q, in.PublicKeyBytes, "public_key_bytes.keyword")
	q = AddTermField(q, in.ShortUrl, "short_url.keyword")
	q = AddTermField(q, in.Signature, "signature.keyword")
	q = AddTermField(q, in.SignatureDigest, "signature_digest.keyword")
	q = AddTermField(q, in.TxId, "tx_id.keyword")
	q = AddTermField(q, in.FeeCurrency, "fee_currency.keyword")
	q = AddTermField(q, in.RepostedClaimId, "reposted_claim_id.keyword")

	q = AddTermsField(q, s.cleanTags(in.AnyTags), "tags.keyword")
	q = AddIndividualTermFields(q, s.cleanTags(in.AllTags), "tags.keyword", false)
	q = AddIndividualTermFields(q, s.cleanTags(in.NotTags), "tags.keyword", true)
	q = AddTermsField(q, in.AnyLanguages, "languages")
	q = AddIndividualTermFields(q, in.AllLanguages, "languages", false)

	q = AddInvertibleField(q, in.ChannelId, "channel_id.keyword")

	q = AddRangeField(q, in.TxPosition, "tx_position")
	q = AddRangeField(q, in.Amount, "amount")
	q = AddRangeField(q, in.Timestamp, "timestamp")
	q = AddRangeField(q, in.CreationTimestamp, "creation_timestamp")
	q = AddRangeField(q, in.Height, "height")
	q = AddRangeField(q, in.CreationHeight, "creation_height")
	q = AddRangeField(q, in.ActivationHeight, "activation_height")
	q = AddRangeField(q, in.ExpirationHeight, "expiration_height")
	q = AddRangeField(q, in.ReleaseTime, "release_time")
	q = AddRangeField(q, in.RepostCount, "repost_count")
	q = AddRangeField(q, in.FeeAmount, "fee_amount")
	q = AddRangeField(q, in.Duration, "duration")
	q = AddRangeField(q, in.CensorType, "censor_type")
	q = AddRangeField(q, in.ChannelJoin, "channel_join")
	q = AddRangeField(q, in.EffectiveAmount, "effective_amount")
	q = AddRangeField(q, in.SupportAmount, "support_amount")
	q = AddRangeField(q, in.TrendingGroup, "trending_group")
	q = AddRangeField(q, in.TrendingMixed, "trending_mixed")
	q = AddRangeField(q, in.TrendingLocal, "trending_local")
	q = AddRangeField(q, in.TrendingGlobal, "trending_global")

	if in.Text != "" {
		textQuery := elastic.NewSimpleQueryStringQuery(in.Text).
			FieldWithBoost("claim_name", 4).
			FieldWithBoost("channel_name", 8).
			FieldWithBoost("title", 1).
			FieldWithBoost("description", 0.5).
			FieldWithBoost("author", 1).
			FieldWithBoost("tags", 0.5)

		q = q.Must(textQuery)
	}

	return q
}

func getUniqueChannels(records []*record, client *elastic.Client, ctx context.Context, searchIndices []string) ([]*pb.Output, map[string]*pb.Output) {
	channels := make(map[string]*pb.Output)
	channelsSet := make(map[string]bool)
	var mget = client.Mget()
	var totalChannels = 0
	for _, r := range records {
		for _, searchIndex := range searchIndices {
			if r.ChannelId != "" && !channelsSet[r.ChannelId] {
				channelsSet[r.ChannelId] = true
				nmget := elastic.NewMultiGetItem().Id(r.ChannelId).Index(searchIndex)
				mget = mget.Add(nmget)
				totalChannels++
			}
			if r.CensorType != 0 && !channelsSet[r.CensoringChannelId] {
				channelsSet[r.CensoringChannelId] = true
				nmget := elastic.NewMultiGetItem().Id(r.CensoringChannelId).Index(searchIndex)
				mget = mget.Add(nmget)
				totalChannels++
			}
		}
	}
	if totalChannels == 0 {
		return []*pb.Output{}, make(map[string]*pb.Output)
	}

	res, err := mget.Do(ctx)
	if err != nil {
		log.Println(err)
		return []*pb.Output{}, make(map[string]*pb.Output)
	}

	channelTxos := make([]*pb.Output, totalChannels)
	//repostedRecords := make([]*record, totalReposted)

	log.Println("total channel", totalChannels)
	for i, doc := range res.Docs {
		var r record
		err := json.Unmarshal(doc.Source, &r)
		if err != nil {
			return []*pb.Output{}, make(map[string]*pb.Output)
		}
		channelTxos[i] = r.recordToOutput()
		channels[r.ClaimId] = channelTxos[i]
		//log.Println(r)
		//repostedRecords[i] = &r
	}

	return channelTxos, channels
}

func getClaimsForReposts(ctx context.Context, client *elastic.Client, records []*record, searchIndices []string) ([]*pb.Output, []*record, map[string]*pb.Output) {

	var totalReposted = 0
	var mget = client.Mget() //.StoredFields("_id")
	/*
		var nmget = elastic.NewMultiGetItem()
		for _, index := range searchIndices {
			nmget = nmget.Index(index)
		}
	*/
	for _, r := range records {
		for _, searchIndex := range searchIndices {
			if r.RepostedClaimId != "" {
				var nmget = elastic.NewMultiGetItem().Id(r.RepostedClaimId).Index(searchIndex)
				//nmget = nmget.Id(r.RepostedClaimId)
				mget = mget.Add(nmget)
				totalReposted++
			}
		}
	}
	//mget = mget.Add(nmget)
	if totalReposted == 0 {
		return []*pb.Output{}, []*record{}, make(map[string]*pb.Output)
	}

	res, err := mget.Do(ctx)
	if err != nil {
		log.Println(err)
		return []*pb.Output{}, []*record{}, make(map[string]*pb.Output)
	}

	claims := make([]*pb.Output, totalReposted)
	repostedRecords := make([]*record, totalReposted)
	respostedMap := make(map[string]*pb.Output)

	log.Println("reposted records", totalReposted)
	for i, doc := range res.Docs {
		var r record
		err := json.Unmarshal(doc.Source, &r)
		if err != nil {
			return []*pb.Output{}, []*record{}, make(map[string]*pb.Output)
		}
		claims[i] = r.recordToOutput()
		repostedRecords[i] = &r
		respostedMap[r.ClaimId] = claims[i]
	}

	return claims, repostedRecords, respostedMap
}

func searchAhead(searchHits []*record, pageSize int, perChannelPerPage int) []*record {
	finalHits := make([]*record, 0, len(searchHits))
	var channelCounters map[string]int
	channelCounters = make(map[string]int)
	nextPageHitsMaybeCheckLater := deque.New()
	searchHitsQ := deque.New()
	for _, rec := range searchHits {
		searchHitsQ.PushRight(rec)
	}
	for !searchHitsQ.Empty() || !nextPageHitsMaybeCheckLater.Empty() {
		if len(finalHits) > 0 && len(finalHits)%pageSize == 0 {
			channelCounters = make(map[string]int)
		} else if len(finalHits) != 0 {
			// means last page was incomplete and we are left with bad replacements
			break
		}

		for i := 0; i < nextPageHitsMaybeCheckLater.Size(); i++ {
			rec := nextPageHitsMaybeCheckLater.PopLeft().(*record)
			if perChannelPerPage > 0 && channelCounters[rec.ChannelId] < perChannelPerPage {
				finalHits = append(finalHits, rec)
				channelCounters[rec.ChannelId] = channelCounters[rec.ChannelId] + 1
			}
		}
		for !searchHitsQ.Empty() {
			hit := searchHitsQ.PopLeft().(*record)
			if hit.ChannelId == "" || perChannelPerPage < 0 {
				finalHits = append(finalHits, hit)
			} else if channelCounters[hit.ChannelId] < perChannelPerPage {
				finalHits = append(finalHits, hit)
				channelCounters[hit.ChannelId] = channelCounters[hit.ChannelId] + 1
				if len(finalHits)%pageSize == 0 {
					break
				}
			} else {
				nextPageHitsMaybeCheckLater.PushRight(hit)
			}
		}
	}
	return finalHits
}

func (r *record) recordToOutput() *pb.Output {
	return &pb.Output{
		TxHash: util.TxIdToTxHash(r.Txid),
		Nout:   r.Nout,
		Height: r.Height,
		Meta: &pb.Output_Claim{
			Claim: &pb.ClaimMeta{
				//Channel:
				//Repost:
				ShortUrl:         r.ShortUrl,
				CanonicalUrl:     r.CanonicalUrl,
				IsControlling:    r.IsControlling,
				TakeOverHeight:   r.TakeOverHeight,
				CreationHeight:   r.CreationHeight,
				ActivationHeight: r.ActivationHeight,
				ExpirationHeight: r.ExpirationHeight,
				ClaimsInChannel:  r.ClaimsInChannel,
				Reposted:         r.Reposted,
				EffectiveAmount:  r.EffectiveAmount,
				SupportAmount:    r.SupportAmount,
				TrendingGroup:    r.TrendingGroup,
				TrendingMixed:    r.TrendingMixed,
				TrendingLocal:    r.TrendingLocal,
				TrendingGlobal:   r.TrendingGlobal,
			},
		},
	}
}

func (r *record) getHitId() string {
	if r.RepostedClaimId != "" {
		return r.RepostedClaimId
	} else {
		return r.ClaimId
	}
}

func removeDuplicates(searchHits []*record) []*record {
	dropped := make(map[*record]bool)
	// claim_id -> (creation_height, hit_id), where hit_id is either reposted claim id or original
	knownIds := make(map[string]*record)

	for _, hit := range searchHits {
		hitHeight := hit.Height
		hitId := hit.getHitId()

		if knownIds[hitId] == nil {
			knownIds[hitId] = hit
		} else {
			prevHit := knownIds[hitId]
			if hitHeight < prevHit.Height {
				knownIds[hitId] = hit
				dropped[prevHit] = true
			} else {
				dropped[hit] = true
			}
		}
	}

	deduped := make([]*record, len(searchHits)-len(dropped))

	var i = 0
	for _, hit := range searchHits {
		if !dropped[hit] {
			deduped[i] = hit
			i++
		}
	}

	return deduped
}

func removeBlocked(searchHits []*record) ([]*record, []*record, map[string]*pb.Blocked) {
	newHits := make([]*record, 0, len(searchHits))
	blockedHits := make([]*record, 0, len(searchHits))
	blockedChannels := make(map[string]*pb.Blocked)
	for _, r := range searchHits {
		if r.CensorType != 0 {
			if blockedChannels[r.CensoringChannelId] == nil {
				blockedObj := &pb.Blocked{
					Count:   1,
					Channel: nil,
				}
				blockedChannels[r.CensoringChannelId] = blockedObj
				blockedHits = append(blockedHits, r)
			} else {
				blockedChannels[r.CensoringChannelId].Count += 1
			}
		} else {
			newHits = append(newHits, r)
		}
	}

	return newHits, blockedHits, blockedChannels
}
