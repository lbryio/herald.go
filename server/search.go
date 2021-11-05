package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"math"
	"reflect"
	"strconv"
	"strings"
	"time"

	"github.com/lbryio/hub/internal/metrics"
	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/lbryio/lbry.go/v2/extras/util"
	"github.com/olivere/elastic/v7"
	"github.com/prometheus/client_golang/prometheus"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"google.golang.org/protobuf/encoding/protojson"
	"gopkg.in/karalabe/cookiejar.v1/collections/deque"
)

// DefaultSearchSize is the default max number of items an
// es search will return.
const DefaultSearchSize = 1000

// record is a struct for the response from es.
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
	RepostCount        uint32  `json:"repost_count"`
	EffectiveAmount    uint64  `json:"effective_amount"`
	SupportAmount      uint64  `json:"support_amount"`
	TrendingScore      float64 `json:"trending_score"`
	ClaimName          string  `json:"claim_name"`
}

// orderField is struct for specifying ordering of es search results.
type orderField struct {
	Field string
	IsAsc bool
}

// StrArrToInterface takes an array of strings and returns them as an array of
// interfaces.
func StrArrToInterface(arr []string) []interface{} {
	searchVals := make([]interface{}, len(arr))
	for i := 0; i < len(arr); i++ {
		searchVals[i] = arr[i]
	}
	return searchVals
}

// AddTermsField takes an es bool query, array of string values and a term
// name and adds a TermsQuery for that name matching those values to the
// bool query.
func AddTermsField(q *elastic.BoolQuery, arr []string, name string) *elastic.BoolQuery {
	if len(arr) == 0 {
		return q
	}
	searchVals := StrArrToInterface(arr)
	return q.Must(elastic.NewTermsQuery(name, searchVals...))
}

// AddTermField takes an es bool query, a string value and a term name
// and adds a TermQuery for that name matching that value to the bool
// query.
func AddTermField(q *elastic.BoolQuery, value string, name string) *elastic.BoolQuery {
	if value != "" {
		return q.Must(elastic.NewTermQuery(name, value))
	}
	return q
}

// AddIndividualTermFields takes a bool query, an array of string values
// a term name, and a bool to invert the query, and adds multiple individual
// TermQuerys for that name matching each of the values.
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

// AddRangeField takes a bool query, a range field struct and a term name
// and adds a term query for that name matching that range field.
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

// AddInvertibleField takes a bool query, an invertible field and a term name
// and adds a term query for that name matching that invertible field.
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

// recordErrorAndDie is for fatal errors. It takes an error, increments the
// fatal error metric in prometheus and prints a fatal error message.
func (s *Server) recordErrorAndDie(err error) {
	metrics.ErrorsCounter.With(prometheus.Labels{"error_type": "fatal"}).Inc()
	log.Fatalln(err)
}

// RoundUpReleaseTime take a bool query, a range query and a term name
// and adds a term query for that name (this is for the release time
// field) with the value rounded up.
func RoundUpReleaseTime(q *elastic.BoolQuery, rq *pb.RangeField, name string) *elastic.BoolQuery {
	if rq == nil {
		return q
	}
	releaseTimeInt, err := strconv.ParseInt(rq.Value[0], 10, 32)
	if err != nil {
		return q
	}
	if releaseTimeInt < 0 {
		releaseTimeInt *= - 1
	}
	releaseTime := strconv.Itoa(int(((releaseTimeInt / 360) + 1) * 360))
	if rq.Op == pb.RangeField_EQ {
		return q.Must(elastic.NewTermQuery(name, releaseTime))
	} else if rq.Op == pb.RangeField_LT {
		return q.Must(elastic.NewRangeQuery(name).Lt(releaseTime))
	} else if rq.Op == pb.RangeField_LTE {
		return q.Must(elastic.NewRangeQuery(name).Lte(releaseTime))
	} else if rq.Op == pb.RangeField_GT {
		return q.Must(elastic.NewRangeQuery(name).Gt(releaseTime))
	} else { // pb.RangeField_GTE
		return q.Must(elastic.NewRangeQuery(name).Gte(releaseTime))
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
	if s.Args.DisableEs {
		log.Println("ElasticSearch disable, return nil to search")
		return nil, nil
	}

	metrics.RequestsCount.With(prometheus.Labels{"method": "search"}).Inc()
	defer func(t time.Time) {
		delta := time.Since(t).Seconds()
		metrics.
			QueryTime.
			With(prometheus.Labels{"method": "search"}).
			Observe(delta)
	}(time.Now())

	var from = 0
	var pageSize = 10
	var orderBy []orderField
	var searchIndices []string
	var searchResult *elastic.SearchResult = nil
	client := s.EsClient
	searchIndices = make([]string, 0, 1)
	searchIndices = append(searchIndices, s.Args.EsIndex)

	//Code for debugging locally
	//indices, _ := client.IndexNames()
	//for _, index := range indices {
	//	if index != "claims" {
	//		log.Println(index)
	//		searchIndices = append(searchIndices, index)
	//	}
	//}

	// If it's been more than RefreshDelta time since we last checked if the
	// es index has been refreshed, we check (this is 2 seconds in prod,
	// 0 seconds in debug / unit testing). If the index has been refreshed
	// a different number of times since we last checked, we purge the cache
	if time.Now().After(s.LastRefreshCheck.Add(s.RefreshDelta)) {
		res, err := client.IndexStats(searchIndices[0]).Do(ctx)
		if err != nil {
			log.Printf("Error on ES index stats\n%v\n", err)
		}
		numRefreshes := res.Indices[searchIndices[0]].Primaries.Refresh.Total
		if numRefreshes != s.NumESRefreshes {
			_ = s.QueryCache.Purge()
			s.NumESRefreshes = numRefreshes
		}
	}

	var records []*record

	cacheKey := s.serializeSearchRequest(in)

	setPageVars(in, &pageSize, &from)

	/*
			cache based on search request params
			include from value and number of results.
			When another search request comes in with same search params
			and same or increased offset (which we currently don't even use?)
			that will be a cache hit.
			FIXME: For now the cache is turned off when in debugging mode
				(for unit tests) because it breaks on some of them.
			FIXME: Currently the cache just skips the initial search,
				the mgets and post processing are still done. There's probably
				a more efficient way to store the final result.
	*/

	if val, err := s.QueryCache.Get(cacheKey); err != nil {

		q := elastic.NewBoolQuery()

		err := s.checkQuery(in)
		if err != nil {
			return nil, err
		}
		q = s.setupEsQuery(q, in, &orderBy)

		fsc := elastic.NewFetchSourceContext(true).Exclude("description", "title")
		search := client.Search().
			Index(searchIndices...).
			FetchSourceContext(fsc).
			Query(q). // specify the query
			From(0).Size(DefaultSearchSize)

		for _, x := range orderBy {
			search = search.Sort(x.Field, x.IsAsc)
		}

		searchResult, err = search.Do(ctx) // execute
		if err != nil && elastic.IsNotFound(err) {
			log.Println("Index returned 404! Check writer. Index: ", searchIndices)
			return &pb.Outputs{}, nil

		} else if err != nil {
			metrics.ErrorsCounter.With(prometheus.Labels{"error_type": "search"}).Inc()
			log.Println("Error executing query: ", err)
			return nil, err
		}

		log.Printf("%s: found %d results in %dms\n", in.Text, len(searchResult.Hits.Hits), searchResult.TookInMillis)

		records = s.searchResultToRecords(searchResult)
		err = s.QueryCache.Set(cacheKey, records)
		if err != nil {
			//FIXME: Should this be fatal?
			log.Println("Error storing records in cache: ", err)
		}
	} else {
		records = val.([]*record)
	}

	txos, extraTxos, blocked := s.postProcessResults(ctx, client, records, in, pageSize, from, searchIndices)

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

// normalizeTag takes a string and normalizes it for search in es.
func (s *Server) normalizeTag(tag string) string {
	c := cases.Lower(language.English)
	res := s.MultiSpaceRe.ReplaceAll(
		s.WeirdCharsRe.ReplaceAll(
			[]byte(strings.TrimSpace(strings.Replace(c.String(tag), "'", "", -1))),
			[]byte(" ")),
		[]byte(" "))

	return string(res)
}

// cleanTags takes an array of tags and normalizes them.
func (s *Server) cleanTags(tags []string) []string {
	cleanedTags := make([]string, len(tags))
	for i, tag := range tags {
		cleanedTags[i] = s.normalizeTag(tag)
	}
	return cleanedTags
}

// searchResultToRecords takes an elastic.SearchResult object and converts
// them to internal record structures.
func (s *Server) searchResultToRecords(
	searchResult *elastic.SearchResult) []*record {
	records := make([]*record, 0, searchResult.TotalHits())

	var r record
	for _, item := range searchResult.Each(reflect.TypeOf(r)) {
		if t, ok := item.(record); ok {
			records = append(records, &t)
		}
	}

	return records
}

// postProcessResults takes es search result records and runs our
// post processing on them.
// TODO: more in depth description.
func (s *Server) postProcessResults(
	ctx context.Context,
	client *elastic.Client,
	records []*record,
	in *pb.SearchRequest,
	pageSize int,
	from int,
	searchIndices []string) ([]*pb.Output, []*pb.Output, []*pb.Blocked) {
	var txos []*pb.Output
	var blockedRecords []*record
	var blocked []*pb.Blocked
	var blockedMap map[string]*pb.Blocked

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
	repostClaims, repostRecords, repostedMap := s.getClaimsForReposts(ctx, client, records, searchIndices)
	//get all unique channels
	channels, channelMap := s.getUniqueChannels(append(append(records, repostRecords...), blockedRecords...), client, ctx, searchIndices)
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

// checkQuery takes a search request and does a sanity check on it for
// validity.
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
			return fmt.Errorf("%s cant have more than %d items.", name, limit)
		}
	}
	return nil
}

// setPageVars takes a search request and pointers to the local pageSize
// and from variables and sets them from the struct.
func setPageVars(in *pb.SearchRequest, pageSize *int, from *int) {
	if in.Limit > 0 {
		log.Printf("############ limit: %d\n", in.Limit)
		*pageSize = int(in.Limit)
	}

	if in.Offset > 0 {
		*from = int(in.Offset)
	}
}

// setupEsQuery takes an elastic.BoolQuery, pb.SearchRequest and orderField
// and adds the search request terms to the bool query.
func (s *Server) setupEsQuery(
	q *elastic.BoolQuery,
	in *pb.SearchRequest,
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
		"normalized": "normalized_name",
		"claim_name": "normalized_name",
		"txid":       "tx_id",
		"nout":	      "tx_nout",
		"reposted":   "repost_count",
		"valid_channel_signature": "is_signature_valid",
		"claim_id":   "_id",
		"signature_digest": "signature",
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
		q = q.Must(elastic.NewTermQuery("public_key_id.keyword", in.PublicKeyId))
	}

	if in.HasChannelSignature {
		q = q.Must(elastic.NewExistsQuery("signature"))
		if in.IsSignatureValid != nil {
			q = q.Must(elastic.NewTermQuery("is_signature_valid", in.IsSignatureValid.Value))
		}
	} else if in.IsSignatureValid != nil {
		q = q.MinimumNumberShouldMatch(1)
		q = q.Should(elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery("signature")))
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
	q = AddTermField(q, in.ShortUrl, "short_url.keyword")
	q = AddTermField(q, in.Signature, "signature.keyword")
	q = AddTermField(q, in.TxId, "tx_id.keyword")
	q = AddTermField(q, strings.ToUpper(in.FeeCurrency), "fee_currency.keyword")
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
	q = RoundUpReleaseTime(q, in.ReleaseTime, "release_time")
	q = AddRangeField(q, in.RepostCount, "repost_count")
	q = AddRangeField(q, in.FeeAmount, "fee_amount")
	q = AddRangeField(q, in.Duration, "duration")
	q = AddRangeField(q, in.CensorType, "censor_type")
	q = AddRangeField(q, in.ChannelJoin, "channel_join")
	q = AddRangeField(q, in.EffectiveAmount, "effective_amount")
	q = AddRangeField(q, in.SupportAmount, "support_amount")
	q = AddRangeField(q, in.TrendingScore, "trending_score")

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

// getUniqueChannels takes the record results from the es search and returns
// the unique channels from those records as a list and a map.
func (s *Server) getUniqueChannels(records []*record, client *elastic.Client, ctx context.Context, searchIndices []string) ([]*pb.Output, map[string]*pb.Output) {
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
		metrics.ErrorsCounter.With(prometheus.Labels{"error_type": "get_unique_channels"}).Inc()
		log.Println(err)
		return []*pb.Output{}, make(map[string]*pb.Output)
	}

	channelTxos := make([]*pb.Output, totalChannels)
	//repostedRecords := make([]*record, totalReposted)

	//log.Println("total channel", totalChannels)
	for i, doc := range res.Docs {
		var r record
		err := json.Unmarshal(doc.Source, &r)
		if err != nil {
			metrics.ErrorsCounter.With(prometheus.Labels{"error_type": "json"}).Inc()
			log.Println(err)
			return []*pb.Output{}, make(map[string]*pb.Output)
		}
		channelTxos[i] = r.recordToOutput()
		channels[r.ClaimId] = channelTxos[i]
		//log.Println(r)
		//repostedRecords[i] = &r
	}

	return channelTxos, channels
}

// getClaimsForReposts takes the record results from the es query and returns
// an array and map of the reposted records as well as an array of those
// records.
func (s *Server) getClaimsForReposts(ctx context.Context, client *elastic.Client, records []*record, searchIndices []string) ([]*pb.Output, []*record, map[string]*pb.Output) {

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
		metrics.ErrorsCounter.With(prometheus.Labels{"error_type": "mget"}).Inc()
		log.Println(err)
		return []*pb.Output{}, []*record{}, make(map[string]*pb.Output)
	}

	claims := make([]*pb.Output, totalReposted)
	repostedRecords := make([]*record, totalReposted)
	respostedMap := make(map[string]*pb.Output)

	//log.Println("reposted records", totalReposted)
	for i, doc := range res.Docs {
		var r record
		err := json.Unmarshal(doc.Source, &r)
		if err != nil {
			metrics.ErrorsCounter.With(prometheus.Labels{"error_type": "json"}).Inc()
			log.Println(err)
			return []*pb.Output{}, []*record{}, make(map[string]*pb.Output)
		}
		claims[i] = r.recordToOutput()
		repostedRecords[i] = &r
		respostedMap[r.ClaimId] = claims[i]
	}

	return claims, repostedRecords, respostedMap
}

// serializeSearchRequest takes a search request and serializes it into a key
// for use in the internal cache for the hub.
func (s *Server) serializeSearchRequest(request *pb.SearchRequest) string {
	// Save the offest / limit and set to zero, cache hits should happen regardless
	// and they're used in post processing
	//offset, limit := request.Offset, request.Limit
	//request.Offset = 0
	//request.Limit = 0

	bytes, err := protojson.Marshal(request)
	if err != nil {
		return ""
	}
	str := string((*s.S256).Sum(bytes))
	// log.Println(str)
	//request.Offset = offset
	//request.Limit = limit

	return str
}

// searchAhead takes an array of record results, the pageSize and
// perChannelPerPage value and returns the hits for this page.
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

// recordToOutput is a function on a record struct to turn it into a pb.Output
// struct.
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
				Reposted:         r.RepostCount,
				EffectiveAmount:  r.EffectiveAmount,
				SupportAmount:    r.SupportAmount,
				TrendingScore:    r.TrendingScore,
			},
		},
	}
}

// getHitId is a function on the record struct to get the id for the search
// hit.
func (r *record) getHitId() string {
	if r.RepostedClaimId != "" {
		return r.RepostedClaimId
	} else {
		return r.ClaimId
	}
}

// removeDuplicates takes an array of record results and remove duplicates.
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

// removeBlocked takes an array of record results from the es search
// and removes blocked records.
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

