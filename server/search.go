package server

import (
	"context"
	"encoding/hex"
	"github.com/btcsuite/btcutil/base58"
	"github.com/golang/protobuf/ptypes/wrappers"
	pb "github.com/lbryio/hub/protobuf/go"
	"math"

	//"github.com/lbryio/hub/schema"
	"github.com/lbryio/hub/util"
	"github.com/olivere/elastic/v7"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"gopkg.in/karalabe/cookiejar.v1/collections/deque"
	"log"
	"reflect"
	"strings"
)

type record struct {
	Txid           	string  `json:"tx_id"`
	Nout           	uint32  `json:"tx_nout"`
	Height         	uint32 	`json:"height"`
	ClaimId        	string 	`json:"claim_id"`
	ChannelId      	string 	`json:"channel_id"`
	CreationHeight 	uint32 	`json:"creation_height"`
	RepostedClaimId string  `json:"reposted_claim_id"`
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

func AddTermsField(arr []string, name string, q *elastic.BoolQuery) *elastic.BoolQuery {
	if len(arr) > 0 {
		searchVals := StrArrToInterface(arr)
		return q.Must(elastic.NewTermsQuery(name, searchVals...))
	}
	return q
}

func AddIndividualTermFields(arr []string, name string, q *elastic.BoolQuery, invert bool) *elastic.BoolQuery {
	if len(arr) > 0 {
		for _, x := range arr {
			if invert {
				q = q.MustNot(elastic.NewTermQuery(name, x))
			} else {
				q = q.Must(elastic.NewTermQuery(name, x))
			}
		}
		return q
	}
	return q
}

func AddRangeField(rq *pb.RangeField, name string, q *elastic.BoolQuery) *elastic.BoolQuery {
	if rq == nil {
		return q
	}

	if len(rq.Value) > 1 {
		if rq.Op != pb.RangeField_EQ {
			return q
		}
		return AddTermsField(rq.Value, name, q)
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

func AddInvertibleField(field *pb.InvertibleField, name string, q *elastic.BoolQuery) *elastic.BoolQuery {
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

func (s *Server) Search(ctx context.Context, in *pb.SearchRequest) (*pb.Outputs, error) {
	var client *elastic.Client = nil
	if s.EsClient == nil {
		esUrl := s.Args.EsHost + ":" + s.Args.EsPort
		tmpClient, err := elastic.NewClient(elastic.SetURL(esUrl), elastic.SetSniff(false))
		if err != nil {
			return nil, err
		}
		client = tmpClient
		s.EsClient = client
	} else {
		client = s.EsClient
	}

	claimTypes := map[string]int {
		"stream": 1,
		"channel": 2,
		"repost": 3,
		"collection": 4,
	}

	streamTypes := map[string]int {
		"video": 1,
		"audio": 2,
		"image": 3,
		"document": 4,
		"binary": 5,
		"model": 6,
	}

	replacements := map[string]string {
		"name": "normalized",
		"txid": "tx_id",
		"claim_hash": "_id",
	}

	textFields := map[string]bool {
		"author": true,
		"canonical_url": true,
		"channel_id": true,
		"claim_name": true,
		"description": true,
		"claim_id": true,
		"media_type": true,
		"normalized": true,
		"public_key_bytes": true,
		"public_key_hash": true,
		"short_url": true,
		"signature": true,
		"signature_digest": true,
		"stream_type": true,
		"title": true,
		"tx_id": true,
		"fee_currency": true,
		"reposted_claim_id": true,
		"tags": true,
	}

	var from = 0
	var size = 1000
	var pageSize = 10
	var orderBy []orderField

	// Ping the Elasticsearch server to get e.g. the version number
	//_, code, err := client.Ping("http://127.0.0.1:9200").Do(ctx)
	//if err != nil {
	//	return nil, err
	//}
	//if code != 200 {
	//	return nil, errors.New("ping failed")
	//}

	q := elastic.NewBoolQuery()

	if in.IsControlling != nil {
		q = q.Must(elastic.NewTermQuery("is_controlling", in.IsControlling.Value))
	}

	if in.AmountOrder != nil {
		in.Limit.Value = 1
		in.OrderBy = []string{"effective_amount"}
		in.Offset = &wrappers.Int32Value{Value: in.AmountOrder.Value - 1}
	}

	if in.Limit != nil {
		pageSize = int(in.Limit.Value)
		log.Printf("page size: %d\n", pageSize)
	}

	if in.Offset != nil {
		from = int(in.Offset.Value)
	}

	if len(in.Name) > 0 {
		normalized := make([]string, len(in.Name))
		for i := 0; i < len(in.Name); i++ {
			normalized[i] = util.Normalize(in.Name[i])
		}
		in.Normalized = normalized
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
			orderBy = append(orderBy, orderField{toAppend, isAsc})
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

	if len(in.XId) > 0 {
		searchVals := make([]interface{}, len(in.XId))
		for i := 0; i < len(in.XId); i++ {
			util.ReverseBytes(in.XId[i])
			searchVals[i] = hex.Dump(in.XId[i])
		}
		if len(in.XId) == 1 && len(in.XId[0]) < 20 {
			q = q.Must(elastic.NewPrefixQuery("_id", string(in.XId[0])))
		} else {
			q = q.Must(elastic.NewTermsQuery("_id", searchVals...))
		}
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
		q = q.Must(elastic.NewTermQuery("public_key_hash.keyword", value))
	}

	if in.HasChannelSignature != nil && in.HasChannelSignature.Value {
		q = q.Must(elastic.NewExistsQuery("signature_digest"))
		if in.SignatureValid != nil {
			q = q.Must(elastic.NewTermQuery("signature_valid", in.SignatureValid.Value))
		}
	} else if in.SignatureValid != nil {
		q = q.MinimumNumberShouldMatch(1)
		q = q.Should(elastic.NewBoolQuery().MustNot(elastic.NewExistsQuery("signature_digest")))
		q = q.Should(elastic.NewTermQuery("signature_valid", in.SignatureValid.Value))
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

	q = AddTermsField(in.PublicKeyHash, "public_key_hash.keyword", q)
	q = AddTermsField(in.Author, "author.keyword", q)
	q = AddTermsField(in.Title, "title.keyword", q)
	q = AddTermsField(in.CanonicalUrl, "canonical_url.keyword", q)
	q = AddTermsField(in.ClaimName, "claim_name.keyword", q)
	q = AddTermsField(in.Description, "description.keyword", q)
	q = AddTermsField(in.MediaType, "media_type.keyword", q)
	q = AddTermsField(in.Normalized, "normalized.keyword", q)
	q = AddTermsField(in.PublicKeyBytes, "public_key_bytes.keyword", q)
	q = AddTermsField(in.ShortUrl, "short_url.keyword", q)
	q = AddTermsField(in.Signature, "signature.keyword", q)
	q = AddTermsField(in.SignatureDigest, "signature_digest.keyword", q)
	q = AddTermsField(in.TxId, "tx_id.keyword", q)
	q = AddTermsField(in.FeeCurrency, "fee_currency.keyword", q)
	q = AddTermsField(in.RepostedClaimId, "reposted_claim_id.keyword", q)


	q = AddTermsField(s.cleanTags(in.AnyTags), "tags.keyword", q)
	q = AddIndividualTermFields(s.cleanTags(in.AllTags), "tags.keyword", q, false)
	q = AddIndividualTermFields(s.cleanTags(in.NotTags), "tags.keyword", q, true)
	q = AddTermsField(in.AnyLanguages, "languages", q)
	q = AddIndividualTermFields(in.AllLanguages, "languages", q, false)

	q = AddInvertibleField(in.ChannelId, "channel_id.keyword", q)
	q = AddInvertibleField(in.ChannelIds, "channel_id.keyword", q)


	q = AddRangeField(in.TxPosition, "tx_position", q)
	q = AddRangeField(in.Amount, "amount", q)
	q = AddRangeField(in.Timestamp, "timestamp", q)
	q = AddRangeField(in.CreationTimestamp, "creation_timestamp", q)
	q = AddRangeField(in.Height, "height", q)
	q = AddRangeField(in.CreationHeight, "creation_height", q)
	q = AddRangeField(in.ActivationHeight, "activation_height", q)
	q = AddRangeField(in.ExpirationHeight, "expiration_height", q)
	q = AddRangeField(in.ReleaseTime, "release_time", q)
	q = AddRangeField(in.Reposted, "reposted", q)
	q = AddRangeField(in.FeeAmount, "fee_amount", q)
	q = AddRangeField(in.Duration, "duration", q)
	q = AddRangeField(in.CensorType, "censor_type", q)
	q = AddRangeField(in.ChannelJoin, "channel_join", q)
	q = AddRangeField(in.EffectiveAmount, "effective_amount", q)
	q = AddRangeField(in.SupportAmount, "support_amount", q)
	q = AddRangeField(in.TrendingGroup, "trending_group", q)
	q = AddRangeField(in.TrendingMixed, "trending_mixed", q)
	q = AddRangeField(in.TrendingLocal, "trending_local", q)
	q = AddRangeField(in.TrendingGlobal, "trending_global", q)

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


	//TODO make this only happen in dev environment
	indices, err := client.IndexNames()
	if err != nil {
		log.Fatalln(err)
	}
	var numIndices = 0
	if len(indices) > 0 {
		numIndices = len(indices) - 1
	}
	searchIndices := make([]string, numIndices)
	j := 0
	for i := 0; j < numIndices; i++ {
		if indices[i] == "claims" {
			continue
		}
		searchIndices[j] = indices[i]
		j = j + 1
	}

	fsc := elastic.NewFetchSourceContext(true).Exclude("description", "title")//.Include("_id")
	log.Printf("from: %d, size: %d\n", from, size)
	search := client.Search().
		Index(searchIndices...).
		FetchSourceContext(fsc).
		Query(q). // specify the query
		From(0).Size(1000)

	for _, x := range orderBy {
		log.Println(x.Field, x.IsAsc)
		search = search.Sort(x.Field, x.IsAsc)
	}

	searchResult, err := search.Do(ctx) // execute
	if err != nil {
		return nil, err
	}

	log.Printf("%s: found %d results in %dms\n", in.Text, len(searchResult.Hits.Hits), searchResult.TookInMillis)

	var txos []*pb.Output
	var records []*record

	records = make([]*record, 0, searchResult.TotalHits())

	var r record
	for _, item := range searchResult.Each(reflect.TypeOf(r)) {
		if t, ok := item.(record); ok {
			records = append(records, &t)
		}
	}

	if in.RemoveDuplicates != nil {
		records = removeDuplicates(records)
	}

	if in.LimitClaimsPerChannel != nil {
		records = searchAhead(records, pageSize, int(in.LimitClaimsPerChannel.Value))
	}

	finalLength := int(math.Min(float64(len(records)), float64(pageSize)))
	txos = make([]*pb.Output, 0, finalLength)
	j = 0
	for i := from; i < from + finalLength && i < len(records) && j < finalLength; i++ {
		t := records[i]
		res := &pb.Output{
			TxHash: util.ToHash(t.Txid),
			Nout:   t.Nout,
			Height: t.Height,
		}
		txos = append(txos, res)
		j += 1
	}

	//// or if you want more control
	//for _, hit := range searchResult.Hits.Hits {
	//	// hit.Index contains the name of the index
	//
	//	var t map[string]interface{} // or could be a Record
	//	err := json.Unmarshal(hit.Source, &t)
	//	if err != nil {
	//		return nil, err
	//	}
	//
	//	b, err := json.MarshalIndent(t, "", "  ")
	//	if err != nil {
	//		fmt.Println("error:", err)
	//	}
	//	fmt.Println(string(b))
	//	//for k := range t {
	//	//	fmt.Println(k)
	//	//}
	//	//return nil, nil
	//}

	log.Printf("totalhits: %d\n", searchResult.TotalHits())
	return &pb.Outputs{
		Txos:   txos,
		Total:  uint32(searchResult.TotalHits()),
		Offset: uint32(int64(from) + searchResult.TotalHits()),
	}, nil
}


func sumCounters(channelCounters map[string]int) int {
	var sum int = 0
	for _, v := range channelCounters {
		sum += v
	}

	return sum
}

func searchAhead(searchHits []*record, pageSize int, perChannelPerPage int) []*record {
	finalHits := make([]*record, 0 , len(searchHits))
	var channelCounters map[string]int
	channelCounters = make(map[string]int)
	nextPageHitsMaybeCheckLater := deque.New()
	searchHitsQ := deque.New()
	for _, rec := range searchHits {
		searchHitsQ.PushRight(rec)
	}
	for !searchHitsQ.Empty() || !nextPageHitsMaybeCheckLater.Empty() {
		if len(finalHits) > 0 && len(finalHits) % pageSize == 0 {
			channelCounters = make(map[string]int)
		} else if len(finalHits) != 0 {
			// means last page was incomplete and we are left with bad replacements
			break
		}

		for i := 0; i < nextPageHitsMaybeCheckLater.Size(); i++ {
			rec := nextPageHitsMaybeCheckLater.PopLeft().(*record)
			if perChannelPerPage > 0  && channelCounters[rec.ChannelId] < perChannelPerPage {
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
				if len(finalHits) % pageSize == 0 {
					break
				}
			} else {
				nextPageHitsMaybeCheckLater.PushRight(hit)
			}
		}
	}
	return finalHits
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
				knownIds[hitId]	= hit
				dropped[prevHit] = true
			} else {
				dropped[hit] = true
			}
		}
	}

	deduped := make([]*record, len(searchHits) - len(dropped))

	var i = 0
	for _, hit := range searchHits {
		if !dropped[hit] {
			deduped[i] = hit
			i++
		}
	}

	return deduped
}
