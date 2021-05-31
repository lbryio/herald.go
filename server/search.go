package server

import (
	"context"
	"encoding/hex"
	"github.com/btcsuite/btcutil/base58"
	"github.com/golang/protobuf/ptypes/wrappers"
	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/olivere/elastic/v7"
	"golang.org/x/text/cases"
	"golang.org/x/text/unicode/norm"
	"log"
	"reflect"
)

type record struct {
	Txid string   `json:"tx_id"`
	Nout uint32   `json:"tx_nout"`
	Height uint32 `json:"height"`
}

type orderField struct {
	Field string
	is_asc bool
}

func ReverseBytes(s []byte) {
	for i, j := 0, len(s)-1; i < j; i, j = i+1, j-1 {
		s[i], s[j] = s[j], s[i]
	}
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
		return AddTermsField(rq.Value, name, q)
	} else if rq.Op == pb.RangeField_LT {
		return q.Must(elastic.NewRangeQuery(name).Lt(rq.Value))
	} else if rq.Op == pb.RangeField_LTE {
		return q.Must(elastic.NewRangeQuery(name).Lte(rq.Value))
	} else if rq.Op == pb.RangeField_GT {
		return q.Must(elastic.NewRangeQuery(name).Gt(rq.Value))
	} else { // pb.RangeField_GTE
		return q.Must(elastic.NewRangeQuery(name).Gte(rq.Value))
	}
}

func AddInvertibleField(field *pb.InvertibleField, name string, q *elastic.BoolQuery) *elastic.BoolQuery {
	if field == nil {
		return q
	}
	searchVals := StrArrToInterface(field.Value)
	if field.Invert {
		return q.MustNot(elastic.NewTermsQuery(name, searchVals...))
	} else {
		return q.Must(elastic.NewTermsQuery(name, searchVals...))
	}
}

func normalize(s string) string {
	c := cases.Fold()
	return c.String(norm.NFD.String(s))
}

func (s *Server) Search(ctx context.Context, in *pb.SearchRequest) (*pb.SearchReply, error) {
	// TODO: reuse elastic client across requests
	client, err := elastic.NewClient(elastic.SetSniff(false))
	if err != nil {
		return nil, err
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
	var size = 10
	var orderBy []orderField

	// Ping the Elasticsearch server to get e.g. the version number
	//_, code, err := client.Ping("http://127.0.0.1:9200").Do(ctx)
	//if err != nil {
	//	return nil, err
	//}
	//if code != 200 {
	//	return nil, errors.New("ping failed")
	//}

	// TODO: support all of this https://github.com/lbryio/lbry-sdk/blob/master/lbry/wallet/server/db/elasticsearch/search.py#L385

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
		size = int(in.Limit.Value)
	}

	if in.Offset != nil {
		from = int(in.Offset.Value)
	}

	if len(in.Name) > 0 {
		normalized := make([]string, len(in.Name))
		for i := 0; i < len(in.Name); i++ {
			normalized[i] = normalize(in.Name[i])
		}
		in.Normalized = normalized
	}

	if len(in.OrderBy) > 0 {
		for _, x := range in.OrderBy {
			var toAppend string
			var is_asc = false
			if x[0] == '^' {
				is_asc = true
				x = x[1:]
			}
			println(x)
			if _, ok := replacements[x]; ok {
				toAppend = replacements[x]
			} else {
				toAppend = x
			}

			if _, ok := textFields[toAppend]; ok {
				toAppend = toAppend + ".keyword"
			}
			orderBy = append(orderBy, orderField{toAppend, is_asc})
		}
	}
	println(orderBy)

	if len(in.ClaimType) > 0 {
		searchVals := make([]interface{}, len(in.ClaimType))
		for i := 0; i < len(in.ClaimType); i++ {
			searchVals[i] = claimTypes[in.ClaimType[i]]
		}
		q = q.Must(elastic.NewTermsQuery("claim_type", searchVals...))
	}

	// FIXME is this a text field or not?
	if len(in.StreamType) > 0 {
		searchVals := make([]interface{}, len(in.StreamType))
		for i := 0; i < len(in.StreamType); i++ {
			searchVals[i] = streamTypes[in.StreamType[i]]
		}
		q = q.Must(elastic.NewTermsQuery("stream_type.keyword", searchVals...))
	}

	if len(in.XId) > 0 {
		searchVals := make([]interface{}, len(in.XId))
		for i := 0; i < len(in.XId); i++ {
			ReverseBytes(in.XId[i])
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
		//FIXME Might need to abstract this to another message so we can tell if the param is passed
		//without relying on it's truth value
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

	var collapse *elastic.CollapseBuilder
	if in.LimitClaimsPerChannel != nil {
		innerHit := elastic.NewInnerHit().Size(int(in.LimitClaimsPerChannel.Value)).Name("channel_id.keyword")
		collapse = elastic.NewCollapseBuilder("channel_id.keyword").InnerHit(innerHit)
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

	q = AddInvertibleField(in.ChannelId, "channel_id.keyword", q)
	q = AddInvertibleField(in.ChannelIds, "channel_id.keyword", q)
	q = AddInvertibleField(in.Tags, "tags.keyword", q)

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


	indices, err := client.IndexNames()
	if err != nil {
		log.Fatalln(err)
	}
	searchIndices := make([]string, len(indices)-1)
	j := 0
	for i := 0; i < len(indices); i++ {
		if indices[i] == "claims" {
			continue
		}
		searchIndices[j] = indices[i]
		j = j + 1
	}

	fsc := elastic.NewFetchSourceContext(true).Exclude("description", "title")
	search := client.Search().
		Index(searchIndices...).
		FetchSourceContext(fsc).
		//Index("twitter").   // search in index "twitter"
		Query(q). // specify the query
		From(from).Size(size)
	if in.LimitClaimsPerChannel != nil {
		search = search.Collapse(collapse)
	}
	for _, x := range orderBy {
		search = search.Sort(x.Field, x.is_asc)
	}

	searchResult, err := search.Do(ctx) // execute
	if err != nil {
		return nil, err
	}

	log.Printf("%s: found %d results in %dms\n", in.Text, len(searchResult.Hits.Hits), searchResult.TookInMillis)

	txos := make([]*pb.Output, len(searchResult.Hits.Hits))

	var r record
	for i, item := range searchResult.Each(reflect.TypeOf(r)) {
		if t, ok := item.(record); ok {
			txos[i] = &pb.Output{
				TxHash: toHash(t.Txid),
				Nout:   t.Nout,
				Height: t.Height,
			}
		}
	}

	// or if you want more control
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

	return &pb.SearchReply{
		Txos:  txos,
		Total: uint32(searchResult.TotalHits()),
	}, nil
}

// convert txid to txHash
func toHash(txid string) []byte {
	t, err := hex.DecodeString(txid)
	if err != nil {
		return nil
	}

	// reverse the bytes. thanks, Satoshi 😒
	for i, j := 0, len(t)-1; i < j; i, j = i+1, j-1 {
		t[i], t[j] = t[j], t[i]
	}

	return t
}

// convert txHash to txid
func FromHash(txHash []byte) string {
	t := make([]byte, len(txHash))
	copy(t, txHash)

	// reverse the bytes. thanks, Satoshi 😒
	for i, j := 0, len(txHash)-1; i < j; i, j = i+1, j-1 {
		txHash[i], txHash[j] = txHash[j], txHash[i]
	}

	return hex.EncodeToString(t)

}
