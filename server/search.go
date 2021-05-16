package server

import (
	"context"
	"encoding/hex"
	pb "github.com/lbryio/hub/protobuf/go"
	"log"
	"reflect"

	"github.com/btcsuite/btcutil/base58"
	"github.com/olivere/elastic/v7"
)

type record struct {
	Txid string `json:"tx_id"`
	Nout uint32 `json:"tx_nout"`
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

func AddTermsQuery(arr []string, name string, q *elastic.BoolQuery) *elastic.BoolQuery {
	if len(arr) > 0 {
		searchVals := StrArrToInterface(arr)
		return q.Must(elastic.NewTermsQuery(name, searchVals...))
	}
	return q
}

func AddRangeQuery(rq *pb.RangeQuery, name string, q *elastic.BoolQuery) *elastic.BoolQuery {
	if rq == nil {
		return q
	}

	if len(rq.Value) > 1 {
		if rq.Op != pb.RangeQuery_EQ {
			return q
		}
		return AddTermsQuery(rq.Value, name, q)
	}
	if rq.Op == pb.RangeQuery_EQ {
		return AddTermsQuery(rq.Value, name, q)
	} else if rq.Op == pb.RangeQuery_LT {
		return q.Must(elastic.NewRangeQuery(name).Lt(rq.Value))
	} else if rq.Op == pb.RangeQuery_LTE {
		return q.Must(elastic.NewRangeQuery(name).Lte(rq.Value))
	} else if rq.Op == pb.RangeQuery_GT {
		return q.Must(elastic.NewRangeQuery(name).Gt(rq.Value))
	} else { // pb.RangeQuery_GTE
		return q.Must(elastic.NewRangeQuery(name).Gte(rq.Value))
	}
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

	if in.AmountOrder > 0 {
		in.Limit = 1
		in.OrderBy = "effective_amount"
		in.Offset = in.AmountOrder - 1
	}

	if len(in.ClaimType) > 0 {
		searchVals := make([]interface{}, len(in.ClaimType))
		for i := 0; i < len(in.ClaimType); i++ {
			searchVals[i] = claimTypes[in.ClaimType[i]]
		}
		q = q.Must(elastic.NewTermsQuery("claim_type", searchVals...))
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


	if len(in.ClaimId) > 0 {
		searchVals := StrArrToInterface(in.ClaimId)
		if len(in.ClaimId) == 1 && len(in.ClaimId[0]) < 20 {
			q = q.Must(elastic.NewPrefixQuery("claim_id.keyword", in.ClaimId[0]))
		} else {
			q = q.Must(elastic.NewTermsQuery("claim_id.keyword", searchVals...))
		}
	}

	if in.PublicKeyId != "" {
		value := hex.Dump(base58.Decode(in.PublicKeyId)[1:21])
		q = q.Must(elastic.NewTermQuery("public_key_hash.keyword", value))
	}

	q = AddTermsQuery(in.PublicKeyHash, "public_key_hash.keyword", q)
	q = AddTermsQuery(in.Author, "author.keyword", q)
	q = AddTermsQuery(in.Title, "title.keyword", q)
	q = AddTermsQuery(in.CanonicalUrl, "canonical_url.keyword", q)
	q = AddTermsQuery(in.ChannelId, "channel_id.keyword", q)
	q = AddTermsQuery(in.ClaimName, "claim_name.keyword", q)
	q = AddTermsQuery(in.Description, "description.keyword", q)
	q = AddTermsQuery(in.MediaType, "media_type.keyword", q)
	q = AddTermsQuery(in.Normalized, "normalized.keyword", q)
	q = AddTermsQuery(in.PublicKeyBytes, "public_key_bytes.keyword", q)
	q = AddTermsQuery(in.ShortUrl, "short_url.keyword", q)
	q = AddTermsQuery(in.Signature, "signature.keyword", q)
	q = AddTermsQuery(in.SignatureDigest, "signature_digest.keyword", q)
	q = AddTermsQuery(in.StreamType, "stream_type.keyword", q)
	q = AddTermsQuery(in.TxId, "tx_id.keyword", q)
	q = AddTermsQuery(in.FeeCurrency, "fee_currency.keyword", q)
	q = AddTermsQuery(in.RepostedClaimId, "reposted_claim_id.keyword", q)
	q = AddTermsQuery(in.Tags, "tags.keyword", q)

	q = AddRangeQuery(in.TxPosition, "tx_position", q)
	q = AddRangeQuery(in.Amount, "amount", q)
	q = AddRangeQuery(in.Timestamp, "timestamp", q)
	q = AddRangeQuery(in.CreationTimestamp, "creation_timestamp", q)
	q = AddRangeQuery(in.Height, "height", q)
	q = AddRangeQuery(in.CreationHeight, "creation_height", q)
	q = AddRangeQuery(in.ActivationHeight, "activation_height", q)
	q = AddRangeQuery(in.ExpirationHeight, "expiration_height", q)
	q = AddRangeQuery(in.ReleaseTime, "release_time", q)
	q = AddRangeQuery(in.Reposted, "reposted", q)
	q = AddRangeQuery(in.FeeAmount, "fee_amount", q)
	q = AddRangeQuery(in.Duration, "duration", q)
	q = AddRangeQuery(in.CensorType, "censor_type", q)
	q = AddRangeQuery(in.ChannelJoin, "channel_join", q)
	q = AddRangeQuery(in.EffectiveAmount, "effective_amount", q)
	q = AddRangeQuery(in.SupportAmount, "support_amount", q)
	q = AddRangeQuery(in.TrendingGroup, "trending_group", q)
	q = AddRangeQuery(in.TrendingMixed, "trending_mixed", q)
	q = AddRangeQuery(in.TrendingLocal, "trending_local", q)
	q = AddRangeQuery(in.TrendingGlobal, "trending_global", q)

	if in.Query != "" {
		textQuery := elastic.NewSimpleQueryStringQuery(in.Query).
			FieldWithBoost("claim_name", 4).
			FieldWithBoost("channel_name", 8).
			FieldWithBoost("title", 1).
			FieldWithBoost("description", 0.5).
			FieldWithBoost("author", 1).
			FieldWithBoost("tags", 0.5)

		q = q.Must(textQuery)
	}


	searchResult, err := client.Search().
		//Index("twitter").   // search in index "twitter"
		Query(q). // specify the query
		//Sort("user", true). // sort by "user" field, ascending
		From(0).Size(10). // take documents 0-9
		//Pretty(true).       // pretty print request and response JSON
		Do(ctx) // execute
	if err != nil {
		return nil, err
	}

	log.Printf("%s: found %d results in %dms\n", in.Query, len(searchResult.Hits.Hits), searchResult.TookInMillis)

	txos := make([]*pb.Output, len(searchResult.Hits.Hits))

	var r record
	for i, item := range searchResult.Each(reflect.TypeOf(r)) {
		if t, ok := item.(record); ok {
			txos[i] = &pb.Output{
				TxHash: toHash(t.Txid),
				Nout:   t.Nout,
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
	//	for k := range t {
	//		fmt.Println(k)
	//	}
	//	return nil, nil
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
