package server

import (
	"context"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"github.com/btcsuite/btcutil/base58"
	"github.com/golang/protobuf/ptypes/wrappers"
	pb "github.com/lbryio/hub/protobuf/go"
	"github.com/lbryio/hub/schema"
	"github.com/lbryio/hub/util"
	"github.com/olivere/elastic/v7"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"
	"log"
	"reflect"
	"strings"
)

type record struct {
	Txid string   `json:"tx_id"`
	Nout uint32   `json:"tx_nout"`
	Height uint32 `json:"height"`
	ClaimId string `json:"claim_id"`
}

type orderField struct {
	Field string
	is_asc bool
}
const (
	errorResolution = iota
	channelResolution = iota
	streamResolution = iota
)
type urlResolution struct {
	resolutionType 	int
	value 			string
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

func (s *Server) fullIdFromShortId(ctx context.Context, channelName string, claimId string) (string, error) {
	return "", nil
}


func (s *Server) resolveStream(ctx context.Context, url *schema.URL, channelId string) (string, error) {
	return "", nil
}

func (s *Server) resolveChannelId(ctx context.Context, url *schema.URL) (string, error) {
	if !url.HasChannel() {
		return "", nil
	}
	if url.Channel.IsFullID() {
		return url.Channel.ClaimId, nil
	}
	if url.Channel.IsShortID() {
		channelId, err := s.fullIdFromShortId(ctx, url.Channel.Name, url.Channel.ClaimId)
		if err != nil {
			return "", err
		}
		return channelId, nil
	}

	in := &pb.SearchRequest{}
	in.Normalized = []string{util.Normalize(url.Channel.Name)}
	if url.Channel.ClaimId == "" && url.Channel.AmountOrder < 0 {
		in.IsControlling = &wrappers.BoolValue{Value: true}
	} else {
		if url.Channel.AmountOrder > 0 {
			in.AmountOrder = &wrappers.Int32Value{Value: int32(url.Channel.AmountOrder)}
		}
		if url.Channel.ClaimId != "" {
			in.ClaimId = &pb.InvertibleField{
				Invert: false,
				Value: []string{url.Channel.ClaimId},
			}
		}
	}

	var size = 1
	var from = 0
	q := elastic.NewBoolQuery()
	q = AddTermsField(in.Normalized, "normalized", q)

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


	searchResult, err := s.EsClient.Search().
		Query(q). // specify the query
		From(from).Size(size).
		Do(ctx)

	if err != nil {
		return "", err
	}

	var r record
	var channelId string
	for _, item := range searchResult.Each(reflect.TypeOf(r)) {
		if t, ok := item.(record); ok {
			channelId = t.ClaimId
		}
	}
	//matches, err := s.Search(ctx, in)
	//if err != nil {
	//	return "", err
	//}


	return channelId, nil
}

func (s *Server) resolveUrl(ctx context.Context, rawUrl string) *urlResolution {
	url := schema.ParseURL(rawUrl)
	if url == nil {
		return nil
	}

	channelId, err := s.resolveChannelId(ctx, url)
	if err != nil {
		return &urlResolution{
			resolutionType: errorResolution,
			value: fmt.Sprintf("Could not find channel in \"%s\".", url),
		}
	}

	stream, _ := s.resolveStream(ctx, url, channelId)

	if url.HasStream() {
		return &urlResolution{
			resolutionType: streamResolution,
			value: stream,
		}
	} else {
		return &urlResolution{
			resolutionType: channelResolution,
			value: channelId,
		}
	}
}
/*
   async def resolve_url(self, raw_url):
       if raw_url not in self.resolution_cache:
           self.resolution_cache[raw_url] = await self._resolve_url(raw_url)
       return self.resolution_cache[raw_url]

   async def _resolve_url(self, raw_url):
       try:
           url = URL.parse(raw_url)
       except ValueError as e:
           return e

       stream = LookupError(f'Could not find claim at "{raw_url}".')

       channel_id = await self.resolve_channel_id(url)
       if isinstance(channel_id, LookupError):
           return channel_id
       stream = (await self.resolve_stream(url, channel_id if isinstance(channel_id, str) else None)) or stream
       if url.has_stream:
           return StreamResolution(stream)
       else:
           return ChannelResolution(channel_id)

   async def resolve_channel_id(self, url: URL):
       if not url.has_channel:
           return
       if url.channel.is_fullid:
           return url.channel.claim_id
       if url.channel.is_shortid:
           channel_id = await self.full_id_from_short_id(url.channel.name, url.channel.claim_id)
           if not channel_id:
               return LookupError(f'Could not find channel in "{url}".')
           return channel_id

       query = url.channel.to_dict()
       if set(query) == {'name'}:
           query['is_controlling'] = True
       else:
           query['order_by'] = ['^creation_height']
       matches, _, _ = await self.search(**query, limit=1)
       if matches:
           channel_id = matches[0]['claim_id']
       else:
           return LookupError(f'Could not find channel in "{url}".')
       return channel_id

   async def resolve_stream(self, url: URL, channel_id: str = None):
       if not url.has_stream:
           return None
       if url.has_channel and channel_id is None:
           return None
       query = url.stream.to_dict()
       if url.stream.claim_id is not None:
           if url.stream.is_fullid:
               claim_id = url.stream.claim_id
           else:
               claim_id = await self.full_id_from_short_id(query['name'], query['claim_id'], channel_id)
           return claim_id

       if channel_id is not None:
           if set(query) == {'name'}:
               # temporarily emulate is_controlling for claims in channel
               query['order_by'] = ['effective_amount', '^height']
           else:
               query['order_by'] = ['^channel_join']
           query['channel_id'] = channel_id
           query['signature_valid'] = True
       elif set(query) == {'name'}:
           query['is_controlling'] = True
       matches, _, _ = await self.search(**query, limit=1)
       if matches:
           return matches[0]['claim_id']

 */

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

	//res := s.resolveUrl(ctx, "@abc#111")
	//log.Println(res)

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
			normalized[i] = util.Normalize(in.Name[i])
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

	var collapse *elastic.CollapseBuilder
	if in.LimitClaimsPerChannel != nil {
		println(in.LimitClaimsPerChannel.Value)
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


	q = AddTermsField(s.cleanTags(in.AnyTags), "tags.keyword", q)
	q = AddIndividualTermFields(s.cleanTags(in.AllTags), "tags.keyword", q, false)
	q = AddIndividualTermFields(s.cleanTags(in.NotTags), "tags.keyword", q, true)
	q = AddTermsField(in.AnyLanguages, "languages", q)
	q = AddIndividualTermFields(in.AllLanguages, "languages", q, false)

	q = AddInvertibleField(in.ChannelId, "channel_id.keyword", q)
	q = AddInvertibleField(in.ChannelIds, "channel_id.keyword", q)
	/*

	 */

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
		Query(q). // specify the query
		From(from).Size(size)
	if in.LimitClaimsPerChannel != nil {
		search = search.Collapse(collapse)
	}
	for _, x := range orderBy {
		log.Println(x.Field, x.is_asc)
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
				TxHash: util.ToHash(t.Txid),
				Nout:   t.Nout,
				Height: t.Height,
			}
		}
	}

	// or if you want more control
	for _, hit := range searchResult.Hits.Hits {
		// hit.Index contains the name of the index

		var t map[string]interface{} // or could be a Record
		err := json.Unmarshal(hit.Source, &t)
		if err != nil {
			return nil, err
		}

		b, err := json.MarshalIndent(t, "", "  ")
		if err != nil {
			fmt.Println("error:", err)
		}
		fmt.Println(string(b))
		//for k := range t {
		//	fmt.Println(k)
		//}
		//return nil, nil
	}

	return &pb.Outputs{
		Txos:   txos,
		Total:  uint32(searchResult.TotalHits()),
		Offset: uint32(int64(from) + searchResult.TotalHits()),
	}, nil
}
