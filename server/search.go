package server

import (
	"context"
	"encoding/hex"
	"log"
	"reflect"

	pb "github.com/lbryio/hub/protobuf/go"

	"github.com/olivere/elastic/v7"
)

type record struct {
	Txid string `json:"tx_id"`
	Nout uint32 `json:"tx_nout"`
}

func (s *Server) Search(ctx context.Context, in *pb.SearchRequest) (*pb.SearchReply, error) {
	// TODO: reuse elastic client across requests
	client, err := elastic.NewClient(elastic.SetSniff(false))
	if err != nil {
		return nil, err
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
	q := elastic.NewSimpleQueryStringQuery(in.Query).
		FieldWithBoost("claim_name", 4).
		FieldWithBoost("channel_name", 8).
		FieldWithBoost("title", 1).
		FieldWithBoost("description", 0.5).
		FieldWithBoost("author", 1).
		FieldWithBoost("tags", 0.5)

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
