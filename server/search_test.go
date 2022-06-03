package server_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	pb "github.com/lbryio/herald/protobuf/go"
	server "github.com/lbryio/herald/server"
	"github.com/olivere/elastic/v7"
)

func TestInt32ArrToInterface(t *testing.T) {
	want := []int32{0, 10, 100}
	got := server.Int32ArrToInterface(want)
	for i, x := range got {
		if x.(int32) != want[i] {
			t.Errorf("flags: got: %v, want: %v\n", x, want[i])
		}
	}
}

func TestStrArrToInterface(t *testing.T) {
	want := []string{"asdf", "qwer", "xczv"}
	got := server.StrArrToInterface(want)
	for i, x := range got {
		if strings.Compare(x.(string), want[i]) != 0 {
			t.Errorf("flags: got: %v, want: %v\n", x, want[i])
		}
	}
}

func TestAddTermsField(t *testing.T) {
	name := "qwer"
	arr := []string{"a", "b", "c"}
	var query *elastic.BoolQuery = elastic.NewBoolQuery()
	query = server.AddTermsField(query, arr, name)
	fmt.Printf("query: %v\n", query)
}

func TestSearch(t *testing.T) {
	handler := http.NotFound
	ts := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		handler(w, r)
	}))
	defer ts.Close()

	handler = func(w http.ResponseWriter, r *http.Request) {
		resp := `{}`

		w.Write([]byte(resp))
	}

	context := context.Background()
	args := makeDefaultArgs()
	hubServer := server.MakeHubServer(context, args)
	req := &pb.SearchRequest{
		Text: "asdf",
	}
	out, err := hubServer.Search(context, req)
	if err != nil {
		log.Println(err)
	}
	log.Println(out)
}
