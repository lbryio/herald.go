package server

import (
	"os"
	"testing"

	pb "github.com/lbryio/herald.go/protobuf/go"
)

// TestParseArgs
func TestParseArgs(t *testing.T) {

	tests := []struct {
		name string
		want bool
	}{
		{
			name: "Correctly disables elastic search",
			want: true,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			os.Args = []string{"serve", "--disable-es"}
			searchRequest := new(pb.SearchRequest)
			args := ParseArgs(searchRequest)
			got := args.DisableEs
			if got != tt.want {
				t.Errorf("flags: got: %v, want: %v\n", got, tt.want)
			}
		})
	}

}
