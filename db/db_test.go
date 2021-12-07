package db

import (
	"testing"
)

// TestOpenDB test to see if we can open a db
func TestOpenDB(t *testing.T) {

	tests := []struct {
		name string
		want string
	}{
		{
			name: "Open a rocksdb database",
			want: "",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			OpenDB("/mnt/d/data/wallet/lbry-rocksdb/")
			got := ""

			if got != tt.want {
				t.Errorf("got: %s, want: %s\n", got, tt.want)
			}
		})
	}

}
