package db

import (
	"testing"
)

// TestOpenDB test to see if we can open a db
func TestOpenDB(t *testing.T) {

	tests := []struct {
		name string
		want int
	}{
		{
			name: "Open a rocksdb database",
			want: 10,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			vals := OpenDB("../resources/tmp.db")
			got := vals

			if got != tt.want {
				t.Errorf("got: %d, want: %d\n", got, tt.want)
			}
		})
	}

}
