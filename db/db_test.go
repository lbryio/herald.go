package db

import (
	"encoding/hex"
	"fmt"
	"log"
	"testing"

	"github.com/lbryio/hub/db/prefixes"
)

func TestReadUTXO2(t *testing.T) {

	tests := []struct {
		name      string
		want      []uint64
		wantTotal int
	}{
		{
			name:      "Read UTXO Key Values With Stop",
			want:      []uint64{2174594, 200000000, 20000000, 100000, 603510, 75000000, 100000, 962984, 25000000, 50000000},
			wantTotal: 7,
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			db, err := GetDB("../resources/asdf.db")
			if err != nil {
				t.Errorf("err not nil: %+v\n", err)
			}
			defer db.Close()
			utxoRow := &PrefixRow{
				// KeyStruct:     UTXOKey{},
				// ValueStruct:   UTXOValue{},
				Prefix:        prefixes.UTXO,
				KeyPackFunc:   nil,
				ValuePackFunc: nil,
				DB:            db,
			}
			b, err := hex.DecodeString("000012")
			if err != nil {
				log.Println(err)
			}
			stopKey := &UTXOKey{
				Prefix: prefixes.UTXO,
				HashX:  b,
				TxNum:  0,
				Nout:   0,
			}
			stop := UTXOKeyPackPartial(stopKey, 1)

			options := NewIterateOptions().WithFillCache(false).WithStop(stop)
			log.Println(options)

			ch := utxoRow.Iter(options)

			var i = 0
			for kv := range ch {
				log.Println(kv.Key)
				log.Println(UTXOKeyUnpack(kv.Key))
				log.Println(UTXOValueUnpack(kv.Value))
				got := UTXOValueUnpack(kv.Value).Amount
				if got != tt.want[i] {
					t.Errorf("got: %d, want: %d\n", got, tt.want[i])
				}
				i++
			}

			got := i
			if got != tt.wantTotal {
				t.Errorf("got: %d, want: %d\n", got, tt.want)
			}
		})
	}

}

// func TestReadUTXO(t *testing.T) {

// 	tests := []struct {
// 		name string
// 		want int
// 	}{
// 		{
// 			name: "Read UTXO Key Values",
// 			want: 12,
// 		},
// 	}

// 	for _, tt := range tests {
// 		t.Run(tt.name, func(t *testing.T) {
// 			db, err := GetDB("../resources/asdf.db")
// 			if err != nil {
// 				t.Errorf("err not nil: %+v\n", err)
// 			}
// 			defer db.Close()

// 			data := ReadPrefixN(db, prefixes.UTXO, tt.want)

// 			got := len(data)

// 			for _, kv := range data {
// 				log.Println(UTXOKeyUnpack(kv.Key))
// 				log.Println(UTXOValueUnpack(kv.Value))
// 			}

// 			if got != tt.want {
// 				t.Errorf("got: %d, want: %d\n", got, tt.want)
// 			}
// 		})
// 	}

// }

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
			vals := OpenDB("../resources/tmp.db", "foo")
			got := vals

			if got != tt.want {
				t.Errorf("got: %d, want: %d\n", got, tt.want)
			}
		})
	}

}

func TestOpenDB2(t *testing.T) {

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
			OpenDB("../resources/asdf.db", "u")
			// got := vals

			// if got != tt.want {
			// 	t.Errorf("got: %d, want: %d\n", got, tt.want)
			// }
		})
	}
}

func TestUTXOKey_String(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		hashx  []byte
		txnum  uint32
		nout   uint16
		want   string
	}{
		{
			name:   "Converts to string",
			prefix: []byte("u"),
			hashx:  []byte("AAAAAAAAAA"),
			txnum:  0,
			nout:   0,
			want:   "*db.UTXOKey(hashX=41414141414141414141, tx_num=0, nout=0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := &UTXOKey{
				Prefix: tt.prefix,
				HashX:  tt.hashx,
				TxNum:  tt.txnum,
				Nout:   tt.nout,
			}

			got := fmt.Sprint(key)
			log.Println(got)
			if got != tt.want {
				t.Errorf("got: %s, want: %s\n", got, tt.want)
			}
		})
	}
}
