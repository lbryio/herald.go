package db

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"testing"

	"github.com/lbryio/hub/db/prefixes"
	"github.com/linxGnu/grocksdb"
)

const tmpPath = "../resources/tmp_rocksdb/"

func TestClaimDiff(t *testing.T) {

	filePath := "../resources/claim_diff.csv"

	log.Println(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Println(err)
	}

	wOpts := grocksdb.NewDefaultWriteOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "tmp")
	if err != nil {
		log.Println(err)
	}
	defer db.Close()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := NewIterateOptions().WithPrefix([]byte{prefixes.ClaimDiff}).WithIncludeValue(true)
	ch := Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.TouchedOrDeletedClaimKey).PackKey()
		got := kv.Value.(*prefixes.TouchedOrDeletedClaimValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
	err = os.RemoveAll("tmp")
	if err != nil {
		log.Println(err)
	}
	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := Iter(db, options2)
	i = 0
	for kv := range ch2 {
		log.Println(kv.Key)
		got := kv.Value.(*prefixes.TouchedOrDeletedClaimValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
	err = os.RemoveAll("tmp")
	if err != nil {
		log.Println(err)
	}
}

func TestUTXO(t *testing.T) {

	filePath := "../resources/utxo.csv"

	log.Println(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Println(err)
	}

	wOpts := grocksdb.NewDefaultWriteOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "tmp")
	if err != nil {
		log.Println(err)
	}
	defer db.Close()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := NewIterateOptions().WithPrefix([]byte{prefixes.UTXO}).WithIncludeValue(true)
	ch := Iter(db, options)
	var i = 0
	for kv := range ch {
		log.Println(kv.Key)
		got := kv.Value.(*prefixes.UTXOValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
	err = os.RemoveAll("./tmp")
	if err != nil {
		log.Println(err)
	}
	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := Iter(db, options2)
	i = 0
	for kv := range ch2 {
		log.Println(kv.Key)
		got := kv.Value.(*prefixes.UTXOValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
	err = os.RemoveAll("./tmp")
	if err != nil {
		log.Println(err)
	}
}

func TestHashXUTXO(t *testing.T) {

	tests := []struct {
		name     string
		filePath string
	}{
		{
			name:     "Read HashX_UTXO correctly",
			filePath: "../resources/hashx_utxo.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log.Println(tt.filePath)
			file, err := os.Open(tt.filePath)
			if err != nil {
				log.Println(err)
			}
			reader := csv.NewReader(file)
			records, err := reader.ReadAll()
			if err != nil {
				log.Println(err)
			}

			wOpts := grocksdb.NewDefaultWriteOptions()
			opts := grocksdb.NewDefaultOptions()
			opts.SetCreateIfMissing(true)
			db, err := grocksdb.OpenDb(opts, "tmp")
			if err != nil {
				log.Println(err)
			}
			defer db.Close()
			for _, record := range records {
				key, err := hex.DecodeString(record[0])
				if err != nil {
					log.Println(err)
				}
				val, err := hex.DecodeString(record[1])
				if err != nil {
					log.Println(err)
				}
				db.Put(wOpts, key, val)
			}
			start, err := hex.DecodeString(records[0][0])
			if err != nil {
				log.Println(err)
			}
			options := NewIterateOptions().WithPrefix([]byte{prefixes.HashXUTXO}).WithStart(start).WithIncludeValue(true)
			ch := Iter(db, options)
			var i = 0
			for kv := range ch {
				log.Println(kv.Key)
				got := kv.Value.(*prefixes.HashXUTXOValue).PackValue()
				want, err := hex.DecodeString(records[i][1])
				if err != nil {
					log.Println(err)
				}
				if !bytes.Equal(got, want) {
					t.Errorf("got: %+v, want: %+v\n", got, want)
				}
				i++
				if i > 9 {
					return
				}
			}
			err = os.RemoveAll("./tmp")
			if err != nil {
				log.Println(err)
			}
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
			want:   "*prefixes.UTXOKey(hashX=41414141414141414141, tx_num=0, nout=0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := &prefixes.UTXOKey{
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
