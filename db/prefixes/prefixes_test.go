package prefixes_test

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"testing"

	dbpkg "github.com/lbryio/hub/db"
	"github.com/lbryio/hub/db/prefixes"
	"github.com/linxGnu/grocksdb"
)

func TestRepostedClaim(t *testing.T) {

	filePath := "../../resources/reposted_claim.csv"

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
	defer func() {
		db.Close()
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}()
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
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.RepostedClaim}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.RepostedKey).PackKey()

		keyPartial3 := prefixes.RepostedKeyPackPartial(kv.Key.(*prefixes.RepostedKey), 3)
		keyPartial2 := prefixes.RepostedKeyPackPartial(kv.Key.(*prefixes.RepostedKey), 2)
		keyPartial1 := prefixes.RepostedKeyPackPartial(kv.Key.(*prefixes.RepostedKey), 1)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial3) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial3, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial2) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial2, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}

		got := kv.Value.(*prefixes.RepostedValue).PackValue()
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

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		got := kv.Value.(*prefixes.RepostedValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestClaimDiff(t *testing.T) {

	filePath := "../../resources/claim_diff.csv"

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
	defer func() {
		db.Close()
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}()
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
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.ClaimDiff}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.TouchedOrDeletedClaimKey).PackKey()

		keyPartial1 := prefixes.TouchedOrDeletedClaimKeyPackPartial(kv.Key.(*prefixes.TouchedOrDeletedClaimKey), 1)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}

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

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
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
}

func TestUTXO(t *testing.T) {

	filePath := "../../resources/utxo.csv"

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
	defer func() {
		db.Close()
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}()
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
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.UTXO}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
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

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
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
}

func TestHashXUTXO(t *testing.T) {

	tests := []struct {
		name     string
		filePath string
	}{
		{
			name:     "Read HashX_UTXO correctly",
			filePath: "../../resources/hashx_utxo.csv",
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
			defer func() {
				db.Close()
				err = os.RemoveAll("./tmp")
				if err != nil {
					log.Println(err)
				}
			}()
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
			options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.HashXUTXO}).WithStart(start).WithIncludeValue(true)
			ch := dbpkg.Iter(db, options)
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
