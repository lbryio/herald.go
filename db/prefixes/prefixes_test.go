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

func testInit(filePath string) (*grocksdb.DB, [][]string, func()) {
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

	// wOpts := grocksdb.NewDefaultWriteOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "tmp")
	if err != nil {
		log.Println(err)
	}
	toDefer := func() {
		db.Close()
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}

	return db, records, toDefer
}

func testGeneric(filePath string, prefix byte, numPartials int) func(*testing.T) {
	return func(t *testing.T) {

		wOpts := grocksdb.NewDefaultWriteOptions()
		db, records, toDefer := testInit(filePath)
		defer toDefer()
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
		options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefix}).WithIncludeValue(true)
		ch := dbpkg.Iter(db, options)
		var i = 0
		for kv := range ch {
			// log.Println(kv.Key)
			_, gotKey, err := prefixes.PackGenericKey(prefix, kv.Key)
			if err != nil {
				log.Println(err)
			}

			for j := 1; j <= numPartials; j++ {
				_, keyPartial, _ := prefixes.PackPartialGenericKey(prefix, kv.Key, j)
				// Check pack partial for sanity
				if !bytes.HasPrefix(gotKey, keyPartial) {
					t.Errorf("%+v should be prefix of %+v\n", keyPartial, gotKey)
				}
			}

			_, got, err := prefixes.PackGenericValue(prefix, kv.Value)
			if err != nil {
				log.Println(err)
			}
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
		var numRecords = 9
		if prefix == prefixes.Undo {
			numRecords = 1
		}
		stop, err := hex.DecodeString(records[numRecords][0])
		if err != nil {
			log.Println(err)
		}
		options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
		ch2 := dbpkg.Iter(db, options2)
		i = 0
		for kv := range ch2 {
			_, got, err := prefixes.PackGenericValue(prefix, kv.Value)
			if err != nil {
				log.Println(err)
			}
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
}

func TestTx(t *testing.T) {
	testGeneric("../../resources/tx.csv", prefixes.Tx, 1)(t)
}

func TestHashXHistory(t *testing.T) {
	filePath := "../../resources/hashx_history.csv"
	testGeneric(filePath, prefixes.HashXHistory, 2)(t)
}

func TestUndo(t *testing.T) {
	filePath := "../../resources/undo.csv"
	testGeneric(filePath, prefixes.Undo, 1)(t)
}

func TestBlockHash(t *testing.T) {
	filePath := "../../resources/block_hash.csv"
	testGeneric(filePath, prefixes.BlockHash, 1)(t)
}

func TestBlockHeader(t *testing.T) {
	filePath := "../../resources/header.csv"
	testGeneric(filePath, prefixes.Header, 1)(t)
}

func TestClaimToTXO(t *testing.T) {
	filePath := "../../resources/claim_to_txo.csv"
	testGeneric(filePath, prefixes.ClaimToTXO, 1)(t)
}

func TestTXOToClaim(t *testing.T) {
	filePath := "../../resources/txo_to_claim.csv"
	testGeneric(filePath, prefixes.TXOToClaim, 2)(t)
}

func TestClaimShortID(t *testing.T) {
	filePath := "../../resources/claim_short_id_prefix.csv"
	testGeneric(filePath, prefixes.ClaimShortIdPrefix, 3)(t)
}

func TestClaimToChannel(t *testing.T) {
	filePath := "../../resources/claim_to_channel.csv"
	testGeneric(filePath, prefixes.ClaimToChannel, 3)(t)
}

func TestChannelToClaim(t *testing.T) {
	filePath := "../../resources/channel_to_claim.csv"
	testGeneric(filePath, prefixes.ChannelToClaim, 4)(t)
}

func TestClaimToSupport(t *testing.T) {
	filePath := "../../resources/claim_to_support.csv"
	testGeneric(filePath, prefixes.ClaimToSupport, 3)(t)
}

func TestSupportToClaim(t *testing.T) {
	filePath := "../../resources/support_to_claim.csv"
	testGeneric(filePath, prefixes.SupportToClaim, 2)(t)
}

func TestClaimExpiration(t *testing.T) {
	filePath := "../../resources/claim_expiration.csv"
	testGeneric(filePath, prefixes.ClaimExpiration, 3)(t)
}

func TestClaimTakeover(t *testing.T) {
	filePath := "../../resources/claim_takeover.csv"
	testGeneric(filePath, prefixes.ClaimTakeover, 1)(t)
}

func TestPendingActivation(t *testing.T) {
	filePath := "../../resources/pending_activation.csv"
	testGeneric(filePath, prefixes.PendingActivation, 4)(t)
}

func TestActivated(t *testing.T) {
	filePath := "../../resources/activated_claim_and_support.csv"
	testGeneric(filePath, prefixes.ActivatedClaimAndSupport, 3)(t)
}

func TestActiveAmount(t *testing.T) {
	filePath := "../../resources/active_amount.csv"
	testGeneric(filePath, prefixes.ActiveAmount, 5)(t)
}

func TestEffectiveAmount(t *testing.T) {
	filePath := "../../resources/effective_amount.csv"
	testGeneric(filePath, prefixes.EffectiveAmount, 4)(t)
}

func TestRepost(t *testing.T) {
	filePath := "../../resources/repost.csv"
	testGeneric(filePath, prefixes.Repost, 1)(t)
}

func TestRepostedClaim(t *testing.T) {
	filePath := "../../resources/reposted_claim.csv"
	testGeneric(filePath, prefixes.RepostedClaim, 3)(t)
}

func TestClaimDiff(t *testing.T) {
	filePath := "../../resources/claim_diff.csv"
	testGeneric(filePath, prefixes.ClaimDiff, 1)(t)
}

func TestUTXO(t *testing.T) {
	filePath := "../../resources/utxo.csv"
	testGeneric(filePath, prefixes.UTXO, 1)(t)
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
