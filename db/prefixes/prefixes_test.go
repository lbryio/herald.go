package prefixes_test

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"testing"

	dbpkg "github.com/lbryio/herald.go/db"
	prefixes "github.com/lbryio/herald.go/db/prefixes"
	"github.com/linxGnu/grocksdb"
)

func testInit(filePath string) (*grocksdb.DB, [][]string, func(), *grocksdb.ColumnFamilyHandle) {
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
	columnFamily := records[0][0]
	records = records[1:]

	cleanupFiles := func() {
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}

	// wOpts := grocksdb.NewDefaultWriteOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "tmp")
	if err != nil {
		log.Println(err)
		// Garbage might have been left behind by a prior crash.
		cleanupFiles()
		db, err = grocksdb.OpenDb(opts, "tmp")
		if err != nil {
			log.Println(err)
		}
	}
	handle, err := db.CreateColumnFamily(opts, columnFamily)
	if err != nil {
		log.Println(err)
	}
	toDefer := func() {
		db.Close()
		cleanupFiles()
	}

	return db, records, toDefer, handle
}

func testGeneric(filePath string, prefix byte, numPartials int) func(*testing.T) {
	return func(t *testing.T) {
		APIs := []*prefixes.SerializationAPI{
			prefixes.GetSerializationAPI([]byte{prefix}),
			// Verify combinations of production vs. "restruct" implementations of
			// serialization API (e.g production Pack() with "restruct" Unpack()).
			prefixes.RegressionAPI_1,
			prefixes.RegressionAPI_2,
			prefixes.RegressionAPI_3,
		}
		for _, api := range APIs {
			opts := dbpkg.NewIterateOptions().WithPrefix([]byte{prefix}).WithSerializer(api).WithIncludeValue(true)
			testGenericOptions(opts, filePath, prefix, numPartials)(t)
		}
	}
}

func testGenericOptions(options *dbpkg.IterOptions, filePath string, prefix byte, numPartials int) func(*testing.T) {
	return func(t *testing.T) {

		wOpts := grocksdb.NewDefaultWriteOptions()
		db, records, toDefer, handle := testInit(filePath)
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
			// db.Put(wOpts, key, val)
			db.PutCF(wOpts, handle, key, val)
		}
		// test prefix
		options = options.WithCfHandle(handle)
		ch := dbpkg.IterCF(db, options)
		var i = 0
		for kv := range ch {
			// log.Println(kv.Key)
			gotKey, err := options.Serializer.PackKey(kv.Key)
			if err != nil {
				log.Println(err)
			}

			for j := 1; j <= numPartials; j++ {
				keyPartial, _ := options.Serializer.PackPartialKey(kv.Key, j)
				// Check pack partial for sanity
				if !bytes.HasPrefix(gotKey, keyPartial) {
					// || (!bytes.HasSuffix(gotKey, []byte{0}) && bytes.Equal(gotKey, keyPartial))
					t.Errorf("%+v should be prefix of %+v\n", keyPartial, gotKey)
				}
			}

			got, err := options.Serializer.PackValue(kv.Value)
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
				t.Errorf("gotKey: %+v, wantKey: %+v\n", gotKey, wantKey)
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
		numRecords := i
		// var numRecords = 9
		// if prefix == prefixes.Undo || prefix == prefixes.DBState {
		// 	numRecords = 1
		// }
		stop, err := hex.DecodeString(records[numRecords-1][0])
		if err != nil {
			log.Println(err)
		}
		options2 := dbpkg.NewIterateOptions().WithSerializer(options.Serializer).WithStart(start).WithStop(stop).WithIncludeValue(true)
		options2 = options2.WithCfHandle(handle)
		ch2 := dbpkg.IterCF(db, options2)
		i = 0
		for kv := range ch2 {
			got, err := options2.Serializer.PackValue(kv.Value)
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

func TestSupportAmount(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.SupportAmount)
	testGeneric(filePath, prefixes.SupportAmount, 1)(t)
}

func TestChannelCount(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ChannelCount)
	testGeneric(filePath, prefixes.ChannelCount, 1)(t)
}

func TestDBState(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.DBState)
	testGeneric(filePath, prefixes.DBState, 0)(t)
}

func TestBlockTxs(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.BlockTXs)
	testGeneric(filePath, prefixes.BlockTXs, 1)(t)
}

func TestTxCount(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.TxCount)
	testGeneric(filePath, prefixes.TxCount, 1)(t)
}

func TestTxHash(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.TxHash)
	testGeneric(filePath, prefixes.TxHash, 1)(t)
}

func TestTxNum(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.TxNum)
	testGeneric(filePath, prefixes.TxNum, 1)(t)
}

func TestTx(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.Tx)
	testGeneric(filePath, prefixes.Tx, 1)(t)
}

func TestHashXHistory(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.HashXHistory)
	testGeneric(filePath, prefixes.HashXHistory, 2)(t)
}

func TestUndo(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.Undo)
	testGeneric(filePath, prefixes.Undo, 1)(t)
}

func TestBlockHash(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.BlockHash)
	testGeneric(filePath, prefixes.BlockHash, 1)(t)
}

func TestBlockHeader(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.Header)
	testGeneric(filePath, prefixes.Header, 1)(t)
}

func TestClaimToTXO(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ClaimToTXO)
	testGeneric(filePath, prefixes.ClaimToTXO, 1)(t)
}

func TestTXOToClaim(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.TXOToClaim)
	testGeneric(filePath, prefixes.TXOToClaim, 2)(t)
}

func TestClaimShortID(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ClaimShortIdPrefix)
	testGeneric(filePath, prefixes.ClaimShortIdPrefix, 3)(t)
}

func TestClaimToChannel(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ClaimToChannel)
	testGeneric(filePath, prefixes.ClaimToChannel, 3)(t)
}

func TestChannelToClaim(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ChannelToClaim)
	testGeneric(filePath, prefixes.ChannelToClaim, 4)(t)
}

func TestClaimToSupport(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ClaimToSupport)
	testGeneric(filePath, prefixes.ClaimToSupport, 3)(t)
}

func TestSupportToClaim(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.SupportToClaim)
	testGeneric(filePath, prefixes.SupportToClaim, 2)(t)
}

func TestClaimExpiration(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ClaimExpiration)
	testGeneric(filePath, prefixes.ClaimExpiration, 3)(t)
}

func TestClaimTakeover(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ClaimTakeover)
	testGeneric(filePath, prefixes.ClaimTakeover, 1)(t)
}

func TestPendingActivation(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.PendingActivation)
	testGeneric(filePath, prefixes.PendingActivation, 4)(t)
}

func TestActivated(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ActivatedClaimAndSupport)
	testGeneric(filePath, prefixes.ActivatedClaimAndSupport, 3)(t)
}

func TestActiveAmount(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ActiveAmount)
	testGeneric(filePath, prefixes.ActiveAmount, 5)(t)
}

func TestEffectiveAmount(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.EffectiveAmount)
	testGeneric(filePath, prefixes.EffectiveAmount, 4)(t)
}

func TestRepost(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.Repost)
	testGeneric(filePath, prefixes.Repost, 1)(t)
}

func TestRepostedClaim(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.RepostedClaim)
	testGeneric(filePath, prefixes.RepostedClaim, 3)(t)
}

func TestClaimDiff(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ClaimDiff)
	testGeneric(filePath, prefixes.ClaimDiff, 1)(t)
}

func TestUTXO(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.UTXO)
	testGeneric(filePath, prefixes.UTXO, 1)(t)
}

func TestHashXUTXO(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.HashXUTXO)
	testGeneric(filePath, prefixes.HashXUTXO, 3)(t)
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

func TestHashXStatus(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.HashXStatus)
	key := &prefixes.HashXStatusKey{}
	testGeneric(filePath, prefixes.HashXStatus, key.NumFields())(t)
}

func TestHashXMempoolStatus(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.HashXMempoolStatus)
	key := &prefixes.HashXMempoolStatusKey{}
	testGeneric(filePath, prefixes.HashXMempoolStatus, key.NumFields())(t)
}
