package prefixes_test

import (
	"bytes"
	"crypto/rand"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"math"
	"math/big"
	"os"
	"sort"
	"testing"

	dbpkg "github.com/lbryio/herald.go/db"
	prefixes "github.com/lbryio/herald.go/db/prefixes"
	"github.com/linxGnu/grocksdb"
	log "github.com/sirupsen/logrus"
)

func TestPrefixRegistry(t *testing.T) {
	for _, prefix := range prefixes.GetPrefixes() {
		if prefixes.GetSerializationAPI(prefix) == nil {
			t.Errorf("prefix %c not registered", prefix)
		}
	}
}

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

			if numPartials != kv.Key.NumFields() {
				t.Errorf("key reports %v fields but %v expected", kv.Key.NumFields(), numPartials)
			}
			for j := 1; j <= numPartials; j++ {
				keyPartial, _ := options.Serializer.PackPartialKey(kv.Key, j)
				// Check pack partial for sanity
				if j < numPartials {
					if !bytes.HasPrefix(gotKey, keyPartial) || (len(keyPartial) >= len(gotKey)) {
						t.Errorf("%+v should be prefix of %+v\n", keyPartial, gotKey)
					}
				} else {
					if !bytes.Equal(gotKey, keyPartial) {
						t.Errorf("%+v should be equal to %+v\n", keyPartial, gotKey)
					}
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
	testGeneric(filePath, prefixes.ClaimShortIdPrefix, 4)(t)
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

func TestBidOrder(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.BidOrder)
	testGeneric(filePath, prefixes.BidOrder, 4)(t)
}

func TestRepost(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.Repost)
	testGeneric(filePath, prefixes.Repost, 1)(t)
}

func TestRepostedClaim(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.RepostedClaim)
	testGeneric(filePath, prefixes.RepostedClaim, 3)(t)
}

func TestRepostedCount(t *testing.T) {
	prefix := byte(prefixes.RepostedCount)
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefix)
	//synthesizeTestData([]byte{prefix}, filePath, []int{20}, []int{4}, [][3]int{})
	key := &prefixes.RepostedCountKey{}
	testGeneric(filePath, prefix, key.NumFields())(t)
}

func TestClaimDiff(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.ClaimDiff)
	testGeneric(filePath, prefixes.ClaimDiff, 1)(t)
}

func TestUTXO(t *testing.T) {
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefixes.UTXO)
	testGeneric(filePath, prefixes.UTXO, 3)(t)
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

func TestTrendingNotifications(t *testing.T) {
	prefix := byte(prefixes.TrendingNotifications)
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefix)
	//synthesizeTestData([]byte{prefix}, filePath, []int{4, 20}, []int{8, 8}, [][3]int{})
	key := &prefixes.TrendingNotificationKey{}
	testGeneric(filePath, prefix, key.NumFields())(t)
}

func TestMempoolTx(t *testing.T) {
	prefix := byte(prefixes.MempoolTx)
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefix)
	//synthesizeTestData([]byte{prefix}, filePath, []int{32}, []int{}, [][3]int{{20, 100, 1}})
	key := &prefixes.MempoolTxKey{}
	testGeneric(filePath, prefix, key.NumFields())(t)
}

func TestTouchedHashX(t *testing.T) {
	prefix := byte(prefixes.TouchedHashX)
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefix)
	//synthesizeTestData([]byte{prefix}, filePath, []int{4}, []int{}, [][3]int{{1, 5, 11}})
	key := &prefixes.TouchedHashXKey{}
	testGeneric(filePath, prefix, key.NumFields())(t)
}

func TestHashXStatus(t *testing.T) {
	prefix := byte(prefixes.HashXStatus)
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefix)
	//synthesizeTestData([]byte{prefix}, filePath, []int{20}, []int{32}, [][3]int{})
	key := &prefixes.HashXStatusKey{}
	testGeneric(filePath, prefix, key.NumFields())(t)
}

func TestHashXMempoolStatus(t *testing.T) {
	prefix := byte(prefixes.HashXMempoolStatus)
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefix)
	//synthesizeTestData([]byte{prefix}, filePath, []int{20}, []int{32}, [][3]int{})
	key := &prefixes.HashXMempoolStatusKey{}
	testGeneric(filePath, prefix, key.NumFields())(t)
}

func TestEffectiveAmount(t *testing.T) {
	prefix := byte(prefixes.EffectiveAmount)
	filePath := fmt.Sprintf("../../testdata/%c.csv", prefix)
	//synthesizeTestData([]byte{prefix}, filePath, []int{20}, []int{8, 8}, [][3]int{})
	key := &prefixes.EffectiveAmountKey{}
	testGeneric(filePath, prefix, key.NumFields())(t)
}

func synthesizeTestData(prefix []byte, filePath string, keyFixed, valFixed []int, valVariable [][3]int) {
	file, err := os.OpenFile(filePath, os.O_CREATE|os.O_TRUNC|os.O_RDWR, 0644)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	records := make([][2][]byte, 0, 20)
	for r := 0; r < 20; r++ {
		key := make([]byte, 0, 1000)
		key = append(key, prefix...)
		val := make([]byte, 0, 1000)
		// Handle fixed columns of key.
		for _, width := range keyFixed {
			v := make([]byte, width)
			rand.Read(v)
			key = append(key, v...)
		}
		// Handle fixed columns of value.
		for _, width := range valFixed {
			v := make([]byte, width)
			rand.Read(v)
			val = append(val, v...)
		}
		// Handle variable length array in value. Each element is "chunk" size.
		for _, w := range valVariable {
			low, high, chunk := w[0], w[1], w[2]
			n, _ := rand.Int(rand.Reader, big.NewInt(int64(high-low)))
			v := make([]byte, chunk*(low+int(n.Int64())))
			rand.Read(v)
			val = append(val, v...)
		}
		records = append(records, [2][]byte{key, val})
	}

	sort.Slice(records, func(i, j int) bool { return bytes.Compare(records[i][0], records[j][0]) == -1 })

	wr := csv.NewWriter(file)
	wr.Write([]string{string(prefix), ""}) // column headers
	for _, rec := range records {
		encoded := []string{hex.EncodeToString(rec[0]), hex.EncodeToString(rec[1])}
		err := wr.Write(encoded)
		if err != nil {
			panic(err)
		}
	}
	wr.Flush()
}

// Fuzz tests for various Key and Value types (EXPERIMENTAL)

func FuzzTouchedHashXKey(f *testing.F) {
	kvs := []prefixes.TouchedHashXKey{
		{
			Prefix: []byte{prefixes.TouchedHashX},
			Height: 0,
		},
		{
			Prefix: []byte{prefixes.TouchedHashX},
			Height: 1,
		},
		{
			Prefix: []byte{prefixes.TouchedHashX},
			Height: math.MaxUint32,
		},
	}

	for _, kv := range kvs {
		seed := make([]byte, 0, 200)
		seed = append(seed, kv.PackKey()...)
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, in []byte) {
		t.Logf("testing: %+v", in)
		out := make([]byte, 0, 200)
		var kv prefixes.TouchedHashXKey
		kv.UnpackKey(in)
		out = append(out, kv.PackKey()...)
		if len(in) >= 5 {
			if !bytes.HasPrefix(in, out) {
				t.Fatalf("%v: not equal after round trip: %v", in, out)
			}
		}
	})
}

func FuzzTouchedHashXValue(f *testing.F) {
	kvs := []prefixes.TouchedHashXValue{
		{
			TouchedHashXs: [][]byte{},
		},
		{
			TouchedHashXs: [][]byte{
				{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
		{
			TouchedHashXs: [][]byte{
				{0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0},
				{0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10},
			},
		},
		{
			TouchedHashXs: [][]byte{
				{0xff, 0xff, 2, 3, 4, 5, 6, 7, 8, 9, 10},
				{0, 1, 0xff, 0xff, 4, 5, 6, 7, 8, 9, 10},
				{0, 1, 2, 3, 0xff, 0xff, 6, 7, 8, 9, 10},
			},
		},
	}

	for _, kv := range kvs {
		seed := make([]byte, 0, 200)
		seed = append(seed, kv.PackValue()...)
		f.Add(seed)
	}

	f.Fuzz(func(t *testing.T, in []byte) {
		t.Logf("testing: %+v", in)
		out := make([]byte, 0, 200)
		var kv prefixes.TouchedHashXValue
		kv.UnpackValue(in)
		out = append(out, kv.PackValue()...)
		if len(in) >= 5 {
			if !bytes.HasPrefix(in, out) {
				t.Fatalf("%v: not equal after round trip: %v", in, out)
			}
		}
	})
}
