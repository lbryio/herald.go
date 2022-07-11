package db_test

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"log"
	"os"
	"strings"
	"testing"

	dbpkg "github.com/lbryio/herald/db"
	"github.com/lbryio/herald/db/prefixes"
	"github.com/lbryio/herald/internal"
	"github.com/linxGnu/grocksdb"
)

////////////////////////////////////////////////////////////////////////////////
// Utility functions for testing
////////////////////////////////////////////////////////////////////////////////

// OpenAndFillTmpDBColumnFamlies opens a db and fills it with data from a csv file using the given column family names
func OpenAndFillTmpDBColumnFamlies(filePath string) (*dbpkg.ReadOnlyDBColumnFamily, [][]string, func(), error) {

	log.Println(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, nil, err
	}

	wOpts := grocksdb.NewDefaultWriteOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "tmp")
	if err != nil {
		return nil, nil, nil, err
	}
	var handleMap map[string]*grocksdb.ColumnFamilyHandle = make(map[string]*grocksdb.ColumnFamilyHandle)

	// Make sure we always create the TxCounts column family
	var cfNameRunes string = records[0][0]
	txCountPrefix := string(prefixes.TxCount)
	if !strings.Contains(cfNameRunes, txCountPrefix) {
		cfNameRunes = cfNameRunes + txCountPrefix
	}
	for _, cfNameRune := range cfNameRunes {
		cfName := string(cfNameRune)
		log.Println(cfName)
		handle, err := db.CreateColumnFamily(opts, cfName)
		if err != nil {
			return nil, nil, nil, err
		}
		handleMap[cfName] = handle
	}
	toDefer := func() {
		db.Close()
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}
	for _, record := range records[1:] {
		cf := record[0]
		if err != nil {
			return nil, nil, nil, err
		}
		handle := handleMap[string(cf)]
		key, err := hex.DecodeString(record[1])
		if err != nil {
			return nil, nil, nil, err
		}
		val, err := hex.DecodeString(record[2])
		if err != nil {
			return nil, nil, nil, err
		}
		db.PutCF(wOpts, handle, key, val)
	}

	myDB := &dbpkg.ReadOnlyDBColumnFamily{
		DB:               db,
		Handles:          handleMap,
		Opts:             grocksdb.NewDefaultReadOptions(),
		BlockedStreams:   make(map[string][]byte),
		BlockedChannels:  make(map[string][]byte),
		FilteredStreams:  make(map[string][]byte),
		FilteredChannels: make(map[string][]byte),
		TxCounts:         nil,
		LastState:        nil,
		Height:           0,
		Headers:          nil,
	}

	// err = dbpkg.ReadDBState(myDB) //TODO: Figure out right place for this
	// if err != nil {
	// 	return nil, nil, nil, err
	// }

	err = myDB.InitTxCounts()
	if err != nil {
		return nil, nil, nil, err
	}

	// err = dbpkg.InitHeaders(myDB)
	// if err != nil {
	// 	return nil, nil, nil, err
	// }

	return myDB, records, toDefer, nil
}

// OpenAndFillTmpDBCF opens a db and fills it with data from a csv file
// using the given column family handle. Old version, should probably remove.
func OpenAndFillTmpDBCF(filePath string) (*grocksdb.DB, [][]string, func(), *grocksdb.ColumnFamilyHandle, error) {

	log.Println(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, nil, nil, err
	}

	wOpts := grocksdb.NewDefaultWriteOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "tmp")
	if err != nil {
		return nil, nil, nil, nil, err
	}
	handle, err := db.CreateColumnFamily(opts, records[0][0])
	if err != nil {
		return nil, nil, nil, nil, err
	}
	toDefer := func() {
		db.Close()
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}
	for _, record := range records[1:] {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			return nil, nil, nil, nil, err
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			return nil, nil, nil, nil, err
		}
		db.PutCF(wOpts, handle, key, val)
	}

	return db, records, toDefer, handle, nil
}

// OpenAndFillTmpDB opens a db and fills it with data from a csv file.
// Old funciont, should probably remove.
func OpenAndFillTmpDB(filePath string) (*grocksdb.DB, [][]string, func(), error) {

	log.Println(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		return nil, nil, nil, err
	}

	wOpts := grocksdb.NewDefaultWriteOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "tmp")
	if err != nil {
		return nil, nil, nil, err
	}
	toDefer := func() {
		db.Close()
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			return nil, nil, nil, err
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			return nil, nil, nil, err
		}
		db.Put(wOpts, key, val)
	}

	return db, records, toDefer, nil
}

// CatCSV Reads a csv version of the db and prints it to stdout,
// while decoding types.
func CatCSV(filePath string) {
	log.Println(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Println(err)
		return
	}
	for _, record := range records[1:] {
		log.Println(record[1])
		keyRaw, err := hex.DecodeString(record[1])
		key, _ := prefixes.UnpackGenericKey(keyRaw)
		log.Println(key)
		if err != nil {
			log.Println(err)
			return
		}
		valRaw, err := hex.DecodeString(record[2])
		// val := prefixes.ClaimTakeoverValueUnpack(valRaw)
		val, _ := prefixes.UnpackGenericValue(keyRaw, valRaw)
		log.Println(val)
		if err != nil {
			log.Println(err)
			return
		}
	}
}

func TestCatFullDB(t *testing.T) {
	t.Skip("Skipping full db test")
	// url := "lbry://@lothrop#2/lothrop-livestream-games-and-code#c"
	// "lbry://@lbry", "lbry://@lbry#3", "lbry://@lbry3f", "lbry://@lbry#3fda836a92faaceedfe398225fb9b2ee2ed1f01a", "lbry://@lbry:1", "lbry://@lbry$1"
	// url := "lbry://@Styxhexenhammer666#2/legacy-media-baron-les-moonves-(cbs#9"
	// url := "lbry://@lbry"
	// url := "lbry://@lbry#3fda836a92faaceedfe398225fb9b2ee2ed1f01a"
	dbPath := "/mnt/sda/wallet_server/_data/lbry-rocksdb/"
	// dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb/"
	secondaryPath := "asdf"
	db, toDefer, err := dbpkg.GetProdDB(dbPath, secondaryPath)

	defer toDefer()
	if err != nil {
		t.Error(err)
		return
	}
	ch := db.ClaimShortIdIter("@lbry", "")
	for row := range ch {
		key := row.Key.(*prefixes.ClaimShortIDKey)
		val := row.Value.(*prefixes.ClaimShortIDValue)
		log.Printf("%#v, %#v\n", key, val)
	}
}

////////////////////////////////////////////////////////////////////////////////
// End utility functions
////////////////////////////////////////////////////////////////////////////////

// TestOpenFullDB Tests running a resolve on a full db.
func TestOpenFullDB(t *testing.T) {
	t.Skip("Skipping full db test")
	// url := "lbry://@lothrop#2/lothrop-livestream-games-and-code#c"
	// "lbry://@lbry", "lbry://@lbry#3", "lbry://@lbry3f", "lbry://@lbry#3fda836a92faaceedfe398225fb9b2ee2ed1f01a", "lbry://@lbry:1", "lbry://@lbry$1"
	// url := "lbry://@Styxhexenhammer666#2/legacy-media-baron-les-moonves-(cbs#9"
	// url := "lbry://@lbry"
	// url := "lbry://@lbry#3fda836a92faaceedfe398225fb9b2ee2ed1f01a"
	// url := "lbry://@lbry$1"
	url := "https://lbry.tv/@lothrop:2/lothrop-livestream-games-and-code:c"
	dbPath := "/mnt/sda/wallet_server/_data/lbry-rocksdb/"
	// dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb/"
	secondaryPath := "asdf"
	db, toDefer, err := dbpkg.GetProdDB(dbPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Error(err)
		return
	}
	expandedResolveResult := db.Resolve(url)
	log.Printf("expandedResolveResult: %#v\n", expandedResolveResult)
	log.Printf("expandedResolveResult: %s\n", expandedResolveResult)
}

// TODO: Finish the constructed data set for the stream part of this resolve.
func TestResolve(t *testing.T) {
	url := "lbry://@Styxhexenhammer666#2/legacy-media-baron-les-moonves-(cbs#9"
	filePath := "../testdata/FULL_resolve.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
		return
	}
	defer toDefer()
	expandedResolveResult := db.Resolve(url)
	log.Printf("%#v\n", expandedResolveResult)
	if expandedResolveResult != nil && expandedResolveResult.Channel != nil {
		log.Println(expandedResolveResult.Channel.GetError())
	}
	if expandedResolveResult != nil && expandedResolveResult.Stream != nil {
		log.Println(expandedResolveResult.Stream.GetError())
	}
}

func TestGetDBState(t *testing.T) {
	filePath := "../testdata/s_resolve.csv"
	want := uint32(1072108)
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()
	state, err := db.GetDBState()
	if err != nil {
		t.Error(err)
	}
	log.Printf("state: %#v\n", state)
	if state.Height != want {
		t.Errorf("Expected %d, got %d", want, state.Height)
	}
}

func TestGetRepostedClaim(t *testing.T) {
	channelHash, _ := hex.DecodeString("2556ed1cab9d17f2a9392030a9ad7f5d138f11bd")
	want := 5
	// Should be non-existent
	channelHash2, _ := hex.DecodeString("2556ed1cab9d17f2a9392030a9ad7f5d138f11bf")
	filePath := "../testdata/W_resolve.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()

	count, err := db.GetRepostedCount(channelHash)
	if err != nil {
		t.Error(err)
	}

	log.Println(count)

	if count != want {
		t.Errorf("Expected %d, got %d", want, count)
	}

	count2, err := db.GetRepostedCount(channelHash2)
	if err != nil {
		t.Error(err)
	}

	if count2 != 0 {
		t.Errorf("Expected 0, got %d", count2)
	}
}

func TestPrintRepost(t *testing.T) {
	filePath := "../testdata/V_resolve.csv"
	CatCSV(filePath)
}

func TestGetRepost(t *testing.T) {
	channelHash, _ := hex.DecodeString("2556ed1cab9d17f2a9392030a9ad7f5d138f11bd")
	channelHash2, _ := hex.DecodeString("000009ca6e0caaaef16872b4bd4f6f1b8c2363e2")
	filePath := "../testdata/V_resolve.csv"
	// want := uint32(3670)
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()

	res, err := db.GetRepost(channelHash)
	if err != nil {
		t.Error(err)
	}

	if !bytes.Equal(res, []byte{}) {
		t.Errorf("Expected empty, got %#v", res)
	}

	res2, err := db.GetRepost(channelHash2)
	if err != nil {
		t.Error(err)
	}

	if bytes.Equal(res2, []byte{}) {
		t.Errorf("Expected non empty, got %#v", res2)
	}
}

func TestPrintChannelCount(t *testing.T) {
	filePath := "../testdata/Z_resolve.csv"
	CatCSV(filePath)
}

func TestGetClaimsInChannelCount(t *testing.T) {
	channelHash, _ := hex.DecodeString("2556ed1cab9d17f2a9392030a9ad7f5d138f11bd")
	filePath := "../testdata/Z_resolve.csv"
	want := uint32(3670)
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()
	count, err := db.GetClaimsInChannelCount(channelHash)
	if err != nil {
		t.Error(err)
	}
	if count != want {
		t.Errorf("Expected %d, got %d", want, count)
	}
}

func TestPrintClaimShortId(t *testing.T) {
	filePath := "../testdata/F_resolve.csv"
	CatCSV(filePath)
}

// TestGetShortClaimIdUrl tests resolving a claim to a short url.
func TestGetShortClaimIdUrl(t *testing.T) {
	name := "@Styxhexenhammer666"
	normalName := internal.NormalizeName(name)
	claimHash, _ := hex.DecodeString("2556ed1cab9d17f2a9392030a9ad7f5d138f11bd")
	// claimHash := []byte{}
	var rootTxNum uint32 = 0x61ec7c
	var position uint16 = 0
	filePath := "../testdata/F_resolve.csv"
	log.Println(filePath)
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()
	shortUrl, err := db.GetShortClaimIdUrl(name, normalName, claimHash, rootTxNum, position)
	if err != nil {
		t.Error(err)
	}
	log.Println(shortUrl)
}

// TestClaimShortIdIter Tests the function to get an iterator of ClaimShortIds
// with a noramlized name and a partial claim id.
func TestClaimShortIdIter(t *testing.T) {
	filePath := "../testdata/F_cat.csv"
	normalName := "cat"
	claimId := "0"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()

	ch := db.ClaimShortIdIter(normalName, claimId)

	for row := range ch {
		key := row.Key.(*prefixes.ClaimShortIDKey)
		log.Println(key)
		if key.NormalizedName != normalName {
			t.Errorf("Expected %s, got %s", normalName, key.NormalizedName)
		}
	}
}

// TestPrintTXOToCLaim Utility function to cat the TXOToClaim csv.
func TestPrintTXOToClaim(t *testing.T) {
	filePath := "../testdata/G_2.csv"
	CatCSV(filePath)
}

// TestGetTXOToClaim Tests getting a claim hash from the db given
// a txNum and position.
func TestGetTXOToClaim(t *testing.T) {
	//&{[71] 1456296 0}
	var txNum uint32 = 1456296
	var position uint16 = 0
	filePath := "../testdata/G_2.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()
	val, err := db.GetCachedClaimHash(txNum, position)
	if err != nil {
		t.Error(err)
	} else if val.Name != "one" {
		t.Error(err)
	}
}

func TestGetClaimToChannel(t *testing.T) {
	streamHashStr := "9a0ed686ecdad9b6cb965c4d6681c02f0bbc66a6"
	claimHashStr := "2556ed1cab9d17f2a9392030a9ad7f5d138f11bd"
	claimHash, _ := hex.DecodeString(claimHashStr)
	streamHash, _ := hex.DecodeString(streamHashStr)

	txNum := uint32(0x6284e3)
	position := uint16(0x0)

	streamTxNum := uint32(0x369e2b2)
	streamPosition := uint16(0x0)

	var val []byte = nil

	filePath := "../testdata/I_resolve.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()

	val, err = db.GetChannelForClaim(claimHash, txNum, position)
	if err != nil {
		t.Error(err)
	}
	if val != nil {
		t.Errorf("Expected nil, got %s", hex.EncodeToString(val))
	}

	val, err = db.GetChannelForClaim(streamHash, streamTxNum, streamPosition)
	if err != nil {
		t.Error(err)
	}
	valStr := hex.EncodeToString(val)
	if valStr != claimHashStr {
		t.Errorf("Expected %s, got %s", claimHashStr, valStr)
	}
}

func TestGetEffectiveAmount(t *testing.T) {
	filePath := "../testdata/S_resolve.csv"
	want := uint64(586370959900)
	claimHashStr := "2556ed1cab9d17f2a9392030a9ad7f5d138f11bd"
	claimHash, _ := hex.DecodeString(claimHashStr)
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()
	db.Height = 1116054

	amount, err := db.GetEffectiveAmount(claimHash, true)
	if err != nil {
		t.Error(err)
	}

	if amount != want {
		t.Errorf("Expected %d, got %d", want, amount)
	}
}

func TestGetSupportAmount(t *testing.T) {
	want := uint64(8654754160700)
	claimHashStr := "2556ed1cab9d17f2a9392030a9ad7f5d138f11bd"
	claimHash, err := hex.DecodeString(claimHashStr)
	if err != nil {
		t.Error(err)
	}
	filePath := "../testdata/a_resolve.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()
	res, err := db.GetSupportAmount(claimHash)
	if err != nil {
		t.Error(err)
	}
	if res != want {
		t.Errorf("Expected %d, got %d", want, res)
	}
}

// TODO: verify where this hash comes from exactly.
func TestGetTxHash(t *testing.T) {
	txNum := uint32(0x6284e3)
	want := "54e14ff0c404c29b3d39ae4d249435f167d5cd4ce5a428ecb745b3df1c8e3dde"

	filePath := "../testdata/X_resolve.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()
	resHash, err := db.GetTxHash(txNum)
	if err != nil {
		t.Error(err)
	}
	resStr := hex.EncodeToString(resHash)
	if want != resStr {
		t.Errorf("Expected %s, got %s", want, resStr)
	}
}

func TestGetExpirationHeight(t *testing.T) {
	var lastUpdated uint32 = 0
	var expHeight uint32 = 0

	expHeight = dbpkg.GetExpirationHeight(lastUpdated)
	if lastUpdated+dbpkg.OriginalClaimExpirationTime != expHeight {
		t.Errorf("Expected %d, got %d", lastUpdated+dbpkg.OriginalClaimExpirationTime, expHeight)
	}

	lastUpdated = dbpkg.ExtendedClaimExpirationForkHeight + 1
	expHeight = dbpkg.GetExpirationHeight(lastUpdated)
	if lastUpdated+dbpkg.ExtendedClaimExpirationTime != expHeight {
		t.Errorf("Expected %d, got %d", lastUpdated+dbpkg.ExtendedClaimExpirationTime, expHeight)
	}

	lastUpdated = 0
	expHeight = dbpkg.GetExpirationHeightFull(lastUpdated, true)
	if lastUpdated+dbpkg.ExtendedClaimExpirationTime != expHeight {
		t.Errorf("Expected %d, got %d", lastUpdated+dbpkg.ExtendedClaimExpirationTime, expHeight)
	}
}

func TestGetActivation(t *testing.T) {
	filePath := "../testdata/R_resolve.csv"
	txNum := uint32(0x6284e3)
	position := uint16(0x0)
	want := uint32(0xa6b65)
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()
	activation, err := db.GetActivation(txNum, position)
	if err != nil {
		t.Error(err)
	}
	if activation != want {
		t.Errorf("Expected %d, got %d", want, activation)
	}
	log.Printf("activation: %#v\n", activation)
}

// TestPrintClaimToTXO Utility function to cat the ClaimToTXO csv.
func TestPrintClaimToTXO(t *testing.T) {
	filePath := "../testdata/E_resolve.csv"
	CatCSV(filePath)
}

// TestGetClaimToTXO Tests getting a ClaimToTXO value from the db.
func TestGetClaimToTXO(t *testing.T) {
	claimHashStr := "2556ed1cab9d17f2a9392030a9ad7f5d138f11bd"
	want := uint32(0x6284e3)
	claimHash, err := hex.DecodeString(claimHashStr)
	if err != nil {
		t.Error(err)
		return
	}
	filePath := "../testdata/E_resolve.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
		return
	}
	defer toDefer()
	res, err := db.GetCachedClaimTxo(claimHash, true)
	if err != nil {
		t.Error(err)
		return
	}
	if res.TxNum != want {
		t.Errorf("Expected %d, got %d", want, res.TxNum)
	}
	log.Printf("res: %#v\n", res)
}

// TestPrintClaimTakeover Utility function to cat the ClaimTakeover csv.
func TestPrintClaimTakeover(t *testing.T) {
	filePath := "../testdata/P_resolve.csv"
	CatCSV(filePath)
}

// TestGetControlingClaim Tests getting a controlling claim value from the db
// based on a name.
func TestGetControllingClaim(t *testing.T) {
	claimName := internal.NormalizeName("@Styxhexenhammer666")
	claimHash := "2556ed1cab9d17f2a9392030a9ad7f5d138f11bd"
	filePath := "../testdata/P_resolve.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
		return
	}
	defer toDefer()
	res, err := db.GetControllingClaim(claimName)
	if err != nil {
		t.Error(err)
	}

	got := hex.EncodeToString(res.ClaimHash)
	if claimHash != got {
		t.Errorf("Expected %s, got %s", claimHash, got)
	}
	log.Println(res)
}

// TestIter Tests the db iterator. Probably needs data format updated.
func TestIter(t *testing.T) {

	filePath := "../testdata/W.csv"

	db, records, toDefer, handle, err := OpenAndFillTmpDBCF(filePath)
	if err != nil {
		t.Error(err)
		return
	}
	// skip the cf
	records = records[1:]
	defer toDefer()
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.RepostedClaim}).WithIncludeValue(true)
	options = options.WithCfHandle(handle)
	// ch := dbpkg.Iter(db, options)
	ch := dbpkg.IterCF(db, options)
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
	options2 = options2.WithCfHandle(handle)
	ch2 := dbpkg.IterCF(db, options2)
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
