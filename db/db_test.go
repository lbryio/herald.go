package db_test

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"log"
	"os"
	"testing"

	dbpkg "github.com/lbryio/hub/db"
	"github.com/lbryio/hub/db/prefixes"
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

	for _, cfNameRune := range records[0][0] {
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
		DB:      db,
		Handles: handleMap,
		Opts:    grocksdb.NewDefaultReadOptions(),
	}

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
	dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb/"
	prefixNames := prefixes.GetPrefixes()
	cfNames := []string{"default", "e", "d", "c"}
	for _, prefix := range prefixNames {
		cfName := string(prefix)
		cfNames = append(cfNames, cfName)
	}
	db, err := dbpkg.GetDBColumnFamlies(dbPath, cfNames)
	toDefer := func() {
		db.DB.Close()
		err = os.RemoveAll("./asdf")
		if err != nil {
			log.Println(err)
		}
	}
	defer toDefer()
	if err != nil {
		t.Error(err)
		return
	}
	ch := dbpkg.ClaimShortIdIter(db, "@lbry", "")
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
	url := "lbry://@Styxhexenhammer666#2/legacy-media-baron-les-moonves-(cbs#9"
	// url := "lbry://@lbry"
	// url := "lbry://@lbry#3fda836a92faaceedfe398225fb9b2ee2ed1f01a"
	// url := "lbry://@lbry$1"
	dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb/"
	prefixNames := prefixes.GetPrefixes()
	cfNames := []string{"default", "e", "d", "c"}
	for _, prefix := range prefixNames {
		cfName := string(prefix)
		cfNames = append(cfNames, cfName)
	}
	db, err := dbpkg.GetDBColumnFamlies(dbPath, cfNames)
	toDefer := func() {
		db.DB.Close()
		err = os.RemoveAll("./asdf")
		if err != nil {
			log.Println(err)
		}
	}
	defer toDefer()
	if err != nil {
		t.Error(err)
		return
	}
	expandedResolveResult := dbpkg.Resolve(db, url)
	log.Printf("expandedResolveResult: %#v\n", expandedResolveResult)
}

// FIXME: Needs new data format
func TestResolve(t *testing.T) {
	filePath := "../testdata/P_cat.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
		return
	}
	defer toDefer()
	expandedResolveResult := dbpkg.Resolve(db, "asdf")
	log.Println(expandedResolveResult)
}

// TestGetDBState Tests reading the db state from rocksdb
func TestGetDBState(t *testing.T) {
	filePath := "../testdata/s_resolve.csv"
	want := uint32(1072108)
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()
	state, err := dbpkg.GetDBState(db)
	if err != nil {
		t.Error(err)
	}
	log.Printf("state: %#v\n", state)
	if state.Height != want {
		t.Errorf("Expected %d, got %d", want, state.Height)
	}
}

// TestPrintChannelCount Utility function to cat the ClaimShortId csv
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
	count, err := dbpkg.GetClaimsInChannelCount(db, channelHash)
	if err != nil {
		t.Error(err)
	}
	if count != want {
		t.Errorf("Expected %d, got %d", want, count)
	}
}

// TestPrintClaimShortId Utility function to cat the ClaimShortId csv
func TestPrintClaimShortId(t *testing.T) {
	filePath := "../testdata/F_cat.csv"
	CatCSV(filePath)
}

// TestGetShortClaimIdUrl tests resolving a claim to a short url.
func TestGetShortClaimIdUrl(t *testing.T) {
	// &{[70] cat 0 2104436 0}
	name := "cat"
	normalName := "cat"
	claimHash := []byte{}
	var rootTxNum uint32 = 2104436
	var position uint16 = 0
	filePath := "../testdata/F_cat.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
	}
	defer toDefer()
	shortUrl, err := dbpkg.GetShortClaimIdUrl(db, name, normalName, claimHash, rootTxNum, position)
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

	ch := dbpkg.ClaimShortIdIter(db, normalName, claimId)

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
	val, err := dbpkg.GetCachedClaimHash(db, txNum, position)
	if err != nil {
		t.Error(err)
	} else if val.Name != "one" {
		t.Error(err)
	}
}

// TestPrintClaimToTXO Utility function to cat the ClaimToTXO csv.
func TestPrintClaimToTXO(t *testing.T) {
	filePath := "../testdata/E_2.csv"
	CatCSV(filePath)
}

// TestGetClaimToTXO Tests getting a ClaimToTXO value from the db.
func TestGetClaimToTXO(t *testing.T) {
	claimHashStr := "00000324e40fcb63a0b517a3660645e9bd99244a"
	claimHash, err := hex.DecodeString(claimHashStr)
	if err != nil {
		t.Error(err)
		return
	}
	filePath := "../testdata/E_2.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
		return
	}
	defer toDefer()
	res, err := dbpkg.GetCachedClaimTxo(db, claimHash)
	if err != nil {
		t.Error(err)
		return
	}
	log.Println(res)
}

// TestPrintClaimTakeover Utility function to cat the ClaimTakeover csv.
func TestPrintClaimTakeover(t *testing.T) {
	filePath := "../testdata/P_cat.csv"
	CatCSV(filePath)
}

// TestGetControlingClaim Tests getting a controlling claim value from the db
// based on a name.
func TestGetControllingClaim(t *testing.T) {
	filePath := "../testdata/P_cat.csv"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
		return
	}
	defer toDefer()
	res, err := dbpkg.GetControllingClaim(db, "cat")
	if err != nil {
		t.Error(err)
		return
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
