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

func TestOpenFullDB(t *testing.T) {
	url := "lbry://@lothrop#2/lothrop-livestream-games-and-code#c"
	dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb/"
	prefixes := prefixes.GetPrefixes()
	cfNames := []string{"default", "e", "d", "c"}
	for _, prefix := range prefixes {
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
	log.Println(expandedResolveResult)
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

func TestPrintClaimShortId(t *testing.T) {
	filePath := "../testdata/F_cat.csv"
	CatCSV(filePath)
}

func TestClaimShortIdIter(t *testing.T) {
	filePath := "../testdata/F_cat.csv"
	normalName := "cat"
	claimId := "0"
	db, _, toDefer, err := OpenAndFillTmpDBColumnFamlies(filePath)
	if err != nil {
		t.Error(err)
		return
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

func TestPrintClaimToTXO(t *testing.T) {
	filePath := "../testdata/E_2.csv"
	CatCSV(filePath)
}

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

func TestPrintClaimTakeover(t *testing.T) {
	filePath := "../testdata/P_cat.csv"
	CatCSV(filePath)
}

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
