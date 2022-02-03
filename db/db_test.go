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

func TestResolve(t *testing.T) {
	filePath := "../resources/reposted_claim.csv"
	db, _, toDefer, err := OpenAndFillTmpDB(filePath)
	if err != nil {
		t.Error(err)
		return
	}
	defer toDefer()
	expandedResolveResult := dbpkg.Resolve(db, "asdf")
	log.Println(expandedResolveResult)
}

func TestIter(t *testing.T) {

	filePath := "../resources/reposted_claim.csv"

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
