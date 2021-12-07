package db

import (
	"fmt"
	"github.com/linxGnu/grocksdb"
	"log"
)

func OpenDB(name string) int {
	// Read db
	opts := grocksdb.NewDefaultOptions()
	db, err := grocksdb.OpenDb(opts, name)
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	if err != nil {
		log.Println(err)
	}

	log.Println(db.Name())

	it := db.NewIterator(ro)
	defer it.Close()

	var i = 0
	it.Seek([]byte("foo"))
	for it = it; it.Valid() && i < 10; it.Next() {
		key := it.Key()
		value := it.Value()

		fmt.Printf("Key: %v Value: %v\n", key.Data(), value.Data())

		key.Free()
		value.Free()
		i++
	}
	if err := it.Err(); err != nil {
		log.Println(err)
	}

	return i
}

func OpenAndWriteDB(in string, out string) {
	// Read db
	opts := grocksdb.NewDefaultOptions()
	db, err := grocksdb.OpenDb(opts, in)
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)
	if err != nil {
		log.Println(err)
	}
	// Write db
	opts.SetCreateIfMissing(true)
	db2, err := grocksdb.OpenDb(opts, out)
	wo := grocksdb.NewDefaultWriteOptions()
	defer db2.Close()

	log.Println(db.Name())
	log.Println(db2.Name())

	it := db.NewIterator(ro)
	defer it.Close()

	var i = 0
	it.Seek([]byte("foo"))
	for it = it; it.Valid() && i < 10; it.Next() {
		key := it.Key()
		value := it.Value()
		fmt.Printf("Key: %v Value: %v\n", key.Data(), value.Data())

		if err := db2.Put(wo, key.Data(), value.Data()); err != nil {
			log.Println(err)
		}

		key.Free()
		value.Free()
		i++
	}
	if err := it.Err(); err != nil {
		log.Println(err)
	}
}
