package db

import (
	"fmt"
	"github.com/linxGnu/grocksdb"
	"log"
)

func OpenDB(name string) {
	opts := grocksdb.NewDefaultOptions()
	db, err := grocksdb.OpenDb(opts, name)
	if err != nil {
		log.Println(err)
	}
	log.Println(db.Name())
	ro := grocksdb.NewDefaultReadOptions()
	ro.SetFillCache(false)

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
}
