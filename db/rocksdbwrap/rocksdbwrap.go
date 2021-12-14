package rocksdbwrap

import (
	"github.com/linxGnu/grocksdb"
)

type RocksDB struct {
	DB *grocksdb.DB
}

type RocksDBIterator struct {
	It *grocksdb.Iterator
}

func (db *RocksDB) NewIterator(opts *grocksdb.ReadOptions) *RocksDBIterator {
	it := db.DB.NewIterator(opts)
	return &RocksDBIterator{
		It: it,
	}
}
