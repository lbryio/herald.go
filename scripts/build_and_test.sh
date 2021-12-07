#!/bin/bash
export CGO_LDFLAGS="-L/usr/local/lib -lrocksdb -lstdc++ -lm -lz -lsnappy -llz4 -lzstd"
export CGO_CFLAGS="-I/usr/local/include/rocksdb"
go build .
go test -v -race ./...
