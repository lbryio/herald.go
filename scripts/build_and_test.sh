#!/bin/bash
./protobuf/build.sh
go version
go build .
rm herald.go
go test -v -race -cover ./...
