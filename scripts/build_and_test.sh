#!/bin/bash
./protobuf/build.sh
go version
go build .
go test -v -race -cover ./...
