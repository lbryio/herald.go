#!/bin/bash
./protobuf/build.sh
go version
go build -o herald .
go test -v -race -cover ./...
