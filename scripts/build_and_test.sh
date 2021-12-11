#!/bin/bash
./protobuf/build.sh
go build .
go test -v -race ./...
