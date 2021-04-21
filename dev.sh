#!/bin/bash

hash reflex 2>/dev/null || go get github.com/cespare/reflex
hash reflex 2>/dev/null || { echo >&2 'Make sure '"$(go env GOPATH)"'/bin is in your $PATH'; exit 1;  }

reflex --decoration=none --start-service=true -- sh -c "go run . serve"
