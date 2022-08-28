#!/bin/bash
curl --request POST 'localhost:8686/' \
	--http1.1 \
	--header 'Content-Type: application/json' \
	--data-raw '{
	"jsonrpc":"2.0",
	"method":"resolve",
	"params":{"data": "asdf"},
	"id":1
}'
