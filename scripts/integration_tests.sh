#!/bin/bash
#
# integration_testing.sh
#
# GitHub Action CI/CD based integration tests for herald.go
# These are smoke / sanity tests for the server behaving correctly on a "live"
# system, and looks for reasonable response codes, not specific correct
# behavior. Those are covered in unit tests.
#
# N.B.
# For the curl based json tests the `id` field existing is needed.
#

# global variables

RES=(0)
FINALRES=0
# functions


function logical_or {
	for res in ${RES[@]}; do
		if [ $res -eq 1 -o $FINALRES -eq 1 ]; then
			FINALRES=1
			return
		fi
	done
}

function want_got {
	if [ "${WANT}" != "${GOT}" ]; then
		echo "WANT: ${WANT}"
		echo "GOT: ${GOT}"
		RES+=(1)
	else
		RES+=(0)
	fi
}

function want_greater {
	if [ ${WANT} -ge ${GOT} ]; then
		echo "WANT: ${WANT}"
		echo "GOT: ${GOT}"
		RES+=(1)
	else
		RES+=(0)
	fi
}

function test_command_with_want {
	echo $CMD
	GOT=`eval $CMD`

	want_got
}

# grpc endpoint testing


read -r -d '' CMD <<- EOM
	grpcurl -plaintext -d '{"value": ["@Styxhexenhammer666:2"]}' 127.0.0.1:50051 pb.Hub.Resolve
	| jq .txos[0].txHash | sed 's/"//g'
EOM
WANT="VOFP8MQEwps9Oa5NJJQ18WfVzUzlpCjst0Wz3xyOPd4="
test_command_with_want

# GOT=`eval $CMD`

#want_got

##
## N.B. This is a degenerate case that takes a long time to run.
## The runtime should be fixed, but in the meantime, we definitely should
## ensure this behaves as expected.
##
## TODO: Test runtime doesn't exceed worst case.
##

#WANT=806389
#read -r -d '' CMD <<- EOM
#	grpcurl -plaintext -d '{"value": ["foo"]}' 127.0.0.1:50051 pb.Hub.Resolve | jq .txos[0].height
#EOM
# test_command_with_want

# json rpc endpoint testing

## blockchain.block

### blockchain.block.get_chunk
read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "blockchain.block.get_chunk", "params": [0]}'
	| jq .result | sed 's/"//g' | head -c 100
EOM
WANT="010000000000000000000000000000000000000000000000000000000000000000000000cc59e59ff97ac092b55e423aa549"
test_command_with_want

### blockchain.block.get_header
read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "blockchain.block.get_header", "params": []}'
	| jq .result.timestamp
EOM
WANT=1446058291
test_command_with_want

### blockchain.block.headers
read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "blockchain.block.headers", "params": []}'
	| jq .result.count
EOM
WANT=0
test_command_with_want

## blockchain.claimtrie

read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "blockchain.claimtrie.resolve", "params":[{"Data": ["@Styxhexenhammer666:2"]}]}'
	| jq .result.txos[0].tx_hash | sed 's/"//g'
EOM
WANT="VOFP8MQEwps9Oa5NJJQ18WfVzUzlpCjst0Wz3xyOPd4="
test_command_with_want

## blockchain.address

### blockchain.address.get_balance

read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "blockchain.address.get_balance", "params":[{"Address": "bGqWuXRVm5bBqLvLPEQQpvsNxJ5ubc6bwN"}]}'
	| jq .result.confirmed
EOM
WANT=44415602186
test_command_with_want

## blockchain.address.get_history

read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "blockchain.address.get_history", "params":[{"Address": "bGqWuXRVm5bBqLvLPEQQpvsNxJ5ubc6bwN"}]}'
	| jq '.result.confirmed | length'
EOM
WANT=82
test_command_with_want

## blockchain.address.listunspent

read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "blockchain.address.listunspent", "params":[{"Address": "bGqWuXRVm5bBqLvLPEQQpvsNxJ5ubc6bwN"}]}'
	| jq '.result | length'
EOM
WANT=32
test_command_with_want

# blockchain.scripthash

## blockchain.scripthash.get_mempool

read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "blockchain.scripthash.get_mempool", "params":[{"scripthash": "bGqWuXRVm5bBqLvLPEQQpvsNxJ5ubc6bwN"}]}'
	| jq .error | sed 's/"//g'
EOM
WANT="encoding/hex: invalid byte: U+0047 'G'"
test_command_with_want

## blockchain.scripthash.get_history

read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "blockchain.scripthash.get_history", "params":[{"scripthash": "bGqWuXRVm5bBqLvLPEQQpvsNxJ5ubc6bwN"}]}'
	| jq .error | sed 's/"//g'
EOM
WANT="encoding/hex: invalid byte: U+0047 'G'"
test_command_with_want

## blockchain.scripthash.listunspent

read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "blockchain.scripthash.listunspent", "params":[{"scripthash": "bGqWuXRVm5bBqLvLPEQQpvsNxJ5ubc6bwN"}]}'
	| jq .error | sed 's/"//g'
EOM
WANT="encoding/hex: invalid byte: U+0047 'G'"
test_command_with_want

## server.banner 

read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "server.banner", "params":[]}'
	| jq .result | sed 's/"//g'
EOM
WANT="You are connected to an 0.107.0 server."
test_command_with_want

## server.version

read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "server.version", "params":[]}'
	| jq .result | sed 's/"//g'
EOM
WANT="0.107.0"
test_command_with_want

## server.features

read -r -d '' CMD <<- EOM
	curl http://127.0.0.1:50002/rpc -s -H "Content-Type: application/json"
	--data '{"id": 1, "method": "server.features", "params":[]}'
EOM
WANT='{"result":{"hosts":{},"pruning":"","server_version":"0.107.0","protocol_min":"0.54.0","protocol_max":"0.199.0","genesis_hash":"9c89283ba0f3227f6c03b70216b9f665f0118d5e0fa729cedf4fb34d6a34f463","description":"Herald","payment_address":"","donation_address":"","daily_fee":"1.0","hash_function":"sha256","trending_algorithm":"fast_ar"},"error":null,"id":1}'
test_command_with_want

# metrics endpoint testing

WANT=0
GOT=$(curl http://127.0.0.1:2112/metrics -s | grep requests | grep resolve | awk '{print $NF}')
want_greater

# caclulate return value

logical_or $RES

if [ $FINALRES -eq 1 ]; then
	echo "Failed!"
	exit 1
else
	echo "Passed!"
	exit 0
fi
