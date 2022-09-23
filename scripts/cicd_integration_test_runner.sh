#!/bin/bash
#
# cicd_integration_test_runner.sh
#
# simple script to kick off herald and call the integration testing
# script
#
# N.B. this currently just works locally until we figure a way to have
# the data in the cicd environment.
#

./herald serve --db-path /mnt/sdb1/wallet_server/_data/lbry-rocksdb &

./integration_tests.sh
