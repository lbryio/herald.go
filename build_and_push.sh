#!/bin/bash

IMPORT_PATH="github.com/lbryio/hub/app"

function print_and_die() {
  echo "$1"
  exit 1
}

# Get new tags from remote
git fetch --tags

# Get latest tag name
LATEST_TAG=$(git describe --tags `git rev-list --tags --max-count=1`)
# Make sure it match the format vX.XXXX.XX.XX
[[ $LATEST_TAG =~ ^v[0-9]+\.[0-9]{4}\.[0-9]{2}\.[0-9]{2}.*$ ]] || print_and_die "bad version ${LATEST_TAG}"
VERSION=$LATEST_TAG

echo "using tag $LATEST_TAG"

# Checkout latest tag
git checkout "$LATEST_TAG"

CGO_ENABLED=0 go build -v -ldflags "-X ${IMPORT_PATH}/meta.version=${VERSION}"
docker build . -t lbry/hub:latest
docker tag lbry/hub:latest lbry/hub:"$LATEST_TAG"
docker push lbry/hub:latest
docker push lbry/hub:"$LATEST_TAG"
