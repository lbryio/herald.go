#!/bin/bash

# Get new tags from remote
git fetch --tags

# Get latest tag name
latestTag=$(git describe --tags `git rev-list --tags --max-count=1`)
# Make sure it match the format vX.XXXX.XX.XX
[[ $latestTag =~ ^v[0-9]+\.[0-9]{4}\.[0-9]{2}\.[0-9]{2}.*$ ]] || exit 1

# Checkout latest tag
git checkout $latestTag

go build .
docker build . -t lbry/hub:latest
docker tag lbry/hub:latest lbry/hub:$latestTag
docker push lbry/hub:latest
docker push lbry/hub:$latestTag
