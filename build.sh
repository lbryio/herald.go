#!/bin/bash

go build .
docker build . -t lbry/hub:latest