#!/bin/bash

go build .
sudo docker build . -t lbry/hub:latest
