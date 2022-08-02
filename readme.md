# LBRY Herald

herald.go is a not yet feature complete go rewrite of the [existing implementation in python](https://github.com/lbryio/hub/tree/master/hub/herald). A herald server provides back-end services to LBRY clients. Services include

- URL resolution
- search
- hub federation and discovery

This project will eventually subsume and replace the
[herald](https://github.com/lbryio/hub/blob/master/docs/docker_examples/hub-compose.yml#L38)
and the [lighthouse](https://github.com/lbryio/lighthouse) search provider.

![](./diagram.png)

## Installation

No install instructions yet. See Contributing below.

## Usage

### Prerequisite: run python block processor and search plugin

Follow the instructions [here](https://lbry.tech/resources/wallet-server).

### Run this hub

```bash
./herald serve
```

```bash
# run with remote services disabled so it can run completely solo
./herald serve --disable-rocksdb-refresh --disable-load-peers --disable-resolve --disable-es --disable-blocking-and-filtering
```

### Search for stuff

```bash
./herald search text goes here
```

## Contributing

Contributions to this project are welcome, encouraged, and compensated. Details [here](https://lbry.tech/contribute).

### Dev Dependencies

Install Go 1.18+

- Ubuntu: `sudo snap install go`
- OSX: `brew install go`
- Windows https://golang.org/doc/install

Download `protoc` from https://github.com/protocolbuffers/protobuf/releases and make sure it is
executable and in your path.

Install Go plugin for protoc and python:

```
go get google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc
pip install grpcio grpcio-tools github3.py
```

Lastly the hub needs protobuf version 3.17.1, it may work with newer version but this is what it's built with, on ubuntu systems you'll have to install this from source see the GitHub actions in `.github/workflows` for an example of this.

Install rocksdb and dependencies your CGO flags, on ubuntu. We use v6.29.5 for feature and statis build support.

```
sudo apt-get install libgflags-dev libsnappy-dev zlib1g-dev libbz2-dev libzstd-dev liblz4-dev
wget https://github.com/facebook/rocksdb/archive/refs/tags/v6.29.5.tar.gz
tar xfzv rocksdb-6.29.5.tar.gz
cd rocksdb-6.29.5
make static_lib
sudo make install
```

```
https://github.com/protocolbuffers/protobuf/releases/download/v3.17.1/protobuf-all-3.17.1.tar.gz
```

If you can run `./protobuf/build.sh` without errors, you have `go` and `protoc` installed correctly. 

Finally, run the block processor as described under Usage.

### Running from Source

Run `./dev.sh` to start the hub. The script will restart the hub as you make changes to `*.go` files. 

To search, use `go run . search text goes here`.

#### Windows

reflex doesn't work on windows, so you'll need to run `go run . serve` and restart manually as you make changes.

## License

This project is MIT licensed. For the full license, see [LICENSE](LICENSE).

## Security

We take security seriously. Please contact security@lbry.com regarding any security issues. [Our PGP key is here](https://lbry.com/faq/pgp-key) if you need it.

## Contact

The primary contact for this project is [@lyoshenka](https://github.com/lyoshenka) ([grin@lbry.com](mailto:grin@lbry.com)).
