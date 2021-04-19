# LBRY Hub

A hub provides back-end services to LBRY clients. Services include

- URL resolution
- search
- wallet services (getting and sending transactions, address subscription)
- hub federation and discovery

This project will eventually subsume and replace the
[wallet server](https://github.com/lbryio/lbry-sdk/blob/v0.92.0/docker/Dockerfile.wallet_server)
and the [lighthouse](https://github.com/lbryio/lighthouse) search provider.

![](./diagram.png)

## Installation

## Usage

### Run python block processor and search plugin

Follow the instructions [here](https://lbry.tech/resources/wallet-server).

### Run this hub

```bash
go run . serve
```

### Search for stuff

```bash
go run . search text goes here
```

## Contributing

Contributions to this project are welcome, encouraged, and compensated. For more details, please check [this](https://lbry.tech/contribute) link.

### Dev Dependencies

Install Go 1.14+

- Ubuntu: `sudo add-apt-repository ppa:longsleep/golang-backports && sudo apt install golang-go`
- OSX: `brew install go`

Download `protoc` from https://github.com/protocolbuffers/protobuf/releases and make sure it is
executable and in your path.

Install Go plugin for protoc:

```
go get google.golang.org/protobuf/cmd/protoc-gen-go google.golang.org/grpc/cmd/protoc-gen-go-grpc
```


## License

This project is MIT licensed. For the full license, see [LICENSE](LICENSE).

## Security

We take security seriously. Please contact security@lbry.com regarding any security issues. [Our GPG key is here](https://lbry.com/faq/gpg-key) if you need it.

## Contact

The primary contact for this project is [@lyoshenka](https://github.com/lyoshenka) ([grin@lbry.com](mailto:grin@lbry.com)).
