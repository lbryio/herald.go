module github.com/lbryio/herald.go

go 1.18

// replace github.com/lbryio/lbry.go/v3 => /home/loki/dev/lbry/lbry.go

require (
	github.com/ReneKroon/ttlcache/v2 v2.8.1
	github.com/akamensky/argparse v1.2.2
	github.com/go-restruct/restruct v1.2.0-alpha
	github.com/lbryio/lbry.go/v3 v3.0.1-beta
	github.com/linxGnu/grocksdb v1.6.42
	github.com/olivere/elastic/v7 v7.0.24
	github.com/prometheus/client_golang v1.11.0
	github.com/prometheus/client_model v0.2.0
	github.com/sirupsen/logrus v1.8.1
	golang.org/x/exp v0.0.0-20220907003533-145caa8ea1d0
	golang.org/x/text v0.3.7
	google.golang.org/grpc v1.46.0
	google.golang.org/protobuf v1.27.1
	gopkg.in/karalabe/cookiejar.v1 v1.0.0-20141109175019-e1490cae028c
)

require golang.org/x/crypto v0.0.0-20211209193657-4570a0811e8b // indirect

require (
	github.com/beorn7/perks v1.0.1 // indirect
	github.com/cespare/xxhash/v2 v2.1.2 // indirect
	github.com/davecgh/go-spew v1.1.1 // indirect
	github.com/golang/protobuf v1.5.2 // indirect
	github.com/josharian/intern v1.0.0 // indirect
	github.com/lbryio/lbcd v0.22.201-beta-rc1
	github.com/mailru/easyjson v0.7.7 // indirect
	github.com/matttproud/golang_protobuf_extensions v1.0.1 // indirect
	github.com/pkg/errors v0.9.1 // indirect
	github.com/pmezard/go-difflib v1.0.0 // indirect
	github.com/prometheus/common v0.26.0 // indirect
	github.com/prometheus/procfs v0.6.0 // indirect
	github.com/stretchr/testify v1.7.0 // indirect
	golang.org/x/net v0.0.0-20211112202133-69e39bad7dc2 // indirect
	golang.org/x/sync v0.0.0-20210220032951-036812b2e83c // indirect
	golang.org/x/sys v0.0.0-20220722155257-8c9f86f7a55f // indirect
	google.golang.org/genproto v0.0.0-20210624195500-8bfb893ecb84 // indirect
	gopkg.in/yaml.v3 v3.0.0-20210107192922-496545a6307b // indirect
)
