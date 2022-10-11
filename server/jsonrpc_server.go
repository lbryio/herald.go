package server

import (
	"github.com/lbryio/herald.go/db"
	log "github.com/sirupsen/logrus"
)

// const (
// 	GENESIS_HASH         = "9c89283ba0f3227f6c03b70216b9f665f0118d5e0fa729cedf4fb34d6a34f463"
// 	HUB_PROTOCOL_VERSION = "0.107.0"
// 	PROTOCOL_MIN         = "0.54.0"
// 	PROTOCOL_MAX         = "0.199.0"
// )

type ServerService struct {
	DB   *db.ReadOnlyDBColumnFamily
	Args *Args
}

type ServerFeaturesReq struct{}

/*
   cls.cached_server_features.update({
       'hosts': {},
       'pruning': None,
       'server_version': cls.version,
       'protocol_min': min_str,
       'protocol_max': max_str,
       'genesis_hash': env.coin.GENESIS_HASH,
       'description': env.description,
       'payment_address': env.payment_address,
       'donation_address': env.donation_address,
       'daily_fee': env.daily_fee,
       'hash_function': 'sha256',
       'trending_algorithm': 'fast_ar'
   })
*/
type ServerFeaturesRes struct {
	Hosts             map[string]string `json:"hosts"`
	Pruning           string            `json:"pruning"`
	ServerVersion     string            `json:"server_version"`
	ProtocolMin       string            `json:"protocol_min"`
	ProtocolMax       string            `json:"protocol_max"`
	GenesisHash       string            `json:"genesis_hash"`
	Description       string            `json:"description"`
	PaymentAddress    string            `json:"payment_address"`
	DonationAddress   string            `json:"donation_address"`
	DailyFee          string            `json:"daily_fee"`
	HashFunction      string            `json:"hash_function"`
	TrendingAlgorithm string            `json:"trending_algorithm"`
}

// Features is the json rpc endpoint for 'server.features'.
func (t *ServerService) Features(req *ServerFeaturesReq, res **ServerFeaturesRes) error {
	log.Println("Features")

	features := &ServerFeaturesRes{
		Hosts:             map[string]string{},
		Pruning:           "",
		ServerVersion:     HUB_PROTOCOL_VERSION,
		ProtocolMin:       PROTOCOL_MIN,
		ProtocolMax:       PROTOCOL_MAX,
		GenesisHash:       GENESIS_HASH,
		Description:       "Herald",
		PaymentAddress:    "",
		DonationAddress:   "",
		DailyFee:          "1.0",
		HashFunction:      "sha256",
		TrendingAlgorithm: "fast_ar",
	}
	*res = features

	return nil
}
