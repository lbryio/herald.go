package server

import (
	log "github.com/sirupsen/logrus"
)

type ServerService struct {
	Args *Args
}

type ServerFeatureService struct {
	Args *Args
}

type ServerFeaturesReq struct{}

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
		Description:       t.Args.ServerDescription,
		PaymentAddress:    t.Args.PaymentAddress,
		DonationAddress:   t.Args.DonationAddress,
		DailyFee:          t.Args.DailyFee,
		HashFunction:      "sha256",
		TrendingAlgorithm: "fast_ar",
	}
	*res = features

	return nil
}

type ServerBannerService struct {
	Args *Args
}

type ServerBannerReq struct{}

type ServerBannerRes string

// Banner is the json rpc endpoint for 'server.banner'.
func (t *ServerService) Banner(req *ServerBannerReq, res **ServerBannerRes) error {
	log.Println("Banner")

	*res = (*ServerBannerRes)(t.Args.Banner)

	return nil
}

type ServerVersionService struct {
	Args *Args
}

type ServerVersionReq struct{}

type ServerVersionRes string

// Banner is the json rpc endpoint for 'server.version'.
// FIXME: This should return a struct with the version and the protocol version.
// <<-- that comment was written by github, scary shit because it's true
func (t *ServerService) Version(req *ServerVersionReq, res **ServerVersionRes) error {
	log.Println("Version")

	*res = (*ServerVersionRes)(&t.Args.ServerVersion)

	return nil
}
