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
		GenesisHash:       t.Args.GenesisHash,
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

type ServerVersionReq [2]string // [client_name, client_version]

type ServerVersionRes [2]string // [version, protocol_version]

// Version is the json rpc endpoint for 'server.version'.
func (t *ServerService) Version(req *ServerVersionReq, res **ServerVersionRes) error {
	// FIXME: We may need to do the computation of a negotiated version here.
	// Also return an error if client is not supported?
	result := [2]string{t.Args.ServerVersion, t.Args.ServerVersion}
	*res = (*ServerVersionRes)(&result)
	log.Printf("Version(%v) -> %v", *req, **res)
	return nil
}
