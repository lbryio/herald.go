package server

import (
	"encoding/json"
	"strconv"
	"testing"

	"github.com/lbryio/herald.go/db"
	"github.com/lbryio/lbcd/chaincfg"
)

// Source: test_variety_of_transactions_and_longish_history (lbry-sdk/tests/integration/transactions)
const regTestDBPath = "../testdata/test_variety_of_transactions/lbry-rocksdb"
const regTestHeight = 502

var regTestAddrs = [30]string{
	"mtgiQkd35xpx3TaZ4RBNirf3uSMQ8tXQ7z",
	"mqMjBtzGTtRty7Y54RqeNLk9QE8rYUfpm3",
	"n2q8ASDZmib4adu2eU4dPvVvjYeU97pks4",
	"mzxYWTJogAtduNaeyH9pSSmBSPkJj33HDJ",
	"mweCKeZkeUUi8RQdHry3Mziphb87vCwiiW",
	"mp7ZuiZgBNJHFX6DVmeZrCj8SuzVQNDLwb",
	"n2zZoBocGCcxe6jFo1anbbAsUFMPXdYfnY",
	"msps28KwRJF77DxhzqD98prdwCrZwdUxJc",
	"mjvkjuss63pq2mpsRn4Q5tsNKVMLG9qUt7",
	"miF9cJn8HiX6vsorRDXtZEgcW7BeWowqkX",
	"mx87wRYFchYaLjXyNaboMuEMRLRboFSPDD",
	"mhvb94idtQvTSCQk9EB16wLLkSrbWizPRG",
	"mx3Fu8FDM4nKR9VYtHWPtSGKVt1D588Ay1",
	"mhqvhX7kLNQ2bUNWZxMhE1z6QEJKrqdV8T",
	"mgekw8L4xEezFtkYdSarL4sk5Sc8n9UtzG",
	"myhFrTz99ZHwbGo7qV4D7fJKfji7YJ3vZ8",
	"mnf8UCVoo6DBq6Tg4QpnFFdV1mFVHi43TF",
	"mn7hKyh6EA8oLAPkvTd9vPEgzLRejLxkj2",
	"msfarwFff7LX6DkXk295x3YMnJtR5Yw8uy",
	"mn8sUv6ryiLn4kzssBTqNaB1oL6qcKDzJ4",
	"mhwgeQFyi1z1RxNR1CphE8PcwG2xBWcxDp",
	"n2jKpDXhVaQHiKqhdQYwwykhoYtKtbh8P1",
	"mhnt4btqpAuiNwjAfFxPEaA4ekCE8faRYN",
	"mmTFCt6Du1VsdxSKc7f21vYsT75KnRy7NM",
	"mm1nx1xSmgRponM5tmdq15KREa7f6M36La",
	"mxMXmMKUqoj19hxEA5r3hZJgirT6nCQh14",
	"mx2L4iqNGzpuNNsDmjvCpcomefDWLAjdv1",
	"mohJcUzQdCYL7nEySKNQC8PUzowNS5gGvo",
	"mjv1vErZiDXsh9TvBDGCBpzobZx7aVYuy7",
	"mwDPTZzHsM6p1DfDnBeojDLRCDceTcejkT",
}

// const dbPath := "/Users/swdev1/hub/scribe_db.599529/lbry-rocksdb"
// const dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb"

func TestServerGetHeight(t *testing.T) {
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(regTestDBPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Error(err)
		return
	}

	s := &BlockchainBlockService{
		DB:    db,
		Chain: &chaincfg.RegressionNetParams,
	}

	req := BlockGetServerHeightReq{}
	var resp *BlockGetServerHeightResp
	err = s.Get_server_height(&req, &resp)
	if err != nil {
		t.Errorf("handler err: %v", err)
	}
	marshalled, err := json.MarshalIndent(resp, "", "    ")
	if err != nil {
		t.Errorf("unmarshal err: %v", err)
	}
	t.Logf("resp: %v", string(marshalled))
	if string(marshalled) != strconv.FormatInt(regTestHeight, 10) {
		t.Errorf("bad height: %v", string(marshalled))
	}
}

func TestGetChunk(t *testing.T) {
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(regTestDBPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Error(err)
		return
	}

	s := &BlockchainBlockService{
		DB:    db,
		Chain: &chaincfg.RegressionNetParams,
	}

	for index := 0; index < 10; index++ {
		req := BlockGetChunkReq(index)
		var resp *BlockGetChunkResp
		err := s.Get_chunk(&req, &resp)
		if err != nil {
			t.Errorf("index: %v handler err: %v", index, err)
		}
		marshalled, err := json.MarshalIndent(resp, "", "    ")
		if err != nil {
			t.Errorf("index: %v unmarshal err: %v", index, err)
		}
		t.Logf("index: %v resp: %v", index, string(marshalled))
		switch index {
		case 0, 1, 2, 3, 4:
			if len(*resp) != (CHUNK_SIZE * HEADER_SIZE * 2) {
				t.Errorf("index: %v bad length: %v", index, len(*resp))
			}
		case 5:
			if len(*resp) != 23*112*2 {
				t.Errorf("index: %v bad length: %v", index, len(*resp))
			}
		default:
			if len(*resp) != 0 {
				t.Errorf("index: %v bad length: %v", index, len(*resp))
			}
		}
	}
}

func TestGetHeader(t *testing.T) {
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(regTestDBPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Error(err)
		return
	}

	s := &BlockchainBlockService{
		DB:    db,
		Chain: &chaincfg.RegressionNetParams,
	}

	for height := 0; height < 700; height += 100 {
		req := BlockGetHeaderReq(height)
		var resp *BlockGetHeaderResp
		err := s.Get_header(&req, &resp)
		if err != nil && height <= 500 {
			t.Errorf("height: %v handler err: %v", height, err)
		}
		marshalled, err := json.MarshalIndent(resp, "", "    ")
		if err != nil {
			t.Errorf("height: %v unmarshal err: %v", height, err)
		}
		t.Logf("height: %v resp: %v", height, string(marshalled))
	}
}

func TestGetBalance(t *testing.T) {
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(regTestDBPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Error(err)
		return
	}

	s := &BlockchainAddressService{
		BlockchainBlockService{
			DB:    db,
			Chain: &chaincfg.RegressionNetParams,
		},
	}

	for _, addr := range regTestAddrs {
		req := AddressGetBalanceReq{addr}
		var resp *AddressGetBalanceResp
		err := s.Get_balance(&req, &resp)
		if err != nil {
			t.Errorf("address: %v handler err: %v", addr, err)
		}
		marshalled, err := json.MarshalIndent(resp, "", "    ")
		if err != nil {
			t.Errorf("address: %v unmarshal err: %v", addr, err)
		}
		t.Logf("address: %v resp: %v", addr, string(marshalled))
	}
}

func TestGetHistory(t *testing.T) {
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(regTestDBPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Error(err)
		return
	}

	s := &BlockchainAddressService{
		BlockchainBlockService{
			DB:    db,
			Chain: &chaincfg.RegressionNetParams,
		},
	}

	for _, addr := range regTestAddrs {
		req := AddressGetHistoryReq{addr}
		var resp *AddressGetHistoryResp
		err := s.Get_history(&req, &resp)
		if err != nil {
			t.Errorf("address: %v handler err: %v", addr, err)
		}
		marshalled, err := json.MarshalIndent(resp, "", "    ")
		if err != nil {
			t.Errorf("address: %v unmarshal err: %v", addr, err)
		}
		t.Logf("address: %v resp: %v", addr, string(marshalled))
	}
}

func TestListUnspent(t *testing.T) {
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(regTestDBPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Error(err)
		return
	}

	s := &BlockchainAddressService{
		BlockchainBlockService{
			DB:    db,
			Chain: &chaincfg.RegressionNetParams,
		},
	}

	for _, addr := range regTestAddrs {
		req := AddressListUnspentReq{addr}
		var resp *AddressListUnspentResp
		err := s.Listunspent(&req, &resp)
		if err != nil {
			t.Errorf("address: %v handler err: %v", addr, err)
		}
		marshalled, err := json.MarshalIndent(resp, "", "    ")
		if err != nil {
			t.Errorf("address: %v unmarshal err: %v", addr, err)
		}
		t.Logf("address: %v resp: %v", addr, string(marshalled))
	}
}
