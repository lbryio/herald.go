package server

import (
	"encoding/json"
	"testing"

	"github.com/lbryio/herald.go/db"
	"github.com/lbryio/lbcd/chaincfg"
)

func TestGetChunk(t *testing.T) {
	dbPath := "/Users/swdev1/hub/scribe_db.599529/lbry-rocksdb"
	// dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb/"
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(dbPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Skip("DB not found")
		t.Error(err)
		return
	}

	s := &Server{
		DB:    db,
		Chain: &chaincfg.MainNetParams,
	}

	for index := 0; index < 10; index++ {
		req := blockGetChunkReq(index)
		resp, err := (&req).Handle(s)
		if err != nil {
			t.Errorf("index: %v handler err: %v", index, err)
		}
		marshalled, err := json.MarshalIndent(resp, "", "    ")
		if err != nil {
			t.Errorf("index: %v unmarshal err: %v", index, err)
		}
		t.Logf("index: %v resp: %v", index, string(marshalled))
		if len(*resp) != (CHUNK_SIZE * HEADER_SIZE * 2) {
			t.Errorf("index: %v bad length: %v", index, len(*resp))
		}
	}
}

func TestGetHeader(t *testing.T) {
	dbPath := "/Users/swdev1/hub/scribe_db.599529/lbry-rocksdb"
	// dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb/"
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(dbPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Skip("DB not found")
		t.Error(err)
		return
	}

	s := &Server{
		DB:    db,
		Chain: &chaincfg.MainNetParams,
	}

	for height := 1000; height < 1010; height++ {
		req := blockGetHeaderReq(height)
		resp, err := (&req).Handle(s)
		if err != nil {
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
	dbPath := "/Users/swdev1/hub/scribe_db.599529/lbry-rocksdb"
	// dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb/"
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(dbPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Skip("DB not found")
		t.Error(err)
		return
	}

	s := &Server{
		DB:    db,
		Chain: &chaincfg.MainNetParams,
	}

	addrs := []string{
		"bCoyqs8Pv4pss5EbNuyuokkdkCqEpDoHmG",
		"bJr6cLth1UmR7wJ14BMc7ch73xBEEV77fV",
	}

	for _, addr := range addrs {
		req := addressGetBalanceReq{addr}
		resp, err := (&req).Handle(s)
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
	dbPath := "/Users/swdev1/hub/scribe_db.599529/lbry-rocksdb"
	// dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb/"
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(dbPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Skip("DB not found")
		t.Error(err)
		return
	}

	s := &Server{
		DB:    db,
		Chain: &chaincfg.MainNetParams,
	}

	addrs := []string{
		"bCoyqs8Pv4pss5EbNuyuokkdkCqEpDoHmG",
		"bJr6cLth1UmR7wJ14BMc7ch73xBEEV77fV",
	}

	for _, addr := range addrs {
		req := addressGetHistoryReq{addr}
		resp, err := (&req).Handle(s)
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
	dbPath := "/Users/swdev1/hub/scribe_db.599529/lbry-rocksdb"
	// dbPath := "/mnt/d/data/snapshot_1072108/lbry-rocksdb/"
	secondaryPath := "asdf"
	db, toDefer, err := db.GetProdDB(dbPath, secondaryPath)
	defer toDefer()
	if err != nil {
		t.Skip("DB not found")
		t.Error(err)
		return
	}

	s := &Server{
		DB:    db,
		Chain: &chaincfg.MainNetParams,
	}

	addrs := []string{
		"bCoyqs8Pv4pss5EbNuyuokkdkCqEpDoHmG",
		"bJr6cLth1UmR7wJ14BMc7ch73xBEEV77fV",
	}

	for _, addr := range addrs {
		req := addressListUnspentReq{addr}
		resp, err := (&req).Handle(s)
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
