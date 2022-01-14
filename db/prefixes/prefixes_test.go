package prefixes_test

import (
	"bytes"
	"encoding/csv"
	"encoding/hex"
	"fmt"
	"log"
	"os"
	"testing"

	dbpkg "github.com/lbryio/hub/db"
	"github.com/lbryio/hub/db/prefixes"
	"github.com/linxGnu/grocksdb"
)

func testInit(filePath string) (*grocksdb.DB, [][]string, func()) {
	log.Println(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Println(err)
	}

	// wOpts := grocksdb.NewDefaultWriteOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "tmp")
	if err != nil {
		log.Println(err)
	}
	toDefer := func() {
		db.Close()
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}

	return db, records, toDefer
}

func TestSupportToClaim(t *testing.T) {

	filePath := "../../resources/support_to_claim.csv"

	wOpts := grocksdb.NewDefaultWriteOptions()
	db, records, toDefer := testInit(filePath)
	defer toDefer()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.SupportToClaim}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.SupportToClaimKey).PackKey()

		keyPartial1 := prefixes.SupportToClaimKeyPackPartial(kv.Key.(*prefixes.SupportToClaimKey), 1)
		keyPartial2 := prefixes.SupportToClaimKeyPackPartial(kv.Key.(*prefixes.SupportToClaimKey), 2)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial2) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial2, gotKey)
		}

		got := kv.Value.(*prefixes.SupportToClaimValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		got := kv.Value.(*prefixes.SupportToClaimValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestClaimExpiration(t *testing.T) {

	filePath := "../../resources/claim_expiration.csv"

	wOpts := grocksdb.NewDefaultWriteOptions()
	db, records, toDefer := testInit(filePath)
	defer toDefer()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.ClaimExpiration}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.ClaimExpirationKey).PackKey()

		keyPartial1 := prefixes.ClaimExpirationKeyPackPartial(kv.Key.(*prefixes.ClaimExpirationKey), 1)
		keyPartial2 := prefixes.ClaimExpirationKeyPackPartial(kv.Key.(*prefixes.ClaimExpirationKey), 2)
		keyPartial3 := prefixes.ClaimExpirationKeyPackPartial(kv.Key.(*prefixes.ClaimExpirationKey), 3)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial2) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial2, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial3) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial3, gotKey)
		}

		got := kv.Value.(*prefixes.ClaimExpirationValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		got := kv.Value.(*prefixes.ClaimExpirationValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestClaimTakeover(t *testing.T) {

	filePath := "../../resources/claim_takeover.csv"

	wOpts := grocksdb.NewDefaultWriteOptions()
	db, records, toDefer := testInit(filePath)
	defer toDefer()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.ClaimTakeover}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.ClaimTakeoverKey).PackKey()

		keyPartial1 := prefixes.ClaimTakeoverKeyPackPartial(kv.Key.(*prefixes.ClaimTakeoverKey), 1)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}

		got := kv.Value.(*prefixes.ClaimTakeoverValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		got := kv.Value.(*prefixes.ClaimTakeoverValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestPendingActivation(t *testing.T) {

	filePath := "../../resources/pending_activation.csv"

	wOpts := grocksdb.NewDefaultWriteOptions()
	db, records, toDefer := testInit(filePath)
	defer toDefer()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.PendingActivation}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.PendingActivationKey).PackKey()

		keyPartial1 := prefixes.PendingActivationKeyPackPartial(kv.Key.(*prefixes.PendingActivationKey), 1)
		keyPartial2 := prefixes.PendingActivationKeyPackPartial(kv.Key.(*prefixes.PendingActivationKey), 2)
		keyPartial3 := prefixes.PendingActivationKeyPackPartial(kv.Key.(*prefixes.PendingActivationKey), 3)
		keyPartial4 := prefixes.PendingActivationKeyPackPartial(kv.Key.(*prefixes.PendingActivationKey), 4)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial2) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial2, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial3) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial3, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial4) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial4, gotKey)
		}

		got := kv.Value.(*prefixes.PendingActivationValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		got := kv.Value.(*prefixes.PendingActivationValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestActivated(t *testing.T) {

	filePath := "../../resources/activated_claim_and_support.csv"

	wOpts := grocksdb.NewDefaultWriteOptions()
	db, records, toDefer := testInit(filePath)
	defer toDefer()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.ActivatedClaimAndSupport}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.ActivationKey).PackKey()

		keyPartial1 := prefixes.ActivationKeyPackPartial(kv.Key.(*prefixes.ActivationKey), 1)
		keyPartial2 := prefixes.ActivationKeyPackPartial(kv.Key.(*prefixes.ActivationKey), 2)
		keyPartial3 := prefixes.ActivationKeyPackPartial(kv.Key.(*prefixes.ActivationKey), 3)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial2) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial2, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial3) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial3, gotKey)
		}

		got := kv.Value.(*prefixes.ActivationValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		got := kv.Value.(*prefixes.ActivationValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestActiveAmount(t *testing.T) {

	filePath := "../../resources/active_amount.csv"

	wOpts := grocksdb.NewDefaultWriteOptions()
	db, records, toDefer := testInit(filePath)
	defer toDefer()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.ActiveAmount}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.ActiveAmountKey).PackKey()

		keyPartial1 := prefixes.ActiveAmountKeyPackPartial(kv.Key.(*prefixes.ActiveAmountKey), 1)
		keyPartial2 := prefixes.ActiveAmountKeyPackPartial(kv.Key.(*prefixes.ActiveAmountKey), 2)
		keyPartial3 := prefixes.ActiveAmountKeyPackPartial(kv.Key.(*prefixes.ActiveAmountKey), 3)
		keyPartial4 := prefixes.ActiveAmountKeyPackPartial(kv.Key.(*prefixes.ActiveAmountKey), 4)
		keyPartial5 := prefixes.ActiveAmountKeyPackPartial(kv.Key.(*prefixes.ActiveAmountKey), 5)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial2) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial2, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial3) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial3, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial4) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial4, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial5) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial5, gotKey)
		}

		got := kv.Value.(*prefixes.ActiveAmountValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		got := kv.Value.(*prefixes.ActiveAmountValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestEffectiveAmount(t *testing.T) {

	filePath := "../../resources/effective_amount.csv"

	wOpts := grocksdb.NewDefaultWriteOptions()
	db, records, toDefer := testInit(filePath)
	defer toDefer()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.EffectiveAmount}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.EffectiveAmountKey).PackKey()

		keyPartial1 := prefixes.EffectiveAmountKeyPackPartial(kv.Key.(*prefixes.EffectiveAmountKey), 1)
		keyPartial2 := prefixes.EffectiveAmountKeyPackPartial(kv.Key.(*prefixes.EffectiveAmountKey), 2)
		keyPartial3 := prefixes.EffectiveAmountKeyPackPartial(kv.Key.(*prefixes.EffectiveAmountKey), 3)
		keyPartial4 := prefixes.EffectiveAmountKeyPackPartial(kv.Key.(*prefixes.EffectiveAmountKey), 4)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial2) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial2, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial3) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial3, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial4) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial4, gotKey)
		}

		got := kv.Value.(*prefixes.EffectiveAmountValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		got := kv.Value.(*prefixes.EffectiveAmountValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestRepost(t *testing.T) {

	filePath := "../../resources/repost.csv"

	wOpts := grocksdb.NewDefaultWriteOptions()
	db, records, toDefer := testInit(filePath)
	defer toDefer()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.Repost}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.RepostKey).PackKey()

		keyPartial1 := prefixes.RepostKeyPackPartial(kv.Key.(*prefixes.RepostKey), 1)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}

		got := kv.Value.(*prefixes.RepostValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		got := kv.Value.(*prefixes.RepostValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestRepostedClaim(t *testing.T) {

	filePath := "../../resources/reposted_claim.csv"

	wOpts := grocksdb.NewDefaultWriteOptions()
	db, records, toDefer := testInit(filePath)
	defer toDefer()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.RepostedClaim}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.RepostedKey).PackKey()

		keyPartial3 := prefixes.RepostedKeyPackPartial(kv.Key.(*prefixes.RepostedKey), 3)
		keyPartial2 := prefixes.RepostedKeyPackPartial(kv.Key.(*prefixes.RepostedKey), 2)
		keyPartial1 := prefixes.RepostedKeyPackPartial(kv.Key.(*prefixes.RepostedKey), 1)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial3) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial3, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial2) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial2, gotKey)
		}
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}

		got := kv.Value.(*prefixes.RepostedValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		got := kv.Value.(*prefixes.RepostedValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestClaimDiff(t *testing.T) {

	filePath := "../../resources/claim_diff.csv"

	log.Println(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Println(err)
	}

	wOpts := grocksdb.NewDefaultWriteOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "tmp")
	if err != nil {
		log.Println(err)
	}
	defer func() {
		db.Close()
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.ClaimDiff}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		// log.Println(kv.Key)
		gotKey := kv.Key.(*prefixes.TouchedOrDeletedClaimKey).PackKey()

		keyPartial1 := prefixes.TouchedOrDeletedClaimKeyPackPartial(kv.Key.(*prefixes.TouchedOrDeletedClaimKey), 1)

		// Check pack partial for sanity
		if !bytes.HasPrefix(gotKey, keyPartial1) {
			t.Errorf("%+v should be prefix of %+v\n", keyPartial1, gotKey)
		}

		got := kv.Value.(*prefixes.TouchedOrDeletedClaimValue).PackValue()
		wantKey, err := hex.DecodeString(records[i][0])
		if err != nil {
			log.Println(err)
		}
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(gotKey, wantKey) {
			t.Errorf("gotKey: %+v, wantKey: %+v\n", got, want)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		log.Println(kv.Key)
		got := kv.Value.(*prefixes.TouchedOrDeletedClaimValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestUTXO(t *testing.T) {

	filePath := "../../resources/utxo.csv"

	log.Println(filePath)
	file, err := os.Open(filePath)
	if err != nil {
		log.Println(err)
	}
	reader := csv.NewReader(file)
	records, err := reader.ReadAll()
	if err != nil {
		log.Println(err)
	}

	wOpts := grocksdb.NewDefaultWriteOptions()
	opts := grocksdb.NewDefaultOptions()
	opts.SetCreateIfMissing(true)
	db, err := grocksdb.OpenDb(opts, "tmp")
	if err != nil {
		log.Println(err)
	}
	defer func() {
		db.Close()
		err = os.RemoveAll("./tmp")
		if err != nil {
			log.Println(err)
		}
	}()
	for _, record := range records {
		key, err := hex.DecodeString(record[0])
		if err != nil {
			log.Println(err)
		}
		val, err := hex.DecodeString(record[1])
		if err != nil {
			log.Println(err)
		}
		db.Put(wOpts, key, val)
	}
	// test prefix
	options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.UTXO}).WithIncludeValue(true)
	ch := dbpkg.Iter(db, options)
	var i = 0
	for kv := range ch {
		log.Println(kv.Key)
		got := kv.Value.(*prefixes.UTXOValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}

	// Test start / stop
	start, err := hex.DecodeString(records[0][0])
	if err != nil {
		log.Println(err)
	}
	stop, err := hex.DecodeString(records[9][0])
	if err != nil {
		log.Println(err)
	}
	options2 := dbpkg.NewIterateOptions().WithStart(start).WithStop(stop).WithIncludeValue(true)
	ch2 := dbpkg.Iter(db, options2)
	i = 0
	for kv := range ch2 {
		log.Println(kv.Key)
		got := kv.Value.(*prefixes.UTXOValue).PackValue()
		want, err := hex.DecodeString(records[i][1])
		if err != nil {
			log.Println(err)
		}
		if !bytes.Equal(got, want) {
			t.Errorf("got: %+v, want: %+v\n", got, want)
		}
		i++
	}
}

func TestHashXUTXO(t *testing.T) {

	tests := []struct {
		name     string
		filePath string
	}{
		{
			name:     "Read HashX_UTXO correctly",
			filePath: "../../resources/hashx_utxo.csv",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			log.Println(tt.filePath)
			file, err := os.Open(tt.filePath)
			if err != nil {
				log.Println(err)
			}
			reader := csv.NewReader(file)
			records, err := reader.ReadAll()
			if err != nil {
				log.Println(err)
			}

			wOpts := grocksdb.NewDefaultWriteOptions()
			opts := grocksdb.NewDefaultOptions()
			opts.SetCreateIfMissing(true)
			db, err := grocksdb.OpenDb(opts, "tmp")
			if err != nil {
				log.Println(err)
			}
			defer func() {
				db.Close()
				err = os.RemoveAll("./tmp")
				if err != nil {
					log.Println(err)
				}
			}()
			for _, record := range records {
				key, err := hex.DecodeString(record[0])
				if err != nil {
					log.Println(err)
				}
				val, err := hex.DecodeString(record[1])
				if err != nil {
					log.Println(err)
				}
				db.Put(wOpts, key, val)
			}
			start, err := hex.DecodeString(records[0][0])
			if err != nil {
				log.Println(err)
			}
			options := dbpkg.NewIterateOptions().WithPrefix([]byte{prefixes.HashXUTXO}).WithStart(start).WithIncludeValue(true)
			ch := dbpkg.Iter(db, options)
			var i = 0
			for kv := range ch {
				log.Println(kv.Key)
				got := kv.Value.(*prefixes.HashXUTXOValue).PackValue()
				want, err := hex.DecodeString(records[i][1])
				if err != nil {
					log.Println(err)
				}
				if !bytes.Equal(got, want) {
					t.Errorf("got: %+v, want: %+v\n", got, want)
				}
				i++
				if i > 9 {
					return
				}
			}
		})
	}
}

func TestUTXOKey_String(t *testing.T) {
	tests := []struct {
		name   string
		prefix []byte
		hashx  []byte
		txnum  uint32
		nout   uint16
		want   string
	}{
		{
			name:   "Converts to string",
			prefix: []byte("u"),
			hashx:  []byte("AAAAAAAAAA"),
			txnum:  0,
			nout:   0,
			want:   "*prefixes.UTXOKey(hashX=41414141414141414141, tx_num=0, nout=0)",
		},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			key := &prefixes.UTXOKey{
				Prefix: tt.prefix,
				HashX:  tt.hashx,
				TxNum:  tt.txnum,
				Nout:   tt.nout,
			}

			got := fmt.Sprint(key)
			log.Println(got)
			if got != tt.want {
				t.Errorf("got: %s, want: %s\n", got, tt.want)
			}
		})
	}
}
