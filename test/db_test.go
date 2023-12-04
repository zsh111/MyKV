package test

import (
	"corekv"
	"corekv/iterator"
	"corekv/utils"
	"corekv/utils/codec"
	"fmt"
	"testing"
	"time"
)

func TestAPI(t *testing.T) {
	opt := utils.NewDefaultOPtions()
	db := corekv.Open(opt)
	defer func() { _ = db.Close() }()

	entry := codec.NewEntry([]byte("corekv"), []byte("study")).WithTTL(time.Second)

	if err := db.Set(entry); err != nil {
		t.Fatal(err)
	}
	// 查询
	if e, err := db.Get(entry.Key); err != nil {
		t.Fatal(err)
	} else {
		t.Logf("db.Get key=%s, value=%s, expiresAt=%d", e.Key, e.Value, e.ExpiresAt)
	}

	//迭代器
	iter := db.NewIterator(&iterator.Options{
		Prefix: []byte("corekv"),
		IsAsc:  false,
	})

	defer func() {
		fmt.Println("执行了")
		_ = iter.Close()
	}()

	for iter.Rewind(); iter.Valid(); iter.Next() {
		it := iter.Item()
		t.Logf("db.NewIterator key=%s, value=%s, expiresAt=%d", it.Entry().Key, it.Entry().Value, it.Entry().ExpiresAt)
	}

	t.Logf("db.Stats.EntryNum=%+v", db.Info().EntryNum)
	if err := db.Del([]byte("corekv")); err != nil {
		t.Fatal(err)
	}

}
