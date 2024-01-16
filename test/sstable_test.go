package test

import (
	"bytes"
	"corekv/lsm"
	"corekv/utils"
	"corekv/utils/codec"
	"fmt"
	"testing"
	"time"
)

var (
	sstopt = &lsm.Options{
		WorkDir:             "/home/zsh/Desktop/Go/work_test/",
		SSTableMaxSize:      1 << 22,
		MemTableSize:        1 << 10,
		BlockSize:           1 << 8,
		BloomFalsePositive:  0.05,
		BaseLevelSize:       1 << 24,
		LevelSizeMultiplier: 10,
		BaseTableSize:       1 << 21,
		TableSizeMultiplier: 2,
		NumLevelZeroTables:  1 << 4,
		MaxLevelNum:         7,
	}
)

// 一个block约60 key
func TestSST(t *testing.T) {
	sl := utils.NewSkipList()
	iteration := 50
	kvlen := 4
	for i := 0; i < iteration; i++ {
		entry := codec.NewEntry(utils.RandBytesChar(kvlen), utils.RandBytesChar(kvlen)).WithTTL(time.Second)
		sl.Add(entry)
	}
	sl.ShowMeSkiplist()
	it := utils.NewSkipListIterator(sl)
	it.Rewind() // 指向yummy
	b := lsm.NewTableBuilder(sstopt)
	for it.Next(); it.Valid(); it.Next() {
		e := it.Item().Entry()
		//fmt.Printf("%v\t", string(e.Key))
		b.Add(e, false)
	}
	//var filename string = sstopt.WorkDir + "0001.sst"
	bo := lsm.NewBlockIterator()
	bl := b.GetBlockList()
	it.Rewind()
	it.Next()
	for _, b2 := range bl {
		bo.SetBlock(b2)
		bo.Rewind()
		for ; bo.Valid(); bo.Next() {
			entry := bo.Item().Entry()
			e := it.Item().Entry()
			if !bytes.Equal(entry.Key, e.Key) {
				fmt.Printf("string(entry.Key): %v\n", string(entry.Key))
			}
			it.Next()
		}
	}
}
