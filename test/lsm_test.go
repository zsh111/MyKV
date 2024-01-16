package test

import (
	"bytes"
	"corekv/file"
	"corekv/lsm"
	"corekv/utils"
	"corekv/utils/codec"
	"fmt"
	"os"
	"testing"
	"time"
)

// const tableSize = 1 << 20

var (
	// 初始化opt
	opt = &lsm.Options{
		WorkDir:            "/home/zsh/Desktop/Go/work_test/",
		SSTableMaxSize:     1024,
		MemTableSize:       1024,
		BlockSize:          1024,
		BloomFalsePositive: 0.05,
		BaseLevelSize: 1 << 20,
		LevelSizeMultiplier: 10,
		BaseTableSize: 1 << 20,
		TableSizeMultiplier: 2,
		NumLevelZeroTables: 15,
		MaxLevelNum: 7,
	}
)

func TestLsm(t *testing.T) {
	// 简单测试插入key和查询，不涉及序列化和反序列化
	file.ClearDir(opt.WorkDir)
	lsm := buildLSM()
	test := func() {
		baseTest(t, lsm, 128)
	}
	runTest(1, test)
}

func TestClose(t *testing.T){
	clearDir()
	lsm := buildLSM()
	testNil := func ()  {
		utils.CondPanic(lsm.Set(nil)!= utils.ErrKeyEmpty,fmt.Errorf("[testNil] lsm.set(nil) != err"))
		_,err := lsm.Get(nil)
		utils.CondPanic(err != utils.ErrKeyEmpty,fmt.Errorf("[testNil] lsm.set(nil) != err"))
	}
	runTest(1,testNil)
}



func baseTest(t *testing.T, lsm *lsm.LSM, n int) {
	key := []byte("CRTS😁硬核课堂MrGSBtL12345678")
	value := []byte("我擦了")
	e := codec.NewEntry(key, value).WithTTL(time.Second)
	lsm.Set(e)
	for i := 0; i < n; i++ {
		k := utils.RandBytesChar(kvlen)
		v := utils.RandBytesChar(kvlen)
		entry := codec.NewEntry(k, v).WithTTL(time.Second)
		lsm.Set(entry)
	}
	v, err := lsm.Get(e.Key)
	utils.Panic(err)
	utils.CondPanic(!bytes.Equal(e.Value, v.Value), fmt.Errorf("lsm.Get(e.Key) value not equal !!!"))
}

func runTest(n int, testFunList ...func()) {
	for _, f := range testFunList {
		for i := 0; i < n; i++ {
			f()
		}
	}
}


func buildLSM() *lsm.LSM {
	c := make(chan map[uint32]int64, 16)
	opt.DiscardStatsCh = &c
	lsm := lsm.NewLSM(opt)
	return lsm
}

// 清空dir
func clearDir(){
	_,err := os.Stat(opt.WorkDir)
	if err == nil {
		os.RemoveAll(opt.WorkDir)
	}
	os.Mkdir(opt.WorkDir,os.ModePerm)
}