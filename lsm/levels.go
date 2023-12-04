package lsm

import (
	"corekv/file"
	"corekv/utils"
	"corekv/utils/codec"
)

type (
	// 包含配置，cache，manifest
	levelManager struct {
		opt      *Options
		cache    *cache
		manifest *file.Manifest
		levels   []*levelHandler
	}

	// 包含层数和多个table
	levelHandler struct {
		levelNum int // 文件层级
		tables   []*table
	}
)

func (lh *levelHandler) close() error {
	return nil
}

func (lh *levelHandler) Get(key []byte) (*codec.Entry, error) {
	// TODO:
	if lh.levelNum == 0 {

	} else {

	}
	return nil, nil
}

func (lm *levelManager) close() error {
	// 逐个关闭
	if err := lm.cache.close(); err != nil {
		return err
	}
	if err := lm.manifest.Close(); err != nil {
		return err
	}
	for i := range lm.levels {
		if err := lm.levels[i].close(); err != nil {
			return err
		}
	}
	return nil
}

func newLevelManager(opt *Options) *levelManager {
	lm := &levelManager{}
	lm.opt = opt
	lm.loadManifest()
	lm.build()
	return lm
}

func (lm *levelManager) loadCache() {}

func (lm *levelManager) loadManifest() {
	lm.manifest = file.OpenManifest(&file.Options{})
}

func (lm *levelManager) build() {
	lm.levels = make([]*levelHandler, 8)

	// 先从lm中读取opt初始化第0层
	lm.levels[0] = &levelHandler{tables: []*table{openTable(lm.opt)}, levelNum: 0}

	for num := 1; num < utils.MaxLevelNum; num++ {
		lm.levels[num] = &levelHandler{tables: []*table{openTable(lm.opt)}, levelNum: num}
	}

	// 逐一加载sstable的index block 构建cache
	lm.loadCache()
}

func (lm *levelManager) flush(immutable *memTable) error {
	return nil
}

func (lm *levelManager) Get(key []byte) (*codec.Entry, error) {
	var (
		entry *codec.Entry
		err   error
	)
	if entry, err = lm.levels[0].Get(key); entry != nil {
		return entry, err
	}
	for level := 1; level < utils.MaxLevelNum; level++ {
		ld := lm.levels[level]
		if entry, err := ld.Get(key); entry != nil {
			return entry, err
		}
	}
	return entry, nil
}
