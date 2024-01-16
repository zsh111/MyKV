package lsm

import (
	utilsCache "corekv/utils/cache"
)

type cache struct {
	indexs *utilsCache.Cache
	blocks *utilsCache.Cache
}

type blockBuffer struct {
	b []byte
}

const defaultCacheSize = 1024

func (c *cache) close() error {
	return nil
}

func newCache(opt *Options) *cache {
	return &cache{indexs: utilsCache.NewCache(defaultCacheSize), blocks: utilsCache.NewCache(defaultCacheSize)}
}

func (c *cache) addIndex(fid uint64, t *table) {
	c.indexs.Set(fid, t)
}
