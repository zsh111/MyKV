package lsm

type cache struct{}

func (c *cache) close() error {
	return nil
}

func newCache(opt *Options) *cache {
	return &cache{}
}
