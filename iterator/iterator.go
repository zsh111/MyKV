package iterator

import "corekv/utils/codec"

type Iterator interface {
	Next()
	Valid() bool
	Rewind()
	Item() Item
	Close() error
	Seek(key []byte)
}

type Item interface {
	Entry() *codec.Entry
}

type Options struct {
	Prefix []byte
	IsAsc  bool
}
