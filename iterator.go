package corekv

import (
	"corekv/iterator"
	"corekv/utils/codec"
)

type DBIterator struct {
	iters []iterator.Iterator
}

type Item struct {
	e *codec.Entry
}

func (it *Item) Entry() *codec.Entry {
	return it.e
}

func (db *DB) NewIterator(opt *iterator.Options) iterator.Iterator {
	dbiter := &DBIterator{}
	dbiter.iters = make([]iterator.Iterator, 0)
	dbiter.iters = append(dbiter.iters, db.lsm.NewIterator(opt))
	return dbiter
}

func (iter *DBIterator) Next() {
	iter.iters[0].Next()
}

func (iter *DBIterator) Valid() bool {
	return iter.iters[0].Valid()
}

func (iter *DBIterator) Rewind() {
	iter.iters[0].Rewind()
}

func (iter *DBIterator) Item() iterator.Item {
	return iter.iters[0].Item()
}

func (iter *DBIterator) Close() error {
	return nil
}
