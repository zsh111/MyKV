package lsm

import (
	"corekv/iterator"
	"corekv/utils"
	"corekv/utils/codec"
)

type (
	Iterator struct {
		it    iterator.Item       // 接口返回一个entry
		iters []iterator.Iterator // 多个迭代器接口切片
	}

	Item struct {
		e *codec.Entry
	}

	memIterator struct {
		it    iterator.Item
		iters []*Iterator
		sl    *utils.SkipList
	}

	levelIterator struct {
		it    *iterator.Item
		iters []*Iterator
	}
)

func (it *Item) Entry() *codec.Entry {
	return it.e
}

// 创建迭代器
func (lsm *LSM) NewIterator(opt *iterator.Options) iterator.Iterator {
	iter := &Iterator{}
	iter.iters = make([]iterator.Iterator, 0)
	iter.iters = append(iter.iters, lsm.memTable.NewIterator(opt))

	for _, imm := range lsm.immutables {
		iter.iters = append(iter.iters, imm.NewIterator(opt))
	}
	iter.iters = append(iter.iters, lsm.levels.NewIterator(opt))
	return iter
}

func (iter *Iterator) Next() {
	iter.iters[0].Next()
}

func (iter *Iterator) Valid() bool {
	return iter.iters[0].Valid()
}

func (iter *Iterator) Rewind() {
	iter.iters[0].Rewind()
}

func (iter *Iterator) Item() iterator.Item {
	return iter.iters[0].Item()
}

func (iter *Iterator) Close() error {
	return nil
}

func (m *memTable) NewIterator(opt *iterator.Options) iterator.Iterator {
	return &memIterator{sl: m.sl}
}

func (iter *memIterator) Next() {
	iter.it = nil
}
func (iter *memIterator) Valid() bool {
	return iter.it != nil
}
func (iter *memIterator) Rewind() {
	entry := iter.sl.Search([]byte("hello"))
	iter.it = &Item{e: entry}
}
func (iter *memIterator) Item() iterator.Item {
	return iter.it
}
func (iter *memIterator) Close() error {
	return nil
}

func (lm *levelManager) NewIterator(options *iterator.Options) iterator.Iterator {
	return &levelIterator{}
}
func (iter *levelIterator) Next() {
}
func (iter *levelIterator) Valid() bool {
	return false
}
func (iter *levelIterator) Rewind() {

}
func (iter *levelIterator) Item() iterator.Item {
	return &Item{}
}
func (iter *levelIterator) Close() error {
	return nil
}
