package lsm

import (
	"bytes"
	"corekv/iterator"
	"corekv/utils"
	"corekv/utils/codec"
	"fmt"
	"sort"
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

func (iter *Iterator) Seek(key []byte) {

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
	entry := iter.sl.GetEntry([]byte("hello"))
	iter.it = &Item{e: entry}
}
func (iter *memIterator) Item() iterator.Item {
	return iter.it
}
func (iter *memIterator) Close() error {
	return nil
}

func (iter *memIterator) Seek(key []byte) {

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

func (iter *levelIterator) Seek(key []byte) {

}

/*------------------------------------------------------------------*/
// table的迭代器
type ConcatIterator struct{
	idx int
	cur iterator.Iterator
	iters []iterator.Iterator
	tables []*table
	Prefix []byte
	IsAsc bool 
}

func NewConcatIterator(tb []*table,prefix []byte,isAsc bool)*ConcatIterator{
	iters := make([]iterator.Iterator, len(tb))
	return &ConcatIterator{
		Prefix: prefix,
		IsAsc: isAsc,
		tables: tb,
		iters: iters,
		idx: -1,
	}
}

func (s *ConcatIterator)setIdx(idx int){
	s.idx = idx
	if idx < 0 || idx >= len(s.iters) {
		s.cur = nil
		return
	}
	if s.iters[idx] ==nil {
		s.iters[idx] = s.tables[idx].NewIterator(s.Prefix,s.IsAsc)
	}
	s.cur = s.iters[s.idx]
}

func (s *ConcatIterator)Rewind(){
	if len(s.iters) == 0 {
		return
	}
	if !s.IsAsc {
		s.setIdx(0)
	}else{
		s.setIdx(len(s.iters)-1)
	}
	s.cur.Rewind()
}

func (s *ConcatIterator)Valid()bool{
	return s.cur!=nil && s.cur.Valid()
}

func (s *ConcatIterator)Item()iterator.Item{
	return s.cur.Item()
}

func (s *ConcatIterator)Seek(key []byte){
	var idx int
	if s.IsAsc {
		idx  = sort.Search(len(s.tables),func (i int)bool  {
			return bytes.Compare(s.tables[i].ss.GetMaxKey(),key) >= 0
		})
	}else {
		n := len(s.tables)
		idx = n - 1 - sort.Search(n,func (i int)bool  {
			return bytes.Compare(s.tables[n-1-i].ss.GetMinKey(),key) <= 0
		})
	}
	if idx >= len(s.tables) || idx < 0 {
		s.setIdx(-1)
		return
	}
	s.setIdx(idx)
	s.cur.Seek(key)
}

func (s *ConcatIterator)Next(){
	s.cur.Next()
	if s.cur.Valid() {
		return
	}
	for{
		if !s.IsAsc {
			s.setIdx(s.idx + 1)
		}else {
			s.setIdx(s.idx - 1)
		}
		if s.cur == nil {
			return
		}
		s.cur.Rewind()
		if s.cur.Valid() {
			break
		}
	}
}

func (s *ConcatIterator)Close()error{
	for _, it := range s.iters {
		if it == nil {
			continue
		}
		if err := it.Close();err != nil {
			return fmt.Errorf("ConcaIterator:%+v",err)
		}
	}
	return nil
}

