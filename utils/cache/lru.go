package cache

import (
	"container/list"
	"fmt"
)

type windowLRU struct {
	data map[uint64]*list.Element
	cap  int
	l    *list.List
}

type item struct {
	stage    int
	key      uint64
	conflict uint64
	value    interface{}
}

func newWindowLRU(size int, data map[uint64]*list.Element) *windowLRU {
	return &windowLRU{
		data: data,
		cap:  size,
		l:    list.New(),
	}
}

//subtitle:true add:false
func (lru *windowLRU) add(newitem item) (eitem item, evicted bool) {
	if lru.l.Len() < lru.cap {
		// 直接加到队列头
		lru.data[newitem.key] = lru.l.PushFront(&newitem)
		return item{}, false
	}
	evictedItem := lru.l.Back()
	it := evictedItem.Value.(*item)
	delete(lru.data, it.key)

	// 直接将newitem加到尾部，减少gc
	eitem, *it = *it, newitem

	lru.data[newitem.key] = evictedItem

	lru.l.MoveToFront(evictedItem)
	return eitem, true
}

// 提权
func (lru *windowLRU) get(v *list.Element) {
	lru.l.MoveToFront(v)
}

func (lru *windowLRU) String() string {
	var s string
	for e := lru.l.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*item).value)
	}
	return s
}
