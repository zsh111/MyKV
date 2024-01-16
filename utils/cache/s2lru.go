package cache

import (
	"container/list"
	"fmt"
)

type segmentedLRU struct {
	data                     map[uint64]*list.Element
	stageOneCap, stageTwoCap int
	stageOne, stageTwo       *list.List
}

const (
	STAGE_ONE = iota + 1
	STAGE_TWO
)

func newSLRU(data map[uint64]*list.Element, stageOneCap, stageTwoCap int) *segmentedLRU {
	return &segmentedLRU{
		data:        data,
		stageOneCap: stageOneCap,
		stageTwoCap: stageTwoCap,
		stageOne:    list.New(),
		stageTwo:    list.New(),
	}
}

func (slru *segmentedLRU) add(newitem item) {
	newitem.stage = 1
	if slru.stageOne.Len() < slru.stageOneCap || slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		slru.data[newitem.key] = slru.stageOne.PushFront(&newitem)
		return
	}
	// 如果stageOne满了
	e := slru.stageOne.Back()
	it := e.Value.(*item)
	delete(slru.data, it.key)

	*it = newitem
	slru.data[newitem.key] = e
	slru.stageOne.MoveToFront(e)
}

func (slru *segmentedLRU) get(v *list.Element) {
	Item := v.Value.(*item)
	if Item.stage == STAGE_TWO {
		slru.stageTwo.MoveToFront(v)
		return
	}
	if slru.stageTwo.Len() < slru.stageTwoCap {
		slru.stageOne.Remove(v)
		Item.stage = STAGE_TWO
		slru.data[Item.key] = slru.stageTwo.PushFront(v)
		return
	}
	// 这里将v提权到stageTwo front
	back := slru.stageTwo.Back()
	itemBack := back.Value.(*item)

	*itemBack, *Item = *Item, *itemBack
	Item.stage = STAGE_TWO

	slru.data[Item.key] = v
	slru.data[itemBack.key] = back

	slru.stageOne.MoveToFront(v)
	slru.stageTwo.MoveToFront(back)

}

func (slru *segmentedLRU) Len() int {
	return slru.stageOne.Len() + slru.stageTwo.Len()
}

func (slru *segmentedLRU) victim() *item {
	if slru.Len() < slru.stageOneCap+slru.stageTwoCap {
		return nil
	}
	v := slru.stageOne.Back()
	return v.Value.(*item)
}

func (slru *segmentedLRU) String() string {
	var s string
	for e := slru.stageTwo.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*item).value)
	}
	s += fmt.Sprintf(" | ")
	for e := slru.stageOne.Front(); e != nil; e = e.Next() {
		s += fmt.Sprintf("%v,", e.Value.(*item).value)
	}
	return s
}
