package cache

import (
	"container/list"
	"corekv/utils"
	"sync"
	"unsafe"

	"github.com/cespare/xxhash/v2"
)

type Cache struct {
	m         sync.RWMutex
	lru       *windowLRU    // 简单的lru，单链表形式，头先
	slru      *segmentedLRU // 两段list，stage1优先级低，stage2优先级高
	bf        *utils.BloomFilter
	c         *cmSketch
	t         int32
	threshold int32
	data      map[uint64]*list.Element
}

type stringStruct struct {
	str unsafe.Pointer
	len int
}

func NewCache(size int) *Cache {
	const lruPercent = 1
	lrusz := (lruPercent + size) / 100
	lrusz = max(lrusz, 1)

	// LFU设定
	slrusz := float32(size) * ((100 - lruPercent) / 100.0)
	slrusz = max(slrusz, 1)

	// stageone 占比20%
	slru0 := int(0.2 * float64(slrusz))
	slru0 = max(slru0, 1)

	data := make(map[uint64]*list.Element, size)

	return &Cache{
		lru:  newWindowLRU(lrusz, data),
		slru: newSLRU(data, slru0, int(slrusz)-slru0),
		bf:   utils.CreateBloom(size, 0.01),
		c:    newcmSketch(int64(size)),
		data: data,
	}
}

func (c *Cache) Set(key interface{}, value interface{}) bool {
	c.m.Lock()
	defer c.m.Unlock()
	return c.set(key, value)
}

func (c *Cache) Get(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.get(key)
}

func (c *Cache) Del(key interface{}) (interface{}, bool) {
	c.m.Lock()
	defer c.m.Unlock()
	return c.del(key)
}

func (c *Cache) set(key, value interface{}) bool {
	keyHash, conflictHash := c.keyToHash(key)
	i := item{
		stage:    0,
		key:      keyHash,
		conflict: conflictHash,
		value:    value,
	}
	evictedItem, evicted := c.lru.add(i)
	//如果window未满，返回true
	if !evicted {
		return true
	}
	// window中满了，进行淘汰，需要从stageone中找到vic进行pk
	victim := c.slru.victim()
	if victim == nil {
		c.slru.add(evictedItem)
		return true
	}

	if c.bf.Allow(uint32(evictedItem.key)) {
		return true
	}

	vcount := c.c.Estimate(victim.key)
	ocount := c.c.Estimate(evictedItem.key)

	if ocount < vcount {
		return true
	}
	// 从window中淘汰的item需要加入stageone
	c.slru.add(evictedItem)
	return true

}

func (c *Cache) get(key interface{}) (interface{}, bool) {
	c.t++
	if c.t == c.threshold {
		c.c.Reset()
		c.bf.Reset()
		c.t = 0
	}
	keyHash, conflictHash := c.keyToHash(key)
	val, ok := c.data[keyHash]
	if !ok {
		c.bf.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}
	it := val.Value.(*item)

	if it.conflict != conflictHash {
		c.bf.Allow(uint32(keyHash))
		c.c.Increment(keyHash)
		return nil, false
	}
	c.bf.Allow(uint32(keyHash))
	c.c.Increment(it.key)

	v := it.value
	if it.stage == 0 {
		c.lru.get(val)
	} else {
		c.slru.get(val)
	}
	return v, true
}

func (c *Cache) del(key interface{}) (interface{}, bool) {
	keyHash, conflictHash := c.keyToHash(key)

	val, ok := c.data[keyHash]
	if !ok {
		return 0, false
	}
	it := val.Value.(*item)
	if conflictHash != 0 && conflictHash != it.conflict {
		return 0, false
	}
	delete(c.data, keyHash)
	return it.conflict, true
}

func (c *Cache) keyToHash(key interface{}) (uint64, uint64) {
	if key == nil {
		return 0, 0
	}
	switch k := key.(type) {
	case uint64:
		return k, 0
	case string:
		return MemHashString(k), xxhash.Sum64String(k)
	case []byte:
		return MemHash(k), xxhash.Sum64(k)
	case byte:
		return uint64(k), 0
	case int:
		return uint64(k), 0
	case int32:
		return uint64(k), 0
	case uint32:
		return uint64(k), 0
	case int64:
		return uint64(k), 0
	default:
		panic("Key type not supported")
	}
}

func MemHashString(str string) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&str))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

func MemHash(data []byte) uint64 {
	ss := (*stringStruct)(unsafe.Pointer(&data))
	return uint64(memhash(ss.str, 0, uintptr(ss.len)))
}

//go:noescape
//go:linkname memhash runtime.memhash
func memhash(p unsafe.Pointer, h, s uintptr) uintptr

func (c *Cache) String() string {
	var s string
	s += c.lru.String() + "|" + c.slru.String()
	return s
}
