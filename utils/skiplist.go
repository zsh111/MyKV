package utils

import (
	"bytes"
	"corekv/iterator"
	"corekv/utils/codec"
	"fmt"
	"math"
	"math/rand"
	"time"
	"unsafe"
)

const (
	MaxNodeSize = int(unsafe.Sizeof(node{}))
)

type (
	SkipList struct {
		height      int
		num         int   // 表示node的数量
		ref         int32 // 迭代器引用
		headNodeOff uint32
		arena       *Arena
	}

	node struct {
		value     uint64
		keyOffset uint32
		keySize   uint32
		height    uint8
		next      [MaxHeight]uint32
	}
)

func (nd *node) encodeValue(valueOffset uint32, valueSize uint32) {
	nd.value = uint64(valueOffset)<<32 | uint64(valueSize)
}
func (nd *node) decodeValue() (valueOffset uint32, valueSize uint32) {
	valueOffset = uint32(nd.value >> 32)
	valueSize = uint32(nd.value)
	return
}

func putNodeKV(arena *Arena, key []byte, vs *codec.ValueStruct, height int) (uint32, *node) {
	nodeOff := arena.PutNode(height)
	nd := arena.GetNode(nodeOff)
	nd.keyOffset = arena.PutKey(key)
	nd.keySize = uint32(len(key))
	nd.encodeValue(arena.PutValue(vs), vs.EncodeSize())
	// k := arena.GetKey(nd.keyOffset, nd.keySize)
	// v := arena.GetValue(nd.decodeValue())
	// fmt.Printf("string(k): %v\n", string(k))
	// fmt.Printf("string(v.GetValue()): %v\n", string(v.GetValue()))
	AssertTrue(height < math.MaxUint8)
	nd.height = uint8(height)
	return nodeOff, nd
}

func NewSkipList() *SkipList {
	arean := NewArena(InitArenaSize)
	yummy := codec.NewEntry([]byte(""), []byte("")).WithTTL(time.Second)
	nodeOff, _ := putNodeKV(arean, yummy.Key, codec.NewValueStruct(yummy), MaxHeight)
	return &SkipList{
		height:      1,
		num:         0,
		arena:       arean,
		headNodeOff: nodeOff,
	}
}

func (s *SkipList) GetEntry(key []byte) *codec.Entry {
	nd := s.Search(key)
	if nd != nil {
		retKey := s.arena.GetKey(nd.keyOffset, nd.keySize)
		valueoff, valuesize := nd.decodeValue()
		vs := s.arena.GetValue(valueoff, valuesize)
		return codec.NewEntry(retKey, vs.GetValue())
	}
	return nil

}

func (s *SkipList) decodeNode(nd *node) *codec.Entry {
	key := s.arena.GetKey(nd.keyOffset, nd.keySize)
	valueoff, valuesize := nd.decodeValue()
	vs := s.arena.GetValue(valueoff, valuesize)
	return codec.NewEntry(key, vs.GetValue())
}

// 表示节点要被提取的层数,0~height，后续可以使用fastrand优化
func Randextract(height int) int {
	ret := 0
	for ret = 0; ret < height; ret++ {
		if rand.Int()%2 == 0 {
			return ret
		}
	}
	return ret
}

func (s *SkipList) constructSL() {
	height := int(math.Log2(BuildSLThreshold))
	s.height = height
	nodeoff := s.headNodeOff
	yummy := s.arena.GetNode(nodeoff)
	nodeoff = yummy.next[0]
	var cache []uint32 //指向每一层当前node的offset
	for i := 0; i < height; i++ {
		yummy.next[i] = nodeoff
		cache = append(cache, nodeoff)
	}
	nodeoff = s.arena.GetNode(nodeoff).next[0]
	for i := 1; i < BuildSLThreshold && nodeoff != 0; i++ {
		for j := 1; j < height; j++ {
			mask := 1 << j
			if i%mask == 0 {
				s.arena.GetNode(cache[j]).next[j] = nodeoff
				cache[j] = nodeoff
			} else {
				break
			}
		}
		nodeoff = s.arena.GetNode(nodeoff).next[0]
	}

}

func (s *SkipList) Add(entry *codec.Entry) error {
	// 为true不中断
	AssertTrue(len(entry.Key) != 0)
	vs := codec.NewValueStruct(entry)
	var cache []*node
	nodeoff := s.headNodeOff
	preoff := nodeoff
	nodei := s.arena.GetNode(nodeoff)
	prenode := nodei
	cmp := -1
	for i := s.height - 1; i >= 0; i-- {
		for nodeoff != 0 {
			nodei = s.arena.GetNode(nodeoff)
			ndkey := s.arena.GetKey(nodei.keyOffset, nodei.keySize)
			cmp = bytes.Compare(ndkey, entry.Key)
			if cmp > 0 {
				cache = append(cache, prenode)
				nodeoff = preoff
				break
			} else if cmp == 0 {
				valueoff := s.arena.PutValue(vs)
				nodei.encodeValue(valueoff, vs.EncodeSize())
				return nil
			} else {
				prenode = nodei
				preoff = nodeoff
				nodeoff = nodei.next[i]
			}
		}
		if cmp < 0 {
			nodeoff = preoff
			cache = append(cache, prenode)
		}
	}
	s.num++
	initialHeight := int(math.Log2(BuildSLThreshold))
	if len(cache) == 1 {
		insertOff, insertNode := putNodeKV(s.arena, entry.Key, vs, initialHeight)
		insertNode.next[0] = cache[0].next[0]
		cache[0].next[0] = insertOff
	} else {
		levels := Randextract(s.height - 1)
		insertOff, insertNode := putNodeKV(s.arena, entry.Key, vs, levels+1)
		for i := 0; i <= levels; i++ {
			insertNode.next[i] = cache[s.height-1-i].next[i]
			cache[s.height-1-i].next[i] = insertOff
		}
	}
	if s.height >= initialHeight && s.height < int(math.Log2(float64(s.num))) {
		s.height++
	}
	if s.num == BuildSLThreshold && s.height == 1 {
		s.constructSL()
	}
	return nil
}

func (s *SkipList) ShowMeSkiplist() {
	head := s.arena.GetNode(s.headNodeOff)
	var nodeoff uint32
	Print("下图为skiplist：")
	for i := 0; i < s.height; i++ {
		fmt.Printf("level %d 的node为： ", i)
		pre := s.arena.GetKey(head.keyOffset, head.keySize)
		nodeoff = head.next[i]
		for nodeoff != 0 {
			nd := s.arena.GetNode(nodeoff)
			ndkey := s.arena.GetKey(nd.keyOffset, nd.keySize)
			fmt.Printf("%s---", string(ndkey))
			if bytes.Compare(pre, ndkey) > 0 {
				fmt.Errorf("err skiplist is not sort ", i)
			}
			nodeoff = nd.next[i]
		}
		fmt.Printf("\n")
	}
}

func (s *SkipList) Search(key []byte) *node {
	nodeoff := s.headNodeOff
	preoff := nodeoff
	nd := s.arena.GetNode(s.headNodeOff)
	pre := nd
	for i := s.height - 1; i >= 0; i-- {
		nd = pre
		nodeoff = preoff
		for nodeoff != 0 {
			ndkey := s.arena.GetKey(nd.keyOffset, nd.keySize)
			ret := bytes.Compare(ndkey, key)
			if ret < 0 {
				pre = nd
				preoff = nodeoff
				nodeoff = nd.next[i]
				nd = s.arena.GetNode(nodeoff)
			} else if ret == 0 {
				return nd
			} else {
				break
			}
		}
	}
	fmt.Println("this key is not exist")
	return nil
}

func (s *SkipList) Update(entry *codec.Entry) {
	nd := s.Search(entry.Key)
	vs := codec.NewValueStruct(entry)
	if nd != nil {
		valueoff := s.arena.PutValue(vs)
		nd.encodeValue(valueoff, vs.EncodeSize())
	} else {
		fmt.Println("This entry is not exist,suggest add")
	}
}

func (s *SkipList) Delete(key []byte) bool {
	var label bool = false
	nodeoff := s.headNodeOff
	preoff := nodeoff
	nd := s.arena.GetNode(s.headNodeOff)
	pre := nd
	for i := s.height - 1; i >= 0; i-- {
		nd = pre
		nodeoff = preoff
		for nodeoff != 0 {
			ndkey := s.arena.GetKey(nd.keyOffset, nd.keySize)
			ret := bytes.Compare(ndkey, key)
			if ret < 0 {
				pre = nd
				preoff = nodeoff
				nodeoff = nd.next[i]
				nd = s.arena.GetNode(nodeoff)
			} else if ret == 0 {
				label = true
				pre.next[i] = nd.next[i]
				break
			} else {
				break
			}
		}
	}
	if label {
		fmt.Println("success delete")
		return label
	} else {
		fmt.Println("this key is not exist")
		return label
	}

}

/*----------------------范围查询----------------------*/

/*-----------------下面是无用函数--------------------*/

/*----------------------迭代器----------------------*/

type SkipListIterator struct {
	list *SkipList //迭代器对象
	nd   *node     // yummy node用作迭代
}

func NewSkipListIterator(sl *SkipList) *SkipListIterator {
	return &SkipListIterator{
		list: sl,
		nd:   nil,
	}
}

func (s *SkipList) Close() error {
	return nil
}

// 将iterator置head
func (s *SkipListIterator) Rewind() {
	// 初始化为yummy node
	s.nd = s.list.arena.GetNode(s.list.headNodeOff)
}

func (s *SkipListIterator) Item() iterator.Item {
	return s.list.decodeNode(s.nd)
}

func (s *SkipListIterator) Valid() bool {
	return s.nd != nil
}

func (s *SkipListIterator) Next() {
	nextNodeOff := s.nd.next[0]
	s.nd = s.list.arena.GetNode(nextNodeOff)
}

func (s *SkipListIterator) Seek(key []byte) {
	s.nd = s.list.Search(key)
}
