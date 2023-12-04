package utils

import (
	"bytes"
	"corekv/utils/codec"
	"fmt"
	"math"
	"math/rand"
	"time"
)

const (
	maxHeight        = 20
	buildSLThreshold = 32
)

type (
	SkipList struct {
		height int
		num    int // 表示node的数量
		head   *node
	}

	node struct {
		member *codec.Entry
		next   []*node
	}
)

func NewSkipList() *SkipList {
	// 创建一个虚拟node
	entry := codec.NewEntry([]byte(""), []byte("")).WithTTL(time.Second)
	return &SkipList{
		height: 1,
		num:    0,
		head: &node{
			member: entry,
			next:   make([]*node, maxHeight),
		},
	}
}

func (s *SkipList) Close() error {
	return nil
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
	height := int(math.Log2(buildSLThreshold))
	s.height = height
	cache := make([]*node, height)
	nd := s.head.next[0]
	for i := 0; i < height; i++ {
		s.head.next[i] = nd
		cache[i] = nd
	}
	nd = nd.next[0]
	for i := 1; i < buildSLThreshold; i++ {
		for j := 1; j < int(height); j++ {
			mask := 1 << j
			if i%mask == 0 {
				cache[j].next[j] = nd
				cache[j] = cache[j].next[j]
			} else {
				break
			}
		}
		nd = nd.next[0]
	}
}

func (s *SkipList) AddUnique(entry *codec.Entry) error {
	insertNode := &node{member: entry, next: make([]*node, maxHeight)}
	var cache []*node
	var label bool = false
	nodei := s.head
	pre := nodei
	cmp := -1
	for i := s.height - 1; i >= 0; i-- {
		for nodei != nil {
			cmp = bytes.Compare(nodei.member.Key, entry.Key)
			if cmp > 0 {
				break
			} else if cmp == 0 {
				break
			} else {
				pre = nodei
				nodei = nodei.next[i]
			}
		}
		if cmp == 0 {
			label = true
			cache = append(cache, nodei)
		} else {
			cache = append(cache, pre)
			nodei = pre
		}
	}
	if len(cache) == 1 {
		if label {
			cache[0].member.Value = entry.Value
		} else {
			insertNode.next[0] = cache[0].next[0]
			cache[0].next[0] = insertNode
			s.num++
		}
	} else {
		// 我们对重复key不进行提层
		levels := Randextract(s.height - 1)
		for i := 0; i <= levels; i++ {
			if !label {
				insertNode.next[i] = cache[s.height-1-i].next[i]
				cache[s.height-1-i].next[i] = insertNode
			} else {
				cache[s.height-1-i].member.Value = entry.Value
				s.num++
				break
			}
		}
	}
	if s.height >= int(math.Log2(buildSLThreshold)) && s.height < int(math.Log2(float64(s.num))) {
		s.height++
		s.head.next[s.height-1] = s.head.next[0]
	}
	if s.num == buildSLThreshold && s.height == 1 {
		s.constructSL()
	}
	return nil
}

func (s *SkipList) ShowMeSkiplist() {
	Print("下图为skiplist：")
	for i := 0; i < s.height; i++ {
		fmt.Printf("level %d 的node为： ", i)
		pre := s.head.member.Key
		nd := s.head.next[i]
		for nd != nil {
			fmt.Printf("%s---", string(nd.member.Key))
			if bytes.Compare(pre, nd.member.Key) > 0 {
				fmt.Errorf("err skiplist is not sort ", i)
			}
			nd = nd.next[i]
		}
		fmt.Printf("\n")
	}
}

func (s *SkipList) Search(key []byte) *codec.Entry {
	nd := s.head
	pre := nd
	for i := s.height - 1; i >= 0; i-- {
		nd = pre
		for nd != nil {
			ret := bytes.Compare(nd.member.Key, key)
			if ret < 0 {
				pre = nd
				nd = nd.next[i]
			} else if ret == 0 {
				return nd.member
			} else {
				nd = pre
				break
			}
		}
	}
	fmt.Println("this key is not exist")
	return nil
}

func (s *SkipList) Update(entry *codec.Entry) {
	ent := s.Search(entry.Key)
	if ent != nil {
		ent.Value = entry.Value
		ent.ExpiresAt = entry.ExpiresAt
	} else {
		fmt.Println("This entry is not exist,suggest add")
	}
}

func (s *SkipList)Delete(entry *codec.Entry){
	if s.Search(entry.Key)==nil{
		fmt.Errorf("this key is not exist")
	}
	
}


/*-----------------下面是无用函数--------------------*/
func (s *SkipList) levelSearch(nd *node, key []byte, level int) (bool, *node) {
	for nd.next[level] != nil {
		cmp := bytes.Compare(nd.next[level].member.Key, key)
		if cmp < 0 {
			nd = nd.next[level]
		} else if cmp == 0 {
			return true, nd.next[level]
		} else {
			return false, nd
		}
	}
	return false, nd
}

// 允许重复key的添加
func (s *SkipList) Add(entry *codec.Entry) error {
	insertNode := &node{member: entry, next: make([]*node, maxHeight)}
	s.num += 1
	begin := s.head
	//缓存每一层找到的前一个节点,如果需要进行向上提取就使用cache
	var cache []*node
	nodei := begin
	for i := s.height - 1; i >= 0; i-- {
		for nodei.next[i] != nil {
			cmp := bytes.Compare(nodei.next[i].member.Key, entry.Key)
			if cmp > 0 {
				break
			} else {
				nodei = nodei.next[i]
			}
		}
		cache = append(cache, nodei)
	}
	if len(cache) == 1 {
		insertNode.next[0] = cache[0].next[0]
		cache[0].next[0] = insertNode
	} else {
		levels := Randextract(s.height - 1)
		// 从下层向上层添加node，而cache是由上层向下层添加的
		//fmt.Println("插入的层数为：",levels,string(entry.Key))
		for i := 0; i <= levels; i++ {
			//fmt.Print("\t","完成对第",i,"层node插入\t")
			insertNode.next[i] = cache[s.height-1-i].next[i]
			cache[s.height-1-i].next[i] = insertNode
		}
	}
	if s.height >= int(math.Log2(buildSLThreshold)) && s.height < int(math.Log2(float64(s.num))) {
		s.height++
		s.head.next[s.height-1] = s.head.next[0]
	}
	// 当最下层节点大于256，我们对节点做一定整理，直接构建一个多层链表
	if s.num == buildSLThreshold && s.height == 1 {
		s.constructSL()
		s.ShowMeSkiplist()
	}
	return nil
}