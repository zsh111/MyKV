package test

import (
	"bytes"
	"corekv/utils"
	"corekv/utils/codec"
	"fmt"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
)

func TestSkiplistBuild(t *testing.T) {
	s := utils.NewSkipList()
	iteration := 1000
	kvlen := 3
	for i := 0; i < iteration; i++ {
		entry := codec.NewEntry(utils.RandBytesInt(kvlen), utils.RandBytesInt(kvlen)).WithTTL(time.Second)
		s.Add(entry)
	}
	s.ShowMeSkiplist()
	fmt.Println("show list finish")
}

func TestSearch(t *testing.T) {
	s := utils.NewSkipList()
	kvlen := 3
	for i := 0; i < iteration; i++ {
		key := utils.RandBytesInt(kvlen)
		value := utils.RandBytesInt(kvlen)
		entry := codec.NewEntry(key, value).WithTTL(time.Second)
		//fmt.Println(i, ": ", string(key))
		//s.ShowMeSkiplist()
		s.Add(entry)

		if s.Search(key) == nil {
			s.ShowMeSkiplist()
			s.Search(key)
		}
		// 不能直接比较地址，当有两个key相同时，这里会直接找到上层的entry返回
		if !bytes.Equal(s.Search(key).Key, entry.Key) {
			s.ShowMeSkiplist()
			s.Search(key)
		}
		//assert.Equal(t, s.Search(key).Key, entry.Key)
		//assert.Equal(t,s.Search(key),entry)
	}
}

func TestAddUniqueAndSearch(t *testing.T) {
	s := utils.NewSkipList()
	kvlen := 2
	iteration := 200
	for i := 0; i < iteration; i++ {
		key := utils.RandBytesInt(kvlen)
		value := utils.RandBytesInt(kvlen)
		entry := codec.NewEntry(key, value).WithTTL(time.Second)
		s.AddUnique(entry)
	}
	s.ShowMeSkiplist()
	for i := 0; i < iteration>>1; i++ {
		key := utils.RandBytesInt(kvlen)
		entry := s.Search(key)
		if entry != nil {
			if !bytes.Equal(entry.Key, key) {
				s.Search(key)
			}
			assert.Equal(t, entry.Key, key)
		}
	}
}
