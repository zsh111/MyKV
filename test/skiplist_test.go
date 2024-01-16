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

func TestAddAndSearch(t *testing.T) {
	s := utils.NewSkipList()
	kvlen := 2
	iteration := 1000
	for i := 0; i < iteration; i++ {
		key := utils.RandBytesInt(kvlen)
		value := utils.RandBytesInt(kvlen)
		entry := codec.NewEntry(key, value).WithTTL(time.Second)
		s.Add(entry)
	}
	s.ShowMeSkiplist()
	for i := 0; i < iteration>>1; i++ {
		key := utils.RandBytesInt(kvlen)
		entry := s.GetEntry(key)
		if entry != nil {
			if !bytes.Equal(entry.Key, key) {
				s.Search(key)
			}
			assert.Equal(t, entry.Key, key)
		}

	}
}
