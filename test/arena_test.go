package test

import (
	"corekv/utils"
	"testing"

	"github.com/stretchr/testify/assert"
)

const (
	arenaSize = 1 << 16
)

func TestArenaKV(t *testing.T) {
	arena := utils.NewArena(arenaSize)
	pointer := 1
	for !arena.Full {
		key := utils.RandBytesChar(kvlen)
		//fmt.Printf("key: %v\n", string(key))
		arena.PutKey(key)
		ret := arena.GetKey(uint32(pointer), uint32(len(key)))
		pointer += len(key)
		assert.Equal(t, ret, key)
	}

}
