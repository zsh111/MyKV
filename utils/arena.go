package utils

import (
	"corekv/utils/codec"
	"sync/atomic"
)

const (
	maxArenaSize = 1 << 30 // 不建议该太大，后面对offset的访问有大小限制
	minArenaSize = 1 << 5
)

type Arena struct {
	pointer uint32
	full    bool
	buf     []byte
}

func NewArena(n int64) *Arena {
	if n < minArenaSize {
		n = minArenaSize
	} else if n > maxArenaSize {
		n = maxArenaSize
	}
	return &Arena{
		pointer: 1,
		full:    false,
		buf:     make([]byte, n),
	}
}

func (arena *Arena) allocate(sz uint32) uint32 {
	offset := atomic.AddUint32(&arena.pointer, sz)
	// 表示可以直接分配,返回分配内存的起点，包含起点
	if offset < uint32(len(arena.buf)) {
		return offset - sz
	}
	growUp := len(arena.buf)
	if growUp+len(arena.buf) > maxArenaSize {
		growUp = int(sz)
		arena.full = true
	}
	newbuf := make([]byte, len(arena.buf)+growUp)
	AssertTrue(len(arena.buf) == copy(newbuf, arena.buf))
	arena.buf = newbuf
	return offset - sz
}

func (arena *Arena) PutKey(key []byte) uint32 {
	// 返回put之前的pointer
	sz := uint32(len(key))
	offset := arena.allocate(sz)
	buf := arena.buf[offset : offset+sz]
	AssertTrue(len(key) == copy(buf, key))
	return arena.pointer - sz
}

func (arena *Arena)PutValue(vs *codec.ValueStruct)uint32{
	sz := vs.EncodeSize()
	offset := arena.allocate(sz)
	vs.Encode(arena.buf[offset:])
	return arena.pointer - sz
}

func (arena *Arena)PutNode(){

}


func (arena *Arena)GetKey(offset uint32,size uint32)[]byte{
	key := arena.buf[offset:offset+size]
	return key
}

func(arena *Arena)GetValue(offset uint32,size uint32)*codec.ValueStruct{
	vs := &codec.ValueStruct{}
	vs.Decode(arena.buf[offset:offset+size])
	return vs
}

