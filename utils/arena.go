package utils

import (
	"corekv/utils/codec"
	"sync/atomic"
	"unsafe"
)

/*
 * arena存储结构为：node|key|value
 */

type Arena struct {
	pointer uint32
	Full    bool
	buf     []byte
}

func NewArena(n int64) *Arena {
	if n < MinArenaSize {
		n = MinArenaSize
	} else if n > MaxArenaSize {
		n = MaxArenaSize
	}
	return &Arena{
		pointer: 1,
		Full:    false,
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
	if growUp+len(arena.buf) > MaxArenaSize {
		growUp = int(sz)
		arena.Full = true
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

func (arena *Arena) PutValue(vs *codec.ValueStruct) uint32 {
	sz := vs.EncodeSize()
	offset := arena.allocate(sz)
	vs.Encode(arena.buf[offset:])
	return arena.pointer - sz
}

// 为了更好的空间利用，这里减少next数组的大小，尽量只存一个node next存在的空间
func (arena *Arena) PutNode(nodeHeight int) uint32 {
	save := (MaxHeight - nodeHeight) * PerNextSize
	sz := MaxNodeSize - save + NodeAlign  //这里分配多了,为了做内存对齐才多分配
	nodeOff := arena.allocate(uint32(sz)) //这个是理论上node开始位置，实际要偏移(对齐)

	nodeOff = (nodeOff + uint32(NodeAlign)) &^ uint32(NodeAlign)
	return nodeOff
}

func (arena *Arena) GetKey(offset uint32, size uint32) []byte {
	key := arena.buf[offset : offset+size]
	return key
}

func (arena *Arena) GetValue(offset uint32, size uint32) *codec.ValueStruct {
	vs := &codec.ValueStruct{}
	vs.Decode(arena.buf[offset : offset+size])
	return vs
}

func (arena *Arena) GetNode(nodeOffset uint32) *node {
	if nodeOffset == 0 {
		return nil
	}
	return (*node)(unsafe.Pointer(&arena.buf[nodeOffset]))
}
