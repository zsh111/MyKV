package codec

import (
	"encoding/binary"
	"fmt"
)

/*为了实现kv分离，单独存储value*/

const (
	codeLen  = 7
	metaInit = 0
)

type ValueStruct struct {
	Meta      byte
	Value     []byte
	ExpiresAt uint64
}

func NewValueStruct(entry *Entry) *ValueStruct {
	return &ValueStruct{
		Meta:      metaInit,
		Value:     entry.Value,
		ExpiresAt: entry.ExpiresAt,
	}
}

func sizeVarint(x uint64) (n int) {
	for {
		n++
		x >>= codeLen
		if x == 0 {
			break
		}
	}
	return n
}

func (vs *ValueStruct) GetValue() []byte {
	return vs.Value
}

func (vs *ValueStruct) EncodeSize() uint32 {
	byteLen := len(vs.Value) + 1
	if byteLen > 1<<30 {
		fmt.Println("nmd，value小一点会死？")
	}
	expireatLen := sizeVarint(vs.ExpiresAt)
	return uint32(byteLen + expireatLen)
}

func (vs *ValueStruct) Encode(buf []byte) uint32 {
	buf[0] = vs.Meta
	sz := binary.PutUvarint(buf[1:], vs.ExpiresAt)
	valueLen := copy(buf[1+sz:], vs.Value)
	return uint32(1 + sz + valueLen)
}

func (vs *ValueStruct) Decode(buf []byte) {
	vs.Meta = buf[0]
	var sz int
	vs.ExpiresAt, sz = binary.Uvarint(buf[1:])
	vs.Value = buf[1+sz:]
}

func IsValuePtr(entry *Entry) bool {
	return false
}
