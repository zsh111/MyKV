package codec

import "time"

type Entry struct {
	Key       []byte
	Value     []byte
	ExpiresAt uint64

	Offset uint32
	Hlen   int
}

func NewEntry(key, value []byte) *Entry {
	return &Entry{
		Key:   key,
		Value: value,
	}
}

func (e *Entry) WithTTL(dur time.Duration) *Entry {
	e.ExpiresAt = uint64(time.Now().Add(dur).Unix())
	return e
}

// 实现iterator的item接口
func (e *Entry) Entry() *Entry {
	return e
}

func (e *Entry) LogHeaderLen() int {
	return e.Hlen
}

func (e *Entry) LogOffset() uint32 {
	return e.Offset
}
