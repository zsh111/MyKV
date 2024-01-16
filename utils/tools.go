package utils

import (
	"encoding/binary"
	"fmt"
	"hash/crc32"
	"log"
	"math"
	"reflect"
	"unsafe"

	"github.com/pkg/errors"
)

func ValueSize(value []byte) int64 {
	return 0
}

type any = interface{}

func Print(a ...any) {
	fmt.Println(a)
}

// separate key and ttl
func ParseKey(key []byte) ([]byte, uint64) {
	if len(key) < 8 {
		return key, 0
	}
	ts := math.MaxUint64 - binary.BigEndian.Uint64(key[len(key)-8:])
	return key[:len(key)-8], ts
}

// false中断
func AssertTrue(b bool) {
	// 函数调用栈的错误信息
	if !b {
		log.Fatalf("%+v", errors.Errorf("Assert failed"))
	}
}

func BytesToU32(b []byte) uint32 {
	return binary.BigEndian.Uint32(b)
}

func BytesToU64(b []byte) uint64 {
	return binary.BigEndian.Uint64(b)
}

func BytesToU32Slice(b []byte) []uint32 {
	if len(b) == 0 {
		return nil
	}
	var u32s []uint32
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&u32s))
	hdr.Len = len(b) / 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&b[0]))
	return u32s

}

func U32ToBytes(v uint32) []byte {
	var buf [4]byte
	binary.BigEndian.PutUint32(buf[:], v)
	return buf[:]
}

func U64ToBytes(v uint64) []byte {
	var buf [8]byte
	binary.BigEndian.PutUint64(buf[:], v)
	return buf[:]
}

func U32SliceToBytes(data []uint32) []byte {
	if len(data) == 0 {
		return nil
	}
	var buf []byte
	hdr := (*reflect.SliceHeader)(unsafe.Pointer(&buf))
	hdr.Len = len(data) * 4
	hdr.Cap = hdr.Len
	hdr.Data = uintptr(unsafe.Pointer(&data[0]))
	return buf
}

func VerifyCheckSum(data []byte, expected []byte) error {
	actual := uint64(crc32.Checksum(data, CastagnoliCrcTable))
	expectedU64 := BytesToU64(expected)
	if actual != expectedU64 {
		return errors.Wrapf(ErrCheckSumMismatch, "actual: %d,, expected: %d", actual, expectedU64)
	}
	return nil
}

func CalCheckSum(data []byte) uint64 {
	return uint64(crc32.Checksum(data, CastagnoliCrcTable))
}
