// Copyright 2021 hardcore-os Project Authors
//
// Licensed under the Apache License, Version 2.0 (the "License")
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
// http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
// See the License for the specific language governing permissions and
// limitations under the License.

package utils

import (
	"math"
)

// Filter is an encoded set of []byte keys.
type Filter []byte

// 暂定所有bloom的k固定，或者可以计算？
type BloomFilter struct {
	bitmap Filter
	k      uint8
}

// MayContainKey _
func (f *BloomFilter) MayContainKey(k []byte) bool {
	return f.MayContain(Hash(k))
}

func (f *BloomFilter) GetBytes() []byte {
	return f.bitmap
}

// MayContain returns whether the filter may contain given key. False positives
// are possible, where it returns true for keys not in the original set.
func (f *BloomFilter) MayContain(h uint32) bool {
	if f.Len() < 2 {
		return false
	}
	k := f.k
	if k > 30 {
		// This is reserved for potentially new encodings for short Bloom filters.
		// Consider it a match.
		return true
	}
	nBits := uint32(8 * (f.Len() - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % nBits
		if f.bitmap[bitPos/8]&(1<<(bitPos%8)) == 0 {
			return false
		}
		h += delta
	}
	return true
}

func (f *BloomFilter) Len() int32 {
	return int32(len(f.bitmap))
}

func (f *BloomFilter) InsertKey(k []byte) bool {
	return f.Insert(Hash(k))
}

func (f *BloomFilter) Insert(h uint32) bool {
	k := f.k
	if k > 30 {
		// This is reserved for potentially new encodings for short Bloom filters.
		// Consider it a match.
		return true
	}
	nBits := uint32(8 * (f.Len() - 1))
	delta := h>>17 | h<<15
	for j := uint8(0); j < k; j++ {
		bitPos := h % uint32(nBits)
		f.bitmap[bitPos/8] |= 1 << (bitPos % 8)
		h += delta
	}
	return true
}

func (f *BloomFilter) AllowKey(k []byte) bool {
	if f == nil {
		return true
	}
	already := f.MayContainKey(k)
	if !already {
		f.InsertKey(k)
	}
	return already
}

func (f *BloomFilter) Allow(h uint32) bool {
	if f == nil {
		return true
	}
	already := f.MayContain(h)
	if !already {
		f.Insert(h)
	}
	return already
}

func (f *BloomFilter) Reset() {
	if f == nil {
		return
	}
	for i := range f.bitmap {
		f.bitmap[i] = 0
	}
}

// NewFilter returns a new Bloom filter that encodes a set of []byte keys with
// the given number of bits per key, approximately.
//
// A good bitsPerKey value is 10, which yields a filter with ~ 1% false
// positive rate.
func newFilter(numEntries int, falsePositive float64) *BloomFilter {
	bitsPerKey := BloomBitsPerKey(numEntries, falsePositive)
	return initFilter(numEntries, bitsPerKey)
}

func CreateBloom(numEntries int, falsePositive float64) *BloomFilter {
	return newFilter(numEntries, falsePositive)
}

// from hash key to filter
func NewFilter(keys []uint32, bitsPerKey int) Filter {
	return Filter(appendFilter(keys, bitsPerKey))
}

// BloomBitsPerKey returns the bits per key required by bloomfilter based on
// the false positive rate.
func BloomBitsPerKey(numEntries int, fp float64) int {
	size := -1 * float64(numEntries) * math.Log(fp) / math.Pow(float64(0.69314718056), 2)
	locs := math.Ceil(size / float64(numEntries))
	return int(locs)
}

func initFilter(numEntries int, bitsPerKey int) *BloomFilter {
	bf := &BloomFilter{}
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	// 0.69 is approximately ln(2).
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	bf.k = uint8(k)

	nBits := numEntries * int(bitsPerKey)
	// For small len(keys), we can see a very high false positive rate. Fix it
	// by enforcing a minimum bloom filter length.
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	filter := make([]byte, nBytes+1)

	//record the K value of this Bloom Filter
	filter[nBytes] = uint8(k)

	bf.bitmap = filter
	return bf
}

func appendFilter(keys []uint32, bitsPerKey int) []byte {
	if bitsPerKey < 0 {
		bitsPerKey = 0
	}
	k := uint32(float64(bitsPerKey) * 0.69)
	if k < 1 {
		k = 1
	}
	if k > 30 {
		k = 30
	}
	nBits := len(keys) * int(bitsPerKey)
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	nBits = nBytes * 8
	filter := make([]byte, nBytes+1)
	for _, h := range keys {
		delta := h>>17 | h<<15
		for j := uint32(0); j < k; j++ {
			bitPos := h % uint32(nBits)
			filter[bitPos/8] |= 1 << (bitPos % 8)
			h += delta
		}
	}
	filter[nBytes] = uint8(k)
	return filter
}

// Hash implements a hashing algorithm similar to the Murmur hash.
func Hash(b []byte) uint32 {
	const (
		seed = 0xbc9f1d34
		m    = 0xc6a4a793
	)
	h := uint32(seed) ^ uint32(len(b))*m
	for ; len(b) >= 4; b = b[4:] {
		h += uint32(b[0]) | uint32(b[1])<<8 | uint32(b[2])<<16 | uint32(b[3])<<24
		h *= m
		h ^= h >> 16
	}
	switch len(b) {
	case 3:
		h += uint32(b[2]) << 16
		fallthrough
	case 2:
		h += uint32(b[1]) << 8
		fallthrough
	case 1:
		h += uint32(b[0])
		h *= m
		h ^= h >> 24
	}
	return h
}
