package test

import (
	utils "coreKV/Utils"
	"testing"
)

func TestSmallBloomFilter(t *testing.T) {
	var hash []uint32
	for _, word := range [][]byte{
		[]byte("hello"), []byte("world"),
	} {
		hash = append(hash, utils.Hash(word))
	}
	want := "1...1.........1.........1.....1...1...1.....1.........1.....1....11....."
	perkeysize := 10
	bloom := utils.Init(int64(len(hash)), int64(perkeysize))
	bloom.MapHashTobitmap(hash, perkeysize)
	//keys为每个key的hash值
	filter := func(array []byte) string {
		s := make([]byte, 8*len(array))
		for i, x := range array {
			for j := 0; j < 8; j++ {
				if x&(1<<uint(j)) != 0 {
					s[8*i+j] = '1'
				} else {
					s[8*i+j] = '.'
				}
			}
		}
		return string(s)
	}
	got := filter(bloom.Extractbitmap())
	if got != want {
		t.Fatalf("bits:\ngot  %q\nwant %q", got, want)
	}
	m := map[string]bool{
		"hello": true,
		"world": true,
		"x":     false,
		"foo":   false,
	}
	for k, want := range m {
		got := bloom.ContainMay([]byte(k))
		if got != want {
			t.Errorf("MayContain: k=%q: got %v, want %v", k, got, want)
		}
	}
}

func TestHash(t *testing.T) {
	testCases := []struct {
		s    string
		want uint32
	}{
		{"", 0xbc9f1d34},
		{"g", 0xd04a8bda},
		{"go", 0x3e0b0745},
		{"gop", 0x0c326610},
		{"goph", 0x8c9d6390},
		{"gophe", 0x9bfd4b0a},
		{"gopher", 0xa78edc7c},
		{"I had a dream it would end this way.", 0xe14a9db9},
	}
	//这是leveldb中测试用例
	for _, tc := range testCases {
		got := utils.Hash([]byte(tc.s))
		if got != tc.want {
			t.Errorf("s=%q: got 0x%08x, want 0x%08x", tc.s, got, tc.want)
		}
	}
}

func TestBloomFilter(t *testing.T) {
	nextLength := func(x int) int {
		if x < 10 {
			return x + 1
		}
		if x < 100 {
			return x + 10
		}
		if x < 1000 {
			return x + 100
		}
		return x + 1000
	}
	le32 := func(i int) []byte {
		b := make([]byte, 4)
		b[0] = uint8(uint32(i) >> 0)
		b[1] = uint8(uint32(i) >> 8)
		b[2] = uint8(uint32(i) >> 16)
		b[3] = uint8(uint32(i) >> 24)
		return b
	}

	nMediocreFilters, nGoodFilters := 0, 0
loop:
	for length := 1; length <= 10000; length = nextLength(length) {
		keys := make([][]byte, 0, length)
		for i := 0; i < length; i++ {
			keys = append(keys, le32(i))//生成4byte的切片
		}
		var hashes []uint32
		for _, key := range keys {
			hashes = append(hashes, utils.Hash(key))
		}
		perkeysize := 10
		bloom := utils.Init(int64(len(hashes)), int64(perkeysize))
		bloom.MapHashTobitmap(hashes, perkeysize)

		Bitmap := bloom.Extractbitmap()
		if len(Bitmap) > (length*10/8)+40 {
			t.Errorf("length=%d: len(f)=%d is too large", length, len(Bitmap))
			continue
		}

		// All added keys must match.
		for _, key := range keys {
			if !bloom.ContainMay(key) {
				t.Errorf("length=%d: did not contain key %q", length, key)
				continue loop
			}
		}

		// Check false positive rate.
		nFalsePositive := 0
		for i := 0; i < 10000; i++ {
			if bloom.ContainMay(le32(1e9 + i)) {
				nFalsePositive++
			}
		}
		if nFalsePositive > 0.02*10000 {
			t.Errorf("length=%d: %d false positives in 10000", length, nFalsePositive)
			continue
		}
		if nFalsePositive > 0.0125*10000 {
			nMediocreFilters++
		} else {
			nGoodFilters++
		}
	}
	if nMediocreFilters > nGoodFilters/5 {
		t.Errorf("%d mediocre filters but only %d good filters", nMediocreFilters, nGoodFilters)
	}
}
