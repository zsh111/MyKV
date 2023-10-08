package utils

import (
	"fmt"
	"math"
)

/*
 * 简单介绍BloomFilter：使用一个bitmap表示key是否存在，其中最后一个byte存储hash的次数k
 * 为了简化这里是使用murmurhash加上一个偏移量实现（偏移量与hash值有关），具体公式表现为：
 * m = -nlnp/(ln2)^2 主要由假阳性率和keys数量确定bitmap长度
 * k = mln2/n 由bitmap长度和n求出hash次数
 * 当给定k（为perkeysize）和n时，可以反求出m和假阳性率p
 */

type BloomFilter struct {
	bitmap []byte //bitmap
	//Falsepositiverate float64 //允许的假阳性率,我们只关心perkeysize，fp可以由其计算出来
	k int //hash的次数，使用bitmap最后一个byte存储k
}

const ln2 float64 = 0.69314718056

var Table = [...]byte{0x01, 0x02, 0x04, 0x80, 0x10, 0x20, 0x40, 0x08} //与常规相反，从右向左开始数

func CreateBloom(elementnum int64, fp float64) *BloomFilter {
	perkeysize := CalPerKeySize(elementnum, fp)
	return Init(elementnum, perkeysize)
}

// 计算bitmap的长度，由n+fp	=> 	m，计算单位key的长度也可以，直接由num计算出总长度
// 我们需要将一个key映射多次
func CalPerKeySize(elementnum int64, fp float64) int64 {
	size := -1 * float64(elementnum) * math.Log(fp) / math.Pow(ln2, 2)
	PerkeySize := math.Ceil(size / float64(elementnum))
	return int64(PerkeySize)
}

// 计算相对上次hash的偏移
func Caloffset(hashval uint32) uint32 {
	return hashval>>17 | hashval<<15
}

// max32G，返回字节数
func (bf *BloomFilter) Len() int64 {
	return int64(len(bf.bitmap))
}

// 初始化
func Init(elementnum int64, perkeysize int64) *BloomFilter {
	bf := new(BloomFilter)
	nBits := elementnum * perkeysize
	if nBits < 64 {
		nBits = 64
	}
	nBytes := (nBits + 7) / 8
	bf.bitmap = make([]byte, nBytes+1)
	k := int(float64(perkeysize) * ln2)
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}
	bf.k = k
	bf.bitmap[nBytes] = uint8(bf.k)
	return bf
}

// 插入数据，这里插入可能存在冲突，多次插入的位置已经为1
func (bf *BloomFilter) Insert(data []byte) {
	value := Hash(data)
	nBits := (bf.Len() - 1) * 8
	delta := Caloffset(value)
	for j := uint8(0); j < uint8(bf.k); j++ {
		hashpos := value % uint32(nBits)
		offset := hashpos % 8
		bf.bitmap[hashpos/8] |= Table[offset]
		value += delta
	}

}

// 清空
func (bf *BloomFilter) Reset() {
	if bf == nil {
		return
	}
	for i := range bf.bitmap {
		bf.bitmap[i] = 0
	}
}

// 检测是否在bitmap中，逐个检查bitmap对应的hash是否为1
func (bf *BloomFilter) ContainMay(k []byte) bool {
	hashval := Hash(k)
	nBits := 8 * (bf.Len() - 1) //bitmap长度
	delta := Caloffset(hashval)
	for j := uint8(0); j < uint8(bf.k); j++ {
		hashpos := hashval % uint32(nBits)
		if bf.bitmap[hashpos/8]&Table[hashpos%8] == 0 {
			return false
		}
		hashval += delta
	}
	return true
}

// 实现一个类似murmurhash函数，将一个byte array映射到uint32
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

func (bf *BloomFilter) MapHashTobitmap(values []uint32, perkeysize int) {
	nBits := (len(bf.bitmap) - 1) * 8
	NeedBits := len(values) * perkeysize
	if nBits < NeedBits {
		fmt.Println("bloom filter bitmap is not enough!")
		return
	}
	k := uint8(float64(perkeysize) * ln2)
	if k < 1 {
		k = 1
	} else if k > 30 {
		k = 30
	}
	for _, x := range values {
		delta := Caloffset(x)
		for j := uint8(0); j < k; j++ {
			hashpos := x % uint32(nBits)
			offset := hashpos % 8
			bf.bitmap[hashpos/8] |= Table[offset]
			x += delta
		}
	}

}

func (bf *BloomFilter) Extractbitmap() []byte {
	array := bf.bitmap[:]
	return array
}
