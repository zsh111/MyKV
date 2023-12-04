package utils

import (
	"crypto/rand"
	"encoding/binary"
	"fmt"
	"math"
	"time"
)

func SecureRandomSeed() int64 {
	var seed int64
	// 使用crypto/rand包生成安全的随机数种子
	randomBytes := make([]byte, 8)
	_, err := rand.Read(randomBytes)
	if err != nil {
		// 处理错误
		fmt.Println("Error generating random seed:", err)
		return time.Now().UnixNano() // 在发生错误时，使用时间戳作为备用种子
	}
	// 将随机字节转换为int64
	seed = int64(binary.LittleEndian.Uint64(randomBytes))
	ret := int64(math.Abs(float64(seed)))
	return ret
}

func RandBytesChar(len int) []byte {
	alphabet := make([]byte, 0)
	alphabet = append(alphabet, byte('A'+SecureRandomSeed()%26))
	for i := 0; i < len; i++ {
		alphabet = append(alphabet, byte('a'+SecureRandomSeed()%26))
	}
	return alphabet
}

func RandBytesInt(len int) []byte {
	alphabet := make([]byte, 0)
	alphabet = append(alphabet, byte('0'+SecureRandomSeed()%10))
	for i := 0; i < len; i++ {
		alphabet = append(alphabet, byte('0'+SecureRandomSeed()%10))
	}
	return alphabet
}
