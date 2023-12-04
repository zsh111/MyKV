package utils

// 实现加速比较[]byte

func CalBytesHash(s []byte) int64 {
	var ret int64 = 0
	l := 8
	for i := 0; i < l; i++ {
		ret |= int64(s[i]) << (56 - i*8)
	}
	return ret
}

func CompareBytes(s1 []byte, s2 []byte) {
	if len(s1) >= 8 && len(s2) >= 8 {

	}
	ret1 := CalBytesHash(s1)
	Print(ret1)
}
