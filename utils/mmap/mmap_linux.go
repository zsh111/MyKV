package mmap

import "os"

func Mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	return mmap(fd, writable, size)
}

// 取消一块内存区域的映射
func Munmap(buffer []byte) error {
	return munmap(buffer)
}

// 表示对磁盘的读进行优化（随机读或者顺序读，默认都优化）
func Madvise(buffer []byte, readahead bool) error {
	return madvise(buffer, readahead)
}

// 表示将内存修改同步到磁盘
func Msync(buffer []byte) error {
	return msyc(buffer)
}

// 对磁盘进行重新的内存映射，将buffer映射到新区域，新区域的大小为size
func Mremap(buffer []byte, size int) ([]byte, error) {
	return mremap(buffer, size)
}
