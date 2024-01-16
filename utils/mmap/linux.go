package mmap

import (
	"os"
	"reflect"
	"unsafe"

	"golang.org/x/sys/unix"
)

// 将文件直接映射到用户内存区域，不切换到内核态，减少空间的转换
func mmap(fd *os.File, writable bool, size int64) ([]byte, error) {
	// 默认限定内存只读
	mtype := unix.PROT_READ
	if writable {
		mtype |= unix.PROT_WRITE
	}
	// 表示这块内存区域所有进程共享
	return unix.Mmap(int(fd.Fd()), 0, int(size), mtype, unix.MAP_SHARED)
}

// 将内存中重新映射页面，替代munmap+mmap
func mremap(data []byte, size int) ([]byte, error) {
	const MREMAP_MAYMOVE = 0x1
	header := (*reflect.SliceHeader)(unsafe.Pointer(&data))
	mmapAddr, _, errno := unix.Syscall6(
		unix.SYS_MREMAP,
		header.Data,
		uintptr(header.Len),
		uintptr(size),
		uintptr(MREMAP_MAYMOVE),
		0,
		0,
	)
	if errno != 0 {
		return nil, errno
	}
	header.Data = mmapAddr
	header.Cap = size
	header.Len = size
	return data, nil
}

// 取消内存映射
func munmap(buffer []byte) error {
	if len(buffer) == 0 || len(buffer) != cap(buffer) {
		return unix.EINVAL
	}
	_, _, errno := unix.Syscall(
		unix.SYS_MUNMAP,
		uintptr(unsafe.Pointer(&buffer[0])),
		uintptr(len(buffer)),
		0,
	)
	if errno != 0 {
		return errno
	}
	return nil
}

func madvise(buffer []byte, readahead bool) error {
	flags := unix.MADV_NORMAL
	if !readahead {
		flags = unix.MADV_RANDOM
	}
	return unix.Madvise(buffer, flags)
}

func msyc(buffer []byte) error {
	return unix.Msync(buffer, unix.MS_SYNC)
}
