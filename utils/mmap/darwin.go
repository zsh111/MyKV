package mmap

// import (
// 	"os"
// 	"syscall"
// 	"unsafe"

// 	"golang.org/x/sys/unix"
// )

// /*直接将文件映射到用户空间，直接操作用户空间，减少内核空间的中转*/

// func mmap(fd *os.File,writable bool,size int64)([]byte, error){
// 	// 设定对内存保护类型，默认为read
// 	mtype := unix.PROT_READ
// 	if  writable {
// 		mtype |= unix.PROT_WRITE
// 	}
// 	// unix.MAP_SHARED表示内存映射区域与其他进程共享
// 	return unix.Mmap(int(fd.Fd()),0,int(size),mtype,unix.MAP_SHARED)
// }

// func munmap(buffer []byte)error{
// 	// 取消这块内存映射
// 	return unix.Munmap(buffer)
// }

// func madvise(buffer []byte,readahead bool)error{
// 	advice := unix.MADV_NORMAL // 表示系统同时优化顺序和随机访问
// 	if !readahead {
// 		advice = unix.MADV_RANDOM // 只优化随机访问
// 	}
// 	// 使用系统调用对系统要求管理的内存区域进行设定
// 	_, _, e1 := syscall.Syscall(syscall.SYS_MADVISE, uintptr(unsafe.Pointer(&buffer[0])),
// 		uintptr(len(buffer)), uintptr(advice))
// 	if e1 != 0 {
// 		return e1
// 	}
// 	return nil
// }

// func msync(b []byte)error{
// 	return unix.Msync(b,unix.MS_SYNC)
// }
