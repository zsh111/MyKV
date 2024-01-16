package test

import (
	"corekv/utils/mmap"
	"fmt"
	"os"
	"testing"
)

func TestMmap(t *testing.T) {
	file, err := os.OpenFile("/home/zsh/Desktop/Go/test/mmapTest.txt", os.O_RDWR, 0)
	if err != nil {
		fmt.Println("Failed to open file:", err)
		return
	}
	defer file.Close()
	fileInfo, err := file.Stat()
	fmt.Printf("fileInfo.Size(): %v\n", fileInfo.Size())
	mmapData, err := mmap.Mmap(file, true, fileInfo.Size())
	fmt.Printf("mmapData: %v\n", string(mmapData))
	mmapData = append(mmapData, []byte("你好")...)
	fmt.Printf("mmapData: %v\n", string(mmapData))
	mmap.Msync(mmapData)
	defer mmap.Munmap(mmapData)
}
