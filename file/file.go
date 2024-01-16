package file

import (
	"corekv/utils"
	"fmt"
	"io"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
)

type Options struct {
	FID      uint64
	FileName string
	Dir      string
	Path     string
	Flag     int
	MaxSz    int
}

// 所有file均建议实现接口
type CoreFile interface {
	Close() error
	Truncature(n int64) error //重新映射
	ReName(name string) error
	NewReader(offset int) io.Reader    // 迭代器
	Bytes(off, sz int) ([]byte, error) // extract bytes
	AllocateSlice(sz, offset int) ([]byte, int, error)
	Sync() error
	Delete() error
	Slice(offset int) []byte
}

/*------------------------file工具函数------------------------*/

func GetFID(name string) uint64 {
	name = path.Base(name)
	if !strings.HasSuffix(name, utils.SSTSuffix) {
		return 0
	}
	name = strings.TrimSuffix(name, utils.SSTSuffix)
	id, err := strconv.Atoi(name)
	if err != nil {
		utils.Err(err)
		return 0
	}
	return uint64(id)
}

// 返回一个dir下的sst文件string
func CreateSSTFilePath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.sst", id))
}

func CreateWALPath(dir string, id uint64) string {
	return filepath.Join(dir, fmt.Sprintf("%05d.wal", id))
}

func ClearDir(dir string) {
	_, err := os.Stat(dir)
	if err != nil {
		os.RemoveAll(dir)
	}
	os.Mkdir(dir, os.ModePerm)

}

func LoadIDMap(dir string) map[uint64]struct{} {
	fileInfos, err := os.ReadDir(dir)
	utils.Err(err)
	idMap := make(map[uint64]struct{})
	for _, info := range fileInfos {
		if info.IsDir() {
			continue
		}
		fileID := GetFID(info.Name())
		if fileID != 0 {
			idMap[fileID] = struct{}{}
		}
	}
	return idMap
}
