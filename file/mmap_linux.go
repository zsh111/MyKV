package file

import (
	"corekv/utils/mmap"
	"encoding/binary"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"

	"github.com/pkg/errors"
)

const oneGB = 1 << 30

type MmapFile struct {
	Data []byte
	Fd   *os.File
}

// 刷新
func SyncDir(dir string) error {
	df, err := os.Open(dir)
	if err != nil {
		return errors.Wrapf(err, "while open dir %s", dir)
	}
	if err := df.Sync(); err != nil {
		return errors.Wrapf(err, "while sync %s", dir)
	}
	if err := df.Close(); err != nil {
		return errors.Wrapf(err, "while close %s", dir)
	}
	return nil
}

// 对文件fd进行mmap映射，创建mmapfile句柄
func NewMmapFile(fd *os.File, sz int, writable bool) (*MmapFile, error) {
	filename := fd.Name()
	fi, err := fd.Stat()
	if err != nil {
		return nil, errors.Wrapf(err, "can not stat file", filename)
	}
	fileSize := fi.Size()
	if sz > 0 && fileSize == 0 {
		// 重定义文件大小，用于mmap
		if err := fd.Truncate(int64(sz)); err != nil {
			return nil, errors.Wrapf(err, "err while truncate")
		}
		fileSize = int64(sz)
	}
	buffer, err := mmap.Mmap(fd, writable, int64(sz))
	if err != nil {
		return nil, errors.Wrapf(err, "error mmap file %s with size %d", filename, sz)
	}
	if fileSize == 0 {
		dir, _ := filepath.Split(filename)
		go SyncDir(dir)
	}
	var rerr error
	return &MmapFile{
		Data: buffer,
		Fd:   fd,
	}, rerr
}

func (m *MmapFile) readFromFd(buf []byte, off int64) (int, error) {
	return m.Fd.ReadAt(buf, off)
}

func (m *MmapFile) getFileName() string {
	return m.Fd.Name()
}

func (m *MmapFile) getStat() (fs.FileInfo, error) {
	return m.Fd.Stat()
}

// 读取filename，作为mmap打开文件，指定标签和映射大小
func OpenMmapFile(filename string, flag int, maxSz int) (*MmapFile, error) {
	fd, err := os.OpenFile(filename, flag, 0666)
	if err != nil {
		return nil, errors.Wrapf(err, "unable to open file %s", filename)
	}
	writable := true
	if flag == os.O_RDONLY {
		writable = false
	}
	fileInfo, err := fd.Stat()
	// 如果sst被打开过，则使用源文件大小
	if err != nil && fileInfo != nil && fileInfo.Size() > 0 {
		maxSz = int(fileInfo.Size())
	}
	return NewMmapFile(fd, maxSz, writable)
}

type mmapReader struct {
	Data   []byte
	offset int
}

// 从mmap中读取文件
func (mr *mmapReader) Read(buf []byte) (int, error) {
	if mr.offset > len(mr.Data) {
		return 0, io.EOF
	}
	n := copy(buf, mr.Data[mr.offset:])
	mr.offset += n
	if n < len(buf) {
		return n, io.EOF
	}
	return n, nil
}

/*----------------------实现CoreFile接口--------------------------*/
func showErr(msg string, filename string, err error) error {
	return fmt.Errorf("while %s file: %s,error: %v\n", msg, filename, err)
}

func (m *MmapFile) Close() error {
	if m.Fd == nil {
		return nil
	}
	if err := m.Sync(); err != nil {
		return showErr("sync", m.getFileName(), err)
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return showErr("munmap", m.getFileName(), err)
	}
	return m.Fd.Close()
}

func (m *MmapFile) Truncature(n int64) error {
	if err := m.Sync(); err != nil {
		return showErr("sync", m.getFileName(), err)
	}
	if err := m.Fd.Truncate(n); err != nil {
		return showErr("truncate", m.getFileName(), err)
	}
	var err error
	m.Data, err = mmap.Mremap(m.Data, int(n))
	return err
}

func (m *MmapFile) ReName(name string) error { return nil }

func (m *MmapFile) NewReader(offset int) io.Reader {
	return &mmapReader{
		Data:   m.Data,
		offset: offset,
	}
}

func (m *MmapFile) Bytes(off, sz int) ([]byte, error) {
	if len(m.Data[off:]) < sz {
		return nil, io.EOF
	}
	return m.Data[off : off+sz], nil
}

// 返回分配的slice和slice position，用于不同mmapfile的数据添加方法
func (m *MmapFile) AllocateSlice(sz, offset int) ([]byte, int, error) {
	start := offset + 4
	if start+sz > len(m.Data) {
		const oneGB = 1 << 30
		growBy := len(m.Data)
		if growBy > oneGB {
			growBy = oneGB
		}
		if growBy < sz+4 {
			growBy = sz + 4
		}
		if err := m.Truncature(int64(len(m.Data)) + int64(growBy)); err != nil {
			return nil, 0, err
		}
	}
	binary.BigEndian.PutUint32(m.Data[offset:], uint32(sz))
	return m.Data[start : start+sz], start + sz, nil
}

func (m *MmapFile) AppendBuffer(offset uint32, buf []byte) error {
	size := len(m.Data)
	needSize := len(buf)
	end := offset + uint32(needSize)
	if end > uint32(size) {
		growBy := size
		if growBy > oneGB {
			growBy = size
		}
		if growBy < needSize {
			growBy = needSize
		}
		if err := m.Truncature(int64(end)); err != nil {
			return err
		}
	}
	dlen := copy(m.Data[offset:end], buf)
	if dlen != needSize {
		return errors.Errorf("dlen != needSize AppendBuffer failed")
	}
	return nil
}

func (m *MmapFile) Sync() error {
	if m == nil {
		return nil
	}
	return mmap.Msync(m.Data)
}

func (m *MmapFile) Delete() error {
	if m.Fd == nil {
		return nil
	}
	if err := mmap.Munmap(m.Data); err != nil {
		return showErr("munmap", m.getFileName(), err)
	}
	if err := m.Fd.Close(); err != nil {
		return showErr("close", m.getFileName(), err)
	}
	return os.Remove(m.getFileName())
}

// Data[sz|[]byte(sz)]
func (m *MmapFile) Slice(offset int) []byte {
	sz := binary.BigEndian.Uint32(m.Data[offset:]) //大端序读取uint32，从offset中
	start := offset + 4
	res, _ := m.Bytes(start, int(sz)) // warn
	return res
}
