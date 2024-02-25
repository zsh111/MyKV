package file

import (
	"bufio"
	"bytes"
	"corekv/utils"
	"corekv/utils/codec"
	"fmt"
	"hash/crc32"
	"io"
	"os"
	"sync"

	"github.com/pkg/errors"
)

// 补充一个预写日志
type WalFile struct {
	lock    *sync.RWMutex
	f       *MmapFile
	opt     *Options
	buf     *bytes.Buffer
	size    uint32
	writeAt uint32
}

func NewWAL() *WalFile {
	return nil
}

func (wf *WalFile) Fid() uint64 {
	return wf.opt.FID
}

func (wf *WalFile) Name() string {
	return wf.f.getFileName()
}

func (wf *WalFile) Close() error {
	fileName := wf.f.getFileName()
	if err := wf.f.Close(); err != nil {
		return err
	}
	return os.Remove(fileName)
}

// 当前被写入的数据大小
func (wf *WalFile) Size() uint32 {
	return wf.writeAt
}

func OpenWalFile(opt *Options) *WalFile {
	omf, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	wf := &WalFile{f: omf, lock: &sync.RWMutex{}, opt: opt}
	wf.buf = &bytes.Buffer{}
	wf.size = uint32(len(wf.f.Data))
	utils.Err(err)
	return wf
}

func (wf *WalFile) Write(entry *codec.Entry) error {
	// 落盘预写日志，同步写
	wf.lock.Lock()
	plen := utils.WalCodec(wf.buf, entry)
	buf := wf.buf.Bytes()
	utils.Panic(wf.f.AppendBuffer(wf.writeAt, buf))
	wf.writeAt += uint32(plen)
	wf.lock.Unlock()
	return nil
}

func (wf *WalFile) ShowWal() {

}

func (wf *WalFile) Truncate(end int64) error {
	if end <= 0 {
		return nil
	}
	if fi, err := wf.f.getStat(); err != nil {
		return fmt.Errorf("while file.stat on file: %s, error: %v\n", wf.Name(), err)
	} else if fi.Size() == end {
		return nil
	}
	wf.size = uint32(end)
	return wf.f.Truncature(end)
}

// db还原
func (wf *WalFile) Iterate(readOnly bool, offset uint32, fn utils.LogEntry) (uint32, error) {
	reader := bufio.NewReader(wf.f.NewReader(int(offset)))
	read := SafeRead{
		Key:          make([]byte, 10),
		Value:        make([]byte, 10),
		RecordOffset: offset,
		wf:           wf,
	}
	var validEndOffset uint32 = offset
loop:
	for {
		e, err := read.MakeEntry(reader)
		switch {
		case err == io.EOF:
			break loop
		case err == io.ErrUnexpectedEOF || err == utils.ErrTruncate:
			break loop
		case err != nil:
			return 0, err
		case len(e.Key) == 0:
			break loop
		}
		var vp utils.ValuePtr
		size := uint32(e.LogHeaderLen() + len(e.Key) + len(e.Value) + crc32.Size)
		read.RecordOffset += size
		validEndOffset = read.RecordOffset
		if err := fn(e, &vp); err != nil {
			if err == utils.ErrStop {
				break
			}
			return 0, errors.WithMessage(err, "Iteration function")
		}
	}
	return validEndOffset, nil
}

// / 封装kv分离
type SafeRead struct {
	Key          []byte
	Value        []byte
	RecordOffset uint32
	wf           *WalFile
}

// 还原key？
func (r *SafeRead) MakeEntry(reader io.Reader) (*codec.Entry, error) {
	tee := utils.NewHashReader(reader)
	var h utils.WalHeader
	_, err := h.Decode(tee)
	if err != nil {
		return nil, err
	}
	if h.KeyLen > uint32(1<<16) {
		return nil, utils.ErrTruncate
	}
	kl := int(h.KeyLen)
	if cap(r.Key) < kl {
		r.Key = make([]byte, kl<<1)
	}
	vl := int(h.ValueLen)
	if cap(r.Value) < vl {
		r.Value = make([]byte, vl<<1)
	}
	e := &codec.Entry{}
	buf := make([]byte, h.KeyLen+h.ValueLen)
	if _, err := io.ReadFull(tee, buf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	e.Key = buf[:h.KeyLen]
	e.Value = buf[h.KeyLen:]
	var crcBuf [crc32.Size]byte
	if _, err := io.ReadFull(reader, crcBuf[:]); err != nil {
		if err == io.EOF {
			err = utils.ErrTruncate
		}
		return nil, err
	}
	crc := utils.BytesToU32(crcBuf[:])
	if crc != tee.Sum32() {
		return nil, utils.ErrTruncate
	}
	e.ExpiresAt = h.ExpiresAt
	return e, nil

}
