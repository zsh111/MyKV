package utils

import (
	"bytes"
	"corekv/utils/codec"
	"encoding/binary"
	"hash"
	"hash/crc32"
	"io"
	"unsafe"
)

const (
	// size of vlog header.
	// +----------------+------------------+
	// | keyID(8 bytes) |  baseIV(12 bytes)|
	// +----------------+------------------+
	ValueLogHeaderSize = 20
	vptrSize           = unsafe.Sizeof(ValuePtr{})
)

type ValuePtr struct {
	Len    uint32
	Offset uint32
	Fid    uint32
}

func (p ValuePtr) IsZero() bool {
	return p.Fid == 0 && p.Offset == 0 && p.Len == 0
}

// compare valueptr
func (p ValuePtr) Less(o *ValuePtr) bool {
	if o == nil {
		return false
	}
	if p.Fid != o.Fid {
		return p.Fid < o.Fid
	}
	if p.Offset != o.Offset {
		return p.Offset < o.Offset
	}
	return p.Len < o.Len
}

// encode valueptr
func (p ValuePtr) Encode() []byte {
	b := make([]byte, vptrSize)
	*(*ValuePtr)(unsafe.Pointer(&b[0])) = p
	return b
}
func (p *ValuePtr) Decode(b []byte) {
	copy(((*[vptrSize]byte)(unsafe.Pointer(p))[:]), b[:vptrSize])
}

type LogEntry func(e *codec.Entry, vp *ValuePtr) error

type WalHeader struct {
	KeyLen    uint32
	ValueLen  uint32
	Meta      byte
	ExpiresAt uint64
}

const maxHeaderSize int = 21

func (h WalHeader) Encode(out []byte) int {
	index := 0
	index = binary.PutUvarint(out[index:], uint64(h.KeyLen))
	index += binary.PutUvarint(out[index:], uint64(h.ValueLen))
	index += binary.PutUvarint(out[index:], uint64(h.Meta))
	index += binary.PutUvarint(out[index:], h.ExpiresAt)
	return index
}
func (h *WalHeader) Decode(reader *HashReader) (int, error) {
	var err error
	klen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.KeyLen = uint32(klen)
	vlen, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.ValueLen = uint32(vlen)
	meta, err := binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	h.Meta = byte(meta)
	h.ExpiresAt, err = binary.ReadUvarint(reader)
	if err != nil {
		return 0, err
	}
	return reader.BytesRead, nil
}

// 预写日志的格式：|header|key|value|crc32|
func WalCodec(buf *bytes.Buffer, e *codec.Entry) int {
	buf.Reset()
	h := WalHeader{
		KeyLen:    uint32(len(e.Key)),
		ValueLen:  uint32(len(e.Value)),
		ExpiresAt: e.ExpiresAt,
	}
	hash := crc32.New(CastagnoliCrcTable)
	writer := io.MultiWriter(buf, hash)
	var headerEnc [maxHeaderSize]byte
	sz := h.Encode(headerEnc[:])
	panic2(writer.Write(headerEnc[:sz]))
	panic2(writer.Write(e.Key))
	panic2(writer.Write(e.Value))
	var crcBuf [crc32.Size]byte
	binary.BigEndian.PutUint32(crcBuf[:], hash.Sum32())
	panic2(buf.Write(crcBuf[:]))
	return len(headerEnc[:sz]) + len(e.Key) + len(e.Value) + len(crcBuf)
}

func panic2(_ interface{}, err error) {
	Panic(err)
}

type HashReader struct {
	R         io.Reader
	H         hash.Hash32
	BytesRead int
}

func NewHashReader(r io.Reader) *HashReader {
	hash := crc32.New(CastagnoliCrcTable)
	return &HashReader{
		R: r,
		H: hash,
	}
}

// Read bytes from reader,return the number of bytes
func (t *HashReader) Read(p []byte) (int, error) {
	n, err := t.R.Read(p)
	if err != nil {
		return n, err
	}
	t.BytesRead += n
	return t.H.Write(p[:n])
}

func (t *HashReader) ReadByte() (byte, error) {
	b := make([]byte, 1)
	_, err := t.Read(b)
	return b[0], err
}
func (t *HashReader) Sum32() uint32 {
	return t.H.Sum32()
}
