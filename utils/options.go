package utils

import (
	"hash/crc32"
	"os"
	"unsafe"
)

const (
	MaxLevelNum           = 7
	DefaultValueThreshold = 1024 // max value size
	MaxHeight             = 20   // skiplist max height
	BuildSLThreshold      = 32   // construct skiplist node num

	InitArenaSize = int64(1 << 10)
	MaxArenaSize  = 1 << 24 // max arena size is 16MB
	MinArenaSize  = 1 << 15 // min arena size is 32KB
	NodeAlign     = int(unsafe.Sizeof(uint64(0)) - 1)
	PerNextSize   = int(unsafe.Sizeof(uint32(0)))

	ManifestFilename        = "MANIFEST"
	ManifestRewriteFilename = "REWRITEMANIFEST"
	ManifestDelThreshold    = 10000
	ManifestDelRatio        = 10
	WalSuffix               = ".wal"
	SSTSuffix               = ".sst"
	DefaultFileFlag         = os.O_CREATE | os.O_RDWR | os.O_APPEND
	DefaultFileMode         = 0666
)

var (
	MagicText          = []byte{'H', 'A', 'R', 'D'}
	MagicVersion       = uint32(1)
	CastagnoliCrcTable = crc32.MakeTable(crc32.Castagnoli)
)

// 主要用于kv分离，设定相关参数，如level层数和maxvaluesize
type Options struct {
	ValueThreshold int64
}

func NewDefaultOPtions() *Options {
	opt := &Options{}
	opt.ValueThreshold = DefaultValueThreshold
	return opt
}
