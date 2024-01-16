package file

import (
	"corekv/pb"
	"corekv/utils"
	"io"
	"os"
	"sync"
	"syscall"
	"time"

	"github.com/golang/protobuf/proto"
	"github.com/pkg/errors"
)

// 封装sst：
type SSTable struct {
	lock           *sync.RWMutex
	f              *MmapFile // 存放block
	maxKey         []byte
	minKey         []byte
	index          *pb.TableIndex // index_data，由pb封装
	hasBloomFilter bool
	indexLen       int // indexLen
	indexStart     int // indexStart
	fid            uint64
	createdAt      time.Time
}

/*----------------------------补充Get方法------------------------------*/
func (ss *SSTable) GetMaxKey() []byte {
	return ss.maxKey
}

func (ss *SSTable) GetMinKey() []byte {
	return ss.minKey
}

func (ss *SSTable) GetIndex() *pb.TableIndex {
	return ss.index
}

func (ss *SSTable) GetBloomLable() bool {
	return ss.hasBloomFilter
}

func (ss *SSTable) GetFID() uint64 {
	return ss.fid
}

// 做文件映射
func OpenSStable(opt *Options) *SSTable {
	mmapfile, err := OpenMmapFile(opt.FileName, os.O_CREATE|os.O_RDWR, opt.MaxSz)
	utils.Err(err)
	return &SSTable{lock: &sync.RWMutex{}, f: mmapfile, fid: opt.FID}
}

// 完整初始化sst，不还原block，直接根据index从fd中读取
func (ss *SSTable) Init() error {
	var blockoff *pb.BlockOffset //单个block
	var err error
	if blockoff, err = ss.initTable(); err != nil {
		return err
	}
	// 从文件中读取创建时间
	stat, _ := ss.f.getStat()
	statType := stat.Sys().(*syscall.Stat_t)
	ss.createdAt = time.Unix(statType.Ctim.Sec, statType.Ctim.Nsec)

	buf := blockoff.GetKey()
	minKey := make([]byte, len(buf))
	copy(minKey, buf)
	ss.minKey = minKey
	ss.maxKey = minKey
	return nil
}

// 从文件中初始化index
func (ss *SSTable) initTable() (*pb.BlockOffset, error) {
	readPos := len(ss.f.Data)

	readPos -= 4
	buf := ss.readCheckError(readPos, 4) // 读取checksum length
	checkSumLen := utils.BytesToU32(buf)
	utils.AssertTrue(checkSumLen > 0)
	readPos -= int(checkSumLen)
	ExcheckSum := ss.readCheckError(readPos, int(checkSumLen))
	readPos -= 4
	buf = ss.readCheckError(readPos, 4)
	ss.indexLen = int(utils.BytesToU32(buf))
	readPos -= ss.indexLen
	ss.indexStart = readPos
	data := ss.readCheckError(ss.indexStart, ss.indexLen)
	if err := utils.VerifyCheckSum(data, ExcheckSum); err != nil {
		return nil, errors.Wrapf(err, "failed to verify checksum for table: %s", ss.f.getFileName())
	}
	indexTable := &pb.TableIndex{}
	if err := proto.Unmarshal(data, indexTable); err != nil {
		return nil, err
	}
	ss.index = indexTable
	ss.hasBloomFilter = len(ss.index.BloomFilter) > 0
	if len(ss.index.GetOffsets()) > 0 {
		return ss.index.GetOffsets()[0], nil
	}
	return nil, utils.ErrSSTIndex

}

func (ss *SSTable) read(off, sz int) ([]byte, error) {
	if len(ss.f.Data) > 0 {
		if len(ss.f.Data[off:]) < sz {
			return nil, io.EOF
		}
	}
	res := make([]byte, sz)
	_, err := ss.f.readFromFd(res, int64(off))
	return res, err
}

// 读取checksum
func (ss *SSTable) readCheckError(off, sz int) []byte {
	buf, err := ss.read(off, sz)
	utils.Panic(err)
	return buf
}

func (ss *SSTable) Close() error {
	return ss.f.Close()
}

/*---------------------mmap操作flush---------------------*/

func (ss *SSTable) Bytes(off, sz int) ([]byte, error) {
	return ss.f.Bytes(off, sz)
}

func (ss *SSTable) Delete() error {
	return ss.f.Delete()
}

func (ss *SSTable) Size() int64 {
	filestat, err := ss.f.getStat()
	utils.Panic(err)
	return filestat.Size()
}

func (ss *SSTable) GetCreatedAt() *time.Time {
	return &ss.createdAt
}

func (ss *SSTable) SetCreatedAt(t *time.Time) {
	ss.createdAt = *t
}

func (ss *SSTable) Truncature(size int64) error {
	return ss.f.Truncature(size)
}
