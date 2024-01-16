package lsm

import (
	"bytes"
	"corekv/file"
	"corekv/iterator"
	"corekv/pb"
	"corekv/utils"
	"corekv/utils/codec"
	"fmt"
	"io"
	"math"
	"os"
	"sort"
	"unsafe"
)

// 这里对SSTable进行抽象和序列化(序列化跳表的中间件)
// sstable中主要是由文件还原sst，这里需要进行序列化
type builder struct {
	curBlock    *block
	blockList   []*block
	indexLen    uint32
	checksum    []byte
	checksumLen uint32

	blockSize          int // from options
	bloomFalsePositive float64
	keyHashes          []uint32 // Bloom
	maxVersion         uint64
	sstSize            int64
	keyCount           uint32
}

type block struct {
	data        []byte
	offsets     []uint32 // 每个kv的offset
	offsetsLen  uint32
	checksum    []byte // 最大8 byte
	checksumLen uint32
	pointer     int
	blockSize   int64
	baseKey     []byte
}

type header struct {
	overlap uint16
	diff    uint16
}

/*--------------header接口--------------*/
func newHeader(overlap, diff uint16) *header {
	return &header{
		overlap: overlap,
		diff:    diff,
	}
}

func (h *header) decode(buf []byte) {
	const headerSize = uint16(unsafe.Sizeof(header{}))
	copy(((*[headerSize]byte)(unsafe.Pointer(h))[:]), buf[:headerSize])
}

func (h header) encode() []byte {
	var buf [4]byte
	*(*header)(unsafe.Pointer(&buf[0])) = h
	return buf[:]
}

/*--------------------------------------*/
/*---------------block接口--------------*/
func newBlock(sz int) *block {
	return &block{
		data: make([]byte, sz),
	}
}

func (b *block) keyDiff(key []byte) []byte {
	var i int = 0
	for ; i < len(key) && i < len(b.baseKey); i++ {
		if key[i] != b.baseKey[i] {
			break
		}
	}
	return key[i:]
}

func (b *block) allocate(sz int) []byte {
	if len(b.data[b.pointer:]) < sz {
		growBy := len(b.data)
		if growBy < sz {
			growBy = sz
		}
		tmp := make([]byte, b.pointer+growBy)
		copy(tmp, b.data)
		b.data = tmp
	}
	b.pointer += sz
	return b.data[b.pointer-sz : b.pointer]
}

func (b *block) ShowBlock() {
	var bi blockIterator
	bi.SetBlock(b)
	bi.Rewind()
	for ; bi.Valid(); bi.Next() {
		entry := bi.Item().Entry()
		fmt.Printf("%v ", string(entry.Key))
		//fmt.Printf("%v ", string(entry.Value))
	}
}

/*--------------------------------------*/

func NewTableBuilder(opt *Options) *builder {
	return &builder{
		blockSize:          opt.BlockSize,
		bloomFalsePositive: opt.BloomFalsePositive,
	}
}

func (tb *builder) GetBlockList() []*block {
	return tb.blockList
}

// 暂时定为新key，isstale: false
func (tb *builder) Add(e *codec.Entry, isStale bool) {
	key := e.Key
	val := codec.NewValueStruct(e)
	//暂时不考虑新建新的block
	if tb.checkBlockFull(key, val) {
		if isStale {
			// 旧key处理
		}
		tb.storeBlock()
		tb.curBlock = newBlock(tb.blockSize)
	}
	newKey, version := utils.ParseKey(key)
	tb.keyHashes = append(tb.keyHashes, utils.Hash(newKey))
	tb.maxVersion = max(tb.maxVersion, version)
	// 取第一个作为basekey一定是最小的
	var diffKey []byte //diffkey中包含了version
	if len(tb.curBlock.baseKey) == 0 {
		tb.curBlock.baseKey = append(tb.curBlock.baseKey[:0], key...)
		diffKey = key
	} else {
		diffKey = tb.curBlock.keyDiff(key)
	}
	overlap := uint16(len(key) - len(diffKey))
	diff := uint16(len(diffKey))
	h := newHeader(overlap, diff)
	tb.curBlock.offsets = append(tb.curBlock.offsets, uint32(tb.curBlock.pointer))

	tb.append(h.encode())
	tb.append(diffKey)
	dst := tb.curBlock.allocate(int(val.EncodeSize()))
	val.Encode(dst)
	// delete
	entry := tb.curBlock.CheckBlockKV(len(tb.curBlock.offsets) - 1)
	utils.AssertTrue(bytes.Equal(entry.Key, key))
	utils.AssertTrue(bytes.Equal(entry.Value, val.Value))
}

func (b *block) CheckBlockKV(idx int) (entry *codec.Entry) {
	if idx >= len(b.offsets) || idx < 0 {
		return nil
	}
	var nxt int
	if idx == len(b.offsets)-1 {
		nxt = b.pointer
	} else {
		nxt = int(b.offsets[idx+1])
	}
	off := b.offsets[idx]
	var h header
	headerSize := unsafe.Sizeof(header{})
	h.decode(b.data[off : off+uint32(headerSize)])
	off += uint32(headerSize)
	diffKey := b.data[off : off+uint32(h.diff)]
	key := make([]byte, len(b.baseKey))
	copy(key, b.baseKey)
	Key := append(key[:h.overlap], diffKey...)
	off += uint32(h.diff)
	var val codec.ValueStruct
	val.Decode(b.data[off:nxt])
	return codec.NewEntry(Key, val.Value)
}

// 新增一个kv增加的size为 true表示产生新block，否则继续在当前block读写
func (tb *builder) checkBlockFull(key []byte, val *codec.ValueStruct) bool {
	if tb.curBlock == nil {
		return true
	}
	if len(tb.curBlock.offsets) <= 0 {
		return false
	}
	// 后续大小为offset(4),offsetlen(4),checksum(8),checklen(4)
	blockExceptData := uint32((len(tb.curBlock.offsets)+1)*4 + 4 + 8 + 4)
	utils.CondPanic(!(blockExceptData < math.MaxUint32), utils.ErrInteger)

	kvHeaderSize := uint16(unsafe.Sizeof(header{}))
	kvSize := len(key) + int(val.EncodeSize())
	tb.curBlock.blockSize = int64(tb.curBlock.pointer) + int64(kvHeaderSize) + int64(kvSize) + int64(blockExceptData)
	utils.CondPanic(!(tb.curBlock.blockSize < math.MaxUint32), utils.ErrInteger)
	return tb.curBlock.blockSize > int64(tb.blockSize)
}

func (tb *builder) storeBlock() {
	if tb.curBlock == nil || len(tb.curBlock.offsets) == 0 {
		return
	}
	tb.append(utils.U32SliceToBytes(tb.curBlock.offsets))
	tb.append(utils.U32ToBytes(uint32(len(tb.curBlock.offsets))))

	checkSum := utils.U64ToBytes(calCheckSum())
	tb.append(checkSum)
	tb.append(utils.U32ToBytes(uint32(len(checkSum))))

	tb.sstSize += tb.curBlock.blockSize
	tb.blockList = append(tb.blockList, tb.curBlock)
	tb.keyCount += uint32(len(tb.curBlock.offsets))
	tb.curBlock = nil
	return
}

func calCheckSum() uint64 {
	var buf [8]byte
	return uint64(utils.CalCheckSum(buf[:]))
}

func (tb *builder) append(data []byte) {
	dst := tb.curBlock.allocate(len(data))
	utils.CondPanic(len(data) != copy(dst, data), utils.ErrBuilderAppend)
}

func (tb *builder) ShowTable() {
	for i, b := range tb.blockList {
		fmt.Printf("\n index block: %v\n", i)
		b.ShowBlock()
	}
}

/*------------------------------flush----------------------------*/
func (tb *builder) createIndex() *pb.TableIndex {
	tableIndex := &pb.TableIndex{}
	var bloom utils.Filter
	if tb.bloomFalsePositive > 0 {
		bitsPerKey := utils.BloomBitsPerKey(len(tb.keyHashes), tb.bloomFalsePositive)
		bloom = utils.NewFilter(tb.keyHashes, bitsPerKey)
	}
	if len(bloom) > 0 {
		tableIndex.BloomFilter = bloom
	}
	tableIndex.KeyCount = tb.keyCount
	tableIndex.MaxVersion = tb.maxVersion
	tableIndex.Offsets = tb.flushBlockOffsets(tableIndex)
	return tableIndex
}

// 整个flush的部分为：blocklist+tableindex+indexlen+checksum+checksumlen
func (tb *builder) Flush(lm *levelManager, tablename string) (*table, error) {
	tb.storeBlock()
	tableIndex := tb.createIndex()
	var dataSize uint32 // all block size
	for i := range tb.blockList {
		dataSize += uint32(tb.blockList[i].pointer)
	}
	data, err := tableIndex.Marshal()
	utils.Panic(err)
	indexLen := len(data)
	checksum := utils.U64ToBytes(utils.CalCheckSum(data))
	checkSumLen := len(checksum)
	totalSize := dataSize + uint32(indexLen) + uint32(checkSumLen) + 4 + 4

	t := &table{lm: lm, fid: file.GetFID(tablename)}

	t.ss = file.OpenSStable(&file.Options{
		FileName: tablename,
		Dir:      lm.opt.WorkDir,
		Flag:     os.O_CREATE | os.O_RDWR,
		MaxSz:    int(totalSize),
	})
	var buf []byte = make([]byte, totalSize)
	var written int
	for _, bl := range tb.blockList {
		written += copy(buf[written:], bl.data[:bl.pointer])
	}
	written += copy(buf[written:], data)
	written += copy(buf[written:], utils.U32ToBytes(uint32(indexLen)))
	written += copy(buf[written:], checksum)
	written += copy(buf[written:], utils.U32ToBytes(uint32(checkSumLen)))
	utils.CondPanic(written != len(buf), fmt.Errorf("tableBuilder.flush written != len(buf)"))
	dst, err := t.ss.Bytes(0, int(totalSize))
	if err != nil {
		return nil, err
	}
	copy(dst, buf)
	return t, nil
}

func (tb *builder) flushBlockOffsets(tableIndex *pb.TableIndex) []*pb.BlockOffset {
	var startOffset uint32
	var offsets []*pb.BlockOffset
	for _, bl := range tb.blockList {
		blockidx := tb.flushBlock(startOffset, bl)
		offsets = append(offsets, blockidx)
		startOffset += uint32(bl.pointer)
	}
	return offsets
}

func (tb *builder) flushBlock(offset uint32, bl *block) *pb.BlockOffset {
	blockOff := &pb.BlockOffset{}
	blockOff.Key = bl.baseKey
	blockOff.Offset = offset
	blockOff.Len = uint32(bl.pointer)
	return blockOff
}

/*------------------------iterator-----------------------*/

// 对block作为iterator的单元
type blockIterator struct {
	data         []byte //block data
	idx          int    // block idx
	err          error
	baseKey      []byte
	entryOffsets []uint32 // offset
	block        *block
	tableID      uint64
	blockID      int

	entry iterator.Item
}

func NewBlockIterator() *blockIterator {
	return &blockIterator{}
}

// 将iterator指向block
func (it *blockIterator) SetBlock(b *block) {
	it.block = b
	it.err = nil
	it.idx = 0
	it.baseKey = b.baseKey // 清空
	it.data = b.data[:]
	it.entryOffsets = b.offsets
}

// 从当前block中读取idx个entry
func (it *blockIterator) setIdx(i int) {
	if i >= len(it.entryOffsets) || i < 0 {
		it.err = io.EOF
		return
	}
	it.idx = i
	it.err = nil
	const headerSize = uint16(unsafe.Sizeof(header{}))
	startOff := int(it.entryOffsets[i])
	var endOff int
	if it.idx+1 == len(it.entryOffsets) {
		endOff = len(it.data)
	} else {
		endOff = int(it.entryOffsets[it.idx+1])
	}
	defer func() {
		if r := recover(); r != nil {
			var debugBuf bytes.Buffer
			fmt.Fprintf(&debugBuf, "===Recovered===\n")
			fmt.Fprintf(&debugBuf, "Table ID: %d\nBlock ID: %d\nEntry Idx: %d\nData len: %d\n"+
				"StartOffset: %d\nEndOffset: %d\nEntryOffsets len: %d\nEntryOffsets: %v\n",
				it.tableID, it.blockID, it.idx, len(it.data), startOff, endOff,
				len(it.entryOffsets), it.entryOffsets)
			panic(debugBuf.String())
		}
	}()

	entryData := it.data[startOff:endOff]
	var h header
	h.decode(entryData)
	diffKey := entryData[headerSize : headerSize+h.diff]
	key := it.baseKey[:]
	key = append(key[:h.overlap], diffKey...)
	val := &codec.ValueStruct{}
	val.Decode(entryData[headerSize+h.diff:])
	e := codec.NewEntry(key, val.Value)
	e.ExpiresAt = val.ExpiresAt
	it.entry = &Item{e: e}
}

func (it *blockIterator) seekFirst() {
	it.setIdx(0)
}

func (it *blockIterator) seekLast() {
	it.setIdx(len(it.entryOffsets) - 1)
}

func (it *blockIterator) Next() {
	it.setIdx(it.idx + 1)
}

func (it *blockIterator) Valid() bool {
	return it.err != io.EOF
}

// 默认为从小到大
func (it *blockIterator) Rewind() {
	it.setIdx(0)
}

func (it *blockIterator) Item() iterator.Item {
	return it.entry
}

func (it *blockIterator) Close() error {
	return nil
}

func (it *blockIterator) Seek(key []byte) {
	it.err = nil
	startIndex := 0

	foundEntryIdx := sort.Search(len(it.entryOffsets), func(idx int) bool {
		if idx < startIndex {
			return false
		}
		it.setIdx(idx)
		return bytes.Compare(it.Item().Entry().Key, key) >= 0
	})
	it.setIdx(foundEntryIdx)
}
